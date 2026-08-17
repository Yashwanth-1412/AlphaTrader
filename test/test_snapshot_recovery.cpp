#include "QuantLink/Lib/logging/logger.h"
#include "market_data/itch_decoder.h"
#include "market_data/market_data.h"
#include "market_order_book.h"

#include <arpa/inet.h>
#include <chrono>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace alphatrader;

// ── Helper: send a raw ITCH packet via UDP ─────────────────────
static int udp_send(const std::string& dst_ip, int port, const void* data, size_t len) {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0)
        return -1;
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    inet_pton(AF_INET, dst_ip.c_str(), &addr.sin_addr);
    int n = sendto(fd, data, len, 0, (struct sockaddr*)&addr, sizeof(addr));
    close(fd);
    return n;
}

// ── Helper: build an ITCH AddOrder message in wire format ─────
// Returns vector<seq(8) + AddOrder(36)>
static auto make_add_order(uint64_t seq, uint64_t order_ref, char side, uint32_t shares, uint32_t price_itch, uint32_t ticker_id) -> std::vector<char> {
    constexpr size_t  MSG_SZ = 8 + 36;
    std::vector<char> buf(MSG_SZ, 0);
    uint64_t          seq_be = htobe64(seq);
    std::memcpy(buf.data(), &seq_be, 8); // seq big-endian
    buf[8] = 'A';                        // msg_type
    // stock_locate (offset 1, 2 bytes BE) = 0 for incremental
    // tracking_number (offset 3, 2 bytes BE) = 0
    // timestamp (offset 5, 6 bytes) = 0
    uint64_t ref_be = htobe64(order_ref);
    std::memcpy(buf.data() + 8 + 11, &ref_be, 8);    // order_ref_num
    buf[8 + 19]        = side;                       // buy_sell
    uint32_t shares_be = htonl(shares);
    std::memcpy(buf.data() + 8 + 20, &shares_be, 4); // shares
    uint64_t ticker_be = htobe64(ticker_id);
    std::memcpy(buf.data() + 8 + 24, &ticker_be, 8); // stock (ticker_id)
    uint32_t price_be = htonl(price_itch);
    std::memcpy(buf.data() + 8 + 32, &price_be, 4);  // price
    return buf;
}

// Snapshot: raw ITCH OrderDelete (no seq prefix)
static auto make_snap_order_delete(uint16_t stock_locate, uint64_t payload) -> std::vector<char> {
    constexpr size_t  SZ = 19;
    std::vector<char> buf(SZ, 0);
    buf[0]      = 'D';
    buf[1]      = (stock_locate >> 8) & 0xFF;
    buf[2]      = stock_locate & 0xFF;
    uint64_t be = htobe64(payload);
    std::memcpy(buf.data() + 11, &be, 8);
    return buf;
}

// Snapshot: raw ITCH AddOrder (no seq prefix, ticker_id in stock[8])
static auto make_snap_add_order(uint64_t order_ref, char side, uint32_t shares, uint32_t price_itch, uint32_t ticker_id) -> std::vector<char> {
    constexpr size_t  SZ = 36;
    std::vector<char> buf(SZ, 0);
    buf[0]          = 'A';
    uint64_t ref_be = htobe64(order_ref);
    std::memcpy(buf.data() + 11, &ref_be, 8);
    buf[19]            = side;
    uint32_t shares_be = htonl(shares);
    std::memcpy(buf.data() + 20, &shares_be, 4);
    uint64_t ticker_be = htobe64(ticker_id);
    std::memcpy(buf.data() + 24, &ticker_be, 8);
    uint32_t price_be = htonl(price_itch);
    std::memcpy(buf.data() + 32, &price_be, 4);
    return buf;
}

// ── main ───────────────────────────────────────────────────────
static bool recv_exact(int fd, char* buf, size_t len) {
    size_t got = 0;
    while (got < len) {
        const ssize_t n = recv(fd, buf + got, len - got, 0);
        if (n <= 0)
            return false;
        got += static_cast<size_t>(n);
    }
    return true;
}

int main() {
    std::cout << "\n═══ Snapshot Recovery Test ═══\n\n";

    const std::string iface    = "lo";
    const std::string inc_ip   = "127.0.0.1";
    const int         inc_port = 21003;

    quantlink::Logger                  logger(8 * 1024 * 1024, "snapshot_test.log", -1);
    quantlink::SPSCQueue<MarketUpdate> md_queue(1024);

    const auto initial_start = make_snap_order_delete(0xBBBB, 0);
    const auto initial_clear = make_snap_order_delete(0xCCCC, 0);
    const auto initial_end   = make_snap_order_delete(0xEEEE, 0);
    const auto snap_start    = make_snap_order_delete(0xBBBB, 7);
    const auto snap_clear    = make_snap_order_delete(0xCCCC, 0);
    const auto snap_add      = make_snap_add_order(200, 'B', 500, 1200000, 0);
    const auto snap_end      = make_snap_order_delete(0xEEEE, 10000);
    // Replay response: status 'R' + incremental frames for seq 3..6 (gap was 4)
    const auto        replay_status = std::vector<char>{'R'};
    const auto        replay_3      = make_add_order(3, 103, 'B', 100, 1100000, 0);
    const auto        replay_4      = make_add_order(4, 104, 'B', 200, 1050000, 0);
    const auto        replay_5      = make_add_order(5, 105, 'S', 150, 2000000, 0);
    const auto        replay_6      = make_add_order(6, 106, 'S', 100, 2100000, 0);
    std::atomic<bool> send_replay{false};
    std::atomic<bool> send_recovery_snapshot{false};

    int snapshot_server = socket(AF_INET, SOCK_STREAM, 0);
    int reuse           = 1;
    setsockopt(snapshot_server, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    sockaddr_in snapshot_addr{};
    snapshot_addr.sin_family      = AF_INET;
    snapshot_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    snapshot_addr.sin_port        = htons(21003);
    bind(snapshot_server, reinterpret_cast<sockaddr*>(&snapshot_addr), sizeof(snapshot_addr));
    listen(snapshot_server, 2);
    std::thread snapshot_thread([&]() {
        for (int request_number = 0; request_number < 3; ++request_number) {
            const int  client       = accept(snapshot_server, nullptr, nullptr);
            const auto send_message = [client](const auto& message) { send(client, message.data(), message.size(), MSG_NOSIGNAL); };
            if (request_number == 0) {
                // Initial sync: 'S' snapshot request
                char request = 0;
                recv_exact(client, &request, 1);
                send_message(initial_start);
                send_message(initial_clear);
                send_message(initial_end);
            } else if (request_number == 1) {
                // Gap recovery: 'R' replay request ('R' + 8-byte BE client seq)
                char request[1 + sizeof(SeqNum)] = {};
                recv_exact(client, request, sizeof(request));
                while (!send_replay.load(std::memory_order_acquire)) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                send_message(replay_status);
                send_message(replay_3);
                send_message(replay_4);
                send_message(replay_5);
                send_message(replay_6);
            } else {
                // Big-gap recovery: 'S' snapshot request
                char request = 0;
                recv_exact(client, &request, 1);
                while (!send_recovery_snapshot.load(std::memory_order_acquire)) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                send_message(snap_start);
                send_message(snap_clear);
                send_message(snap_add);
                send_message(snap_end);
            }
            close(client);
        }
        close(snapshot_server);
    });

    MarketDataConsumer consumer(&md_queue, &logger, iface, "127.0.0.1", 21003, inc_ip, inc_port, inc_port);
    consumer.start(-1);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    int  passed = 0, failed = 0;
    auto check = [&](const std::string& desc, bool cond) {
        if (cond) {
            std::cout << "  ✓ " << desc << "\n";
            ++passed;
        } else {
            std::cout << "  ✗ " << desc << " <<< FAIL\n";
            ++failed;
        }
    };

    auto drain = [&]() -> std::vector<MarketUpdate> {
        std::vector<MarketUpdate> out;
        MarketUpdate              u;
        while (md_queue.pop(u))
            out.push_back(u);
        return out;
    };

    auto drain_flushed = [&]() -> std::vector<MarketUpdate> {
        std::vector<MarketUpdate> out;
        MarketUpdate              u;
        while (md_queue.pop(u))
            out.push_back(u);
        logger.flush();
        return out;
    };

    // The consumer requests the initial authoritative snapshot over TCP.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    drain();
    check("initial snapshot synchronizes market data", market_data_synchronized.load(std::memory_order_acquire));

    // ═══════════════════════════════════════════════════════════
    // Phase 1: Send incrementals (no gap yet)
    // ═══════════════════════════════════════════════════════════
    std::cout << "─── Phase 1: Normal incrementals ───\n";
    auto pkt1 = make_add_order(1, 100, 'B', 200, 1000000, 0);
    auto pkt2 = make_add_order(2, 101, 'S', 150, 2000000, 0);
    udp_send(inc_ip, inc_port, pkt1.data(), pkt1.size());
    udp_send(inc_ip, inc_port, pkt2.data(), pkt2.size());
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    auto s1 = drain();
    check("received 2 normal AddOrder updates", s1.size() == 2);
    check("seq=1 type=A qty=200", s1.size() >= 1 && s1[0].seq_num == 1 && s1[0].qty == 200);
    check("seq=2 type=A qty=150", s1.size() >= 2 && s1[1].seq_num == 2 && s1[1].qty == 150);

    // ═══════════════════════════════════════════════════════════
    // Phase 2: Trigger gap (seq=7, consumer expected seq=3)
    // ═══════════════════════════════════════════════════════════
    std::cout << "─── Phase 2: Gap injection ───\n";
    auto pkt3 = make_add_order(7, 102, 'B', 300, 1500000, 0);
    udp_send(inc_ip, inc_port, pkt3.data(), pkt3.size());
    // Recovery is in progress: the replay response is withheld by the server,
    // so the consumer stays in recovery while increments are queued internally.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    auto s2 = drain();
    // During recovery, incremental updates are queued but NOT pushed to SPSCQueue
    // until recovery completes. So we expect 0 new updates from the queue.
    check("no updates pushed during recovery (queued internally)", s2.empty());
    check("market data is unavailable during recovery", !market_data_synchronized.load(std::memory_order_acquire));

    // ═══════════════════════════════════════════════════════════
    // Phase 3: Replay delivery (server replayed seq 3..6)
    // ═══════════════════════════════════════════════════════════
    std::cout << "─── Phase 3: Replay delivery ───\n";

    send_replay.store(true, std::memory_order_release);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto s3 = drain_flushed();
    // Replayed frames (seq 3..6) plus the queued incremental (seq=7 AddOrder ref=102)
    // should be pushed in contiguous order after replay completes.
    check("updates pushed after replay recovery completes", s3.size() == 5);
    bool contiguous = s3.size() == 5;
    for (size_t i = 0; i < s3.size(); ++i) {
        if (s3[i].seq_num != static_cast<SeqNum>(3 + i))
            contiguous = false;
    }
    check("replayed updates are contiguous (seq 3..7)", contiguous);
    int replay_orders = 0, queued_orders = 0;
    for (auto& m : s3) {
        if (m.order_ref >= 103 && m.order_ref <= 106)
            ++replay_orders;
        if (m.order_ref == 102)
            ++queued_orders;
    }
    check("replayed frames (ref 103..106) delivered", replay_orders == 4);
    check("queued incremental (ref=102) flushed after replay", queued_orders == 1);

    MarketOrderBook book(0, &logger);
    for (const auto& update : s3)
        book.applyUpdate(update);
    check("replay rebuilds the order book", book.trackedOrders() == 5 && book.getBBO().bid_price == 1500000 && book.getBBO().bid_qty == 300);
    check("market data is synchronized after replay", market_data_synchronized.load(std::memory_order_acquire));

    // ═══════════════════════════════════════════════════════════
    // Phase 4: Gap larger than replay capacity → snapshot fallback
    // ═══════════════════════════════════════════════════════════
    std::cout << "─── Phase 4: Big gap (snapshot fallback) ───\n";
    auto pkt4 = make_add_order(10000, 300, 'S', 100, 2500000, 0);
    udp_send(inc_ip, inc_port, pkt4.data(), pkt4.size());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    auto s4 = drain();
    check("no updates pushed during big-gap recovery", s4.empty());

    send_recovery_snapshot.store(true, std::memory_order_release);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto s5 = drain();
    check("updates pushed after snapshot recovery completes", !s5.empty());
    int snapshot_starts = 0, snapshot_clears = 0, snapshot_ends = 0;
    int snapshot_orders = 0, dropped_orders = 0;
    for (auto& m : s5) {
        if (m.type == UpdateType::SNAPSHOT_START)
            ++snapshot_starts;
        if (m.type == UpdateType::SNAPSHOT_CLEAR)
            ++snapshot_clears;
        if (m.type == UpdateType::SNAPSHOT_END)
            ++snapshot_ends;
        if (m.order_ref == 200)
            ++snapshot_orders; // from snapshot
        if (m.order_ref == 300)
            ++dropped_orders;  // from queued incremental
    }
    check("snapshot controls are forwarded", snapshot_starts == 1 && snapshot_clears == 1 && snapshot_ends == 1);
    check("snapshot AddOrder (ref=200) applied", snapshot_orders >= 1);
    // queued incremental at seq=10000 is NOT replayed because snapshot covers up to last_inc_seq=10000
    check("queued incremental (seq<=last_inc_seq) correctly discarded", dropped_orders == 0);

    MarketOrderBook book2(0, &logger);
    for (const auto& update : s5)
        book2.applyUpdate(update);
    check("snapshot rebuilds the order book", book2.trackedOrders() == 1 && book2.getBBO().bid_price == 1200000 && book2.getBBO().bid_qty == 500);
    check("market data is synchronized after snapshot", market_data_synchronized.load(std::memory_order_acquire));

    // ═══════════════════════════════════════════════════════════
    // Phase 5: Back to normal — send more incrementals
    // ═══════════════════════════════════════════════════════════
    std::cout << "─── Phase 5: Resume normal ───\n";
    auto pkt5 = make_add_order(10001, 400, 'S', 100, 2600000, 0);
    udp_send(inc_ip, inc_port, pkt5.data(), pkt5.size());
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    auto s6 = drain();
    check("resumed: new AddOrder (seq=10001) received", s6.size() == 1 && s6[0].seq_num == 10001);

    // ── Cleanup ─────────────────────────────────────────────
    consumer.stop();
    snapshot_thread.join();

    std::cout << "\n═══ Results: " << (failed == 0 ? "ALL PASS" : "SOME FAILED") << "  (" << passed << " passed, " << failed << " failed) ═══\n";
    return failed > 0 ? 1 : 0;
}
