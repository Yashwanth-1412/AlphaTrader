#include "market_data/itch_decoder.h"
#include "market_data/market_data_consumer.h"
#include "market_order_book.h"
#include "market_data/market_data_sync.h"

#include "QuantLink/Lib/logging/logger.h"

#include <chrono>
#include <cstring>
#include <iostream>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace alphatrader;

// ── Helper: send a raw ITCH packet via UDP ─────────────────────
static int udp_send(const std::string& dst_ip, int port,
                    const void* data, size_t len) {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, dst_ip.c_str(), &addr.sin_addr);
    int n = sendto(fd, data, len, 0, (struct sockaddr*)&addr, sizeof(addr));
    close(fd);
    return n;
}

// ── Helper: build an ITCH AddOrder message in wire format ─────
// Returns vector<seq(8) + AddOrder(36)>
static auto make_add_order(uint64_t seq, uint64_t order_ref,
                           char side, uint32_t shares,
                           uint32_t price_itch, uint32_t ticker_id) -> std::vector<char> {
    constexpr size_t MSG_SZ = 8 + 36;
    std::vector<char> buf(MSG_SZ, 0);
    std::memcpy(buf.data(), &seq, 8);                 // seq little-endian
    buf[8] = 'A';                                     // msg_type
    // stock_locate (offset 1, 2 bytes BE) = 0 for incremental
    // tracking_number (offset 3, 2 bytes BE) = 0
    // timestamp (offset 5, 6 bytes) = 0
    uint64_t ref_be = htobe64(order_ref);
    std::memcpy(buf.data() + 8 + 11, &ref_be, 8);    // order_ref_num
    buf[8 + 19] = side;                               // buy_sell
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
    constexpr size_t SZ = 19;
    std::vector<char> buf(SZ, 0);
    buf[0] = 'D';
    buf[1] = (stock_locate >> 8) & 0xFF;
    buf[2] = stock_locate & 0xFF;
    uint64_t be = htobe64(payload);
    std::memcpy(buf.data() + 11, &be, 8);
    return buf;
}

// Snapshot: raw ITCH AddOrder (no seq prefix, ticker_id in stock[8])
static auto make_snap_add_order(uint64_t order_ref, char side, uint32_t shares,
                                 uint32_t price_itch, uint32_t ticker_id) -> std::vector<char> {
    constexpr size_t SZ = 36;
    std::vector<char> buf(SZ, 0);
    buf[0] = 'A';
    uint64_t ref_be = htobe64(order_ref);
    std::memcpy(buf.data() + 11, &ref_be, 8);
    buf[19] = side;
    uint32_t shares_be = htonl(shares);
    std::memcpy(buf.data() + 20, &shares_be, 4);
    uint64_t ticker_be = htobe64(ticker_id);
    std::memcpy(buf.data() + 24, &ticker_be, 8);
    uint32_t price_be = htonl(price_itch);
    std::memcpy(buf.data() + 32, &price_be, 4);
    return buf;
}

// ── main ───────────────────────────────────────────────────────
int main() {
    std::cout << "\n═══ Snapshot Recovery Test ═══\n\n";

    const std::string iface = "lo";
    const std::string inc_ip = "127.0.0.1";
    const int inc_port = 21003;
    const std::string snap_ip = "127.0.0.1";
    const int snap_port = 21004;

    quantlink::Logger logger(8 * 1024 * 1024, "snapshot_test.log", -1);
    quantlink::SPSCQueue<MarketUpdate> md_queue(1024);

    MarketDataConsumer consumer(&md_queue, &logger,
                                iface, snap_ip, snap_port, inc_ip, inc_port);
    consumer.start(-1);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    int passed = 0, failed = 0;
    auto check = [&](const std::string& desc, bool cond) {
        if (cond) { std::cout << "  ✓ " << desc << "\n"; ++passed; }
        else      { std::cout << "  ✗ " << desc << " <<< FAIL\n"; ++failed; }
    };

    auto drain = [&]() -> std::vector<MarketUpdate> {
        std::vector<MarketUpdate> out; MarketUpdate u;
        while (md_queue.pop(u)) out.push_back(u);
        return out;
    };

    // Establish the initial authoritative state before accepting incrementals.
    auto initial_start = make_snap_order_delete(0xBBBB, 0);
    auto initial_clear = make_snap_order_delete(0xCCCC, 0);
    auto initial_end = make_snap_order_delete(0xEEEE, 0);
    udp_send(snap_ip, snap_port, initial_start.data(), initial_start.size());
    udp_send(snap_ip, snap_port, initial_clear.data(), initial_clear.size());
    udp_send(snap_ip, snap_port, initial_end.data(), initial_end.size());
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    drain();
    check("initial snapshot synchronizes market data",
          market_data_synchronized.load(std::memory_order_acquire));

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
    // Wait longer — consumer enters recovery (startSnapshotSync → init snapshot socket)
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    auto s2 = drain();
    // During recovery, incremental updates are queued but NOT pushed to SPSCQueue
    // until recovery completes. So we expect 0 new updates from the queue.
    check("no updates pushed during recovery (queued internally)", s2.empty());
    check("market data is unavailable during recovery",
          !market_data_synchronized.load(std::memory_order_acquire));

    // ═══════════════════════════════════════════════════════════
    // Phase 3: Send snapshot (START → CLEAR → AddOrder → END)
    // ═══════════════════════════════════════════════════════════
    std::cout << "─── Phase 3: Snapshot delivery ───\n";

    // Snapshot messages arrive as raw ITCH structs (no seq prefix)
    auto snap_start = make_snap_order_delete(0xBBBB, 7);  // START, last_inc_seq = 7
    auto snap_clear = make_snap_order_delete(0xCCCC, 0);   // CLEAR ticker=0
    auto snap_add   = make_snap_add_order(200, 'B', 500, 1200000, 0); // resting order
    auto snap_end   = make_snap_order_delete(0xEEEE, 7);  // END, resume_seq = 7

    udp_send(snap_ip, snap_port, snap_start.data(), snap_start.size());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    udp_send(snap_ip, snap_port, snap_clear.data(), snap_clear.size());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    udp_send(snap_ip, snap_port, snap_add.data(), snap_add.size());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    udp_send(snap_ip, snap_port, snap_end.data(), snap_end.size());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto s3 = drain();
    // After recovery completes: the queued incremental (seq=7 AddOrder 300 @ 1500000)
    // AND the snapshot AddOrder (ref=200, 500 @ 1200000) should both be pushed
    check("updates pushed after recovery completion", !s3.empty());
    int snapshot_starts = 0, snapshot_clears = 0, snapshot_ends = 0;
    int snapshot_orders = 0, queued_orders = 0;
    for (auto& m : s3) {
        if (m.type == UpdateType::SNAPSHOT_START) ++snapshot_starts;
        if (m.type == UpdateType::SNAPSHOT_CLEAR) ++snapshot_clears;
        if (m.type == UpdateType::SNAPSHOT_END) ++snapshot_ends;
        if (m.order_ref == 200) ++snapshot_orders;  // from snapshot
        if (m.order_ref == 102) ++queued_orders;     // from queued incremental
    }
    check("snapshot controls are forwarded", snapshot_starts == 1 && snapshot_clears == 1 && snapshot_ends == 1);
    check("snapshot AddOrder (ref=200) applied", snapshot_orders >= 1);
    // queued incremental at seq=7 is NOT replayed because snapshot covers up to last_inc_seq=7
    check("queued incremental (seq<=last_inc_seq) correctly discarded",
          queued_orders == 0);

    MarketOrderBook book(0, &logger);
    for (const auto& update : s3) book.applyUpdate(update);
    check("snapshot rebuilds the order book", book.trackedOrders() == 1 &&
          book.getBBO().bid_price == 1200000 && book.getBBO().bid_qty == 500);
    check("market data is synchronized after replay",
          market_data_synchronized.load(std::memory_order_acquire));

    // ═══════════════════════════════════════════════════════════
    // Phase 4: Back to normal — send more incrementals
    // ═══════════════════════════════════════════════════════════
    std::cout << "─── Phase 4: Resume normal ───\n";
    auto pkt4 = make_add_order(8, 300, 'S', 100, 2500000, 0);
    udp_send(inc_ip, inc_port, pkt4.data(), pkt4.size());
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    auto s4 = drain();
    check("resumed: new AddOrder (seq=8) received",
          s4.size() == 1 && s4[0].seq_num == 8);

    // ── Cleanup ─────────────────────────────────────────────
    consumer.stop();

    std::cout << "\n═══ Results: " << (failed == 0 ? "ALL PASS" : "SOME FAILED")
              << "  (" << passed << " passed, " << failed << " failed) ═══\n";
    return failed > 0 ? 1 : 0;
}
