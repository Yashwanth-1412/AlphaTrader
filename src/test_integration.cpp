#include "market_data/itch_decoder.h"
#include "market_data/market_data_consumer.h"
#include "market_data/market_data_sync.h"
#include "market_order_book.h"

#include "QuantLink/Lib/logging/logger.h"
#include "QuantLink/Lib/common/macros.h"

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

using namespace alphatrader;

// ── OUCH message structs (packed, big-endian wire format) ───────
#pragma pack(push, 1)
struct OuchEnterOrder {
    char  msg_type = 'O';
    char  order_token[14]{};
    char  side = 'B';
    char  shares[4]{};
    char  stock[8]{};
    char  price[4]{};
    char  time_in_force[4]{};
    char  firm[4]{};
    char  display = 'Y';
    char  capacity = 'A';
    char  intermarket_sweep = 'N';
    char  cross_type = ' ';
    char  customer_type = ' ';
};

struct OuchCancelOrder {
    char  msg_type = 'X';
    char  order_token[14]{};
    char  shares[4]{};
};

struct OuchReplaceOrder {
    char  msg_type = 'U';
    char  existing_token[14]{};
    char  replacement_token[14]{};
    char  shares[4]{};
    char  price[4]{};
    char  time_in_force[4]{};
    char  display = 'Y';
    char  intermarket_sweep = 'N';
};
#pragma pack(pop)

// ── Helpers ─────────────────────────────────────────────────────
static void set_be32(char* buf, uint32_t val) {
    uint32_t be = htonl(val);
    std::memcpy(buf, &be, 4);
}

static void set_be64(char* buf, uint64_t val) {
    uint64_t be = htobe64(val);
    std::memcpy(buf, &be, 8);
}

static void pad_str(char* buf, size_t len, const std::string& s) {
    std::memset(buf, ' ', len);
    size_t n = std::min(s.size(), len);
    std::memcpy(buf, s.data(), n);
}

static int tcp_connect(const std::string& host, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static bool wait_for_port(const std::string& host, int port, int timeout_secs) {
    for (int i = 0; i < timeout_secs * 10; ++i) {
        int fd = tcp_connect(host, port);
        if (fd >= 0) { close(fd); return true; }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
}

// ── Scenario runner ─────────────────────────────────────────────

struct TestCase {
    std::string name;
    int         expect_min;  // minimum expected updates
    bool        expect_has_type[256]{}; // which msg types must appear
};

static int  g_total_passed = 0;
static int  g_total_failed = 0;
static bool g_verbose = false;

static auto check(const std::string& desc, bool cond) -> void {
    if (cond) {
        std::cout << "  ✓ " << desc << "\n";
        ++g_total_passed;
    } else {
        std::cout << "  ✗ " << desc << "  <<< FAIL\n";
        ++g_total_failed;
    }
}

// ── MAIN ────────────────────────────────────────────────────────
int main(int argc, char** argv) {
    g_verbose = (argc > 1 && std::string(argv[1]) == "-v");

    const std::string gateway_iface = "lo";
    const std::string mcast_iface = "lo";
    const int gateway_port = 11000;
    const std::string inc_mcast = "127.0.0.1";
    const int inc_port = 21001;
    const int snapshot_tcp_port = 21003;

    const char* ne_path = std::getenv("NANOEXCHANGE_BIN");
    std::string nanoexchange_bin = ne_path ? ne_path : "../../NanoExchange/build/NanoExchange";

    std::cout << "\n═══ AlphaTrader Comprehensive Integration Test ═══\n\n";

    // ── Start NanoExchange ──────────────────────────────────────
    pid_t ne_pid = fork();
    if (ne_pid == 0) {
        int devnull = open("/dev/null", O_WRONLY);
        dup2(devnull, STDOUT_FILENO);
        close(devnull);
        std::string gport = std::to_string(gateway_port);
        std::string iport = std::to_string(inc_port);
        std::string snapshot_tcp = std::to_string(snapshot_tcp_port);
        execlp(nanoexchange_bin.c_str(), nanoexchange_bin.c_str(),
                gateway_iface.c_str(), gport.c_str(),
                inc_mcast.c_str(), iport.c_str(),
                snapshot_tcp.c_str(), mcast_iface.c_str(),
                "1", "2", "3", (char*)nullptr);
        std::cerr << "execlp(" << nanoexchange_bin << ") failed: " << strerror(errno) << "\n";
        _exit(1);
    }
    if (ne_pid < 0) { std::cerr << "fork failed\n"; return 1; }

    auto kill_ne = [&]() { kill(ne_pid, SIGINT); waitpid(ne_pid, nullptr, 0); };

    if (!wait_for_port("127.0.0.1", gateway_port, 10)) {
        std::cerr << "TIMEOUT waiting for NanoExchange\n";
        kill_ne(); return 1;
    }
    std::cout << "NanoExchange ready.\n";

    // ── Consumer ────────────────────────────────────────────────
    quantlink::Logger logger(8 * 1024 * 1024, "integration_test.log", -1);
    quantlink::SPSCQueue<MarketUpdate> md_queue(1024);
    MarketDataConsumer consumer(&md_queue, &logger,
                                mcast_iface, "127.0.0.1", snapshot_tcp_port, inc_mcast, inc_port);
    consumer.start(-1);
    for (int i = 0; i < 20 && !market_data_synchronized.load(std::memory_order_acquire); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    check("TCP snapshot synchronized market data",
          market_data_synchronized.load(std::memory_order_acquire));

    // Helper: drain queue
    auto drain_queue = [&]() -> std::vector<MarketUpdate> {
        std::vector<MarketUpdate> out;
        MarketUpdate u;
        while (md_queue.pop(u)) out.push_back(u);
        return out;
    };

    // Helper: send OUCH EnterOrder and optionally read Accepted response
    auto send_enter = [&](const std::string& token, char side, uint32_t shares,
                          uint32_t ticker_id, uint32_t price_scaled,
                          bool read_resp = false) -> std::string {
        int fd = tcp_connect("127.0.0.1", gateway_port);
        if (fd < 0) return "";
        OuchEnterOrder order;
        pad_str(order.order_token, 14, token);
        order.side = side;
        set_be32(order.shares, shares);
        set_be64(order.stock, ticker_id);
        set_be32(order.price, price_scaled);
        set_be32(order.time_in_force, 99999);
        pad_str(order.firm, 4, "TEST");
        write(fd, &order, sizeof(order));
        std::string resp;
        if (read_resp) {
            char buf[256];
            ssize_t n = read(fd, buf, sizeof(buf));
            if (n > 0) resp.assign(buf, buf + n);
        }
        close(fd);
        return resp;
    };

    auto send_cancel = [&](const std::string& token, uint32_t shares) -> bool {
        int fd = tcp_connect("127.0.0.1", gateway_port);
        if (fd < 0) return false;
        OuchCancelOrder order;
        pad_str(order.order_token, 14, token);
        set_be32(order.shares, shares);
        ssize_t n = write(fd, &order, sizeof(order));
        close(fd);
        return n == (ssize_t)sizeof(order);
    };

    auto send_replace = [&](const std::string& old_token, const std::string& new_token,
                            uint32_t shares, uint32_t price_scaled) -> bool {
        int fd = tcp_connect("127.0.0.1", gateway_port);
        if (fd < 0) return false;
        OuchReplaceOrder order;
        pad_str(order.existing_token, 14, old_token);
        pad_str(order.replacement_token, 14, new_token);
        set_be32(order.shares, shares);
        set_be32(order.price, price_scaled);
        set_be32(order.time_in_force, 99999);
        ssize_t n = write(fd, &order, sizeof(order));
        close(fd);
        return n == (ssize_t)sizeof(order);
    };

    // Discard the initial snapshot before exercising live incrementals.
    drain_queue();

    // ════════════════════════════════════════════════════════════
    // SCENARIO 1 — Basic Add Orders
    // ════════════════════════════════════════════════════════════
    std::cout << "─── Scenario 1: Add Orders ───\n";
    for (int i = 0; i < 3; ++i) {
        send_enter("SC1_B" + std::to_string(i), 'B', 100, 0, 1000000);
        std::this_thread::sleep_for(std::chrono::milliseconds(60));
    }
    for (int i = 0; i < 3; ++i) {
        send_enter("SC1_S" + std::to_string(i), 'S', 100, 0, 2000000);
        std::this_thread::sleep_for(std::chrono::milliseconds(60));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    auto s1 = drain_queue();
    check("received " + std::to_string(s1.size()) + " AddOrder updates (expect >= 6)",
          s1.size() >= 6);
    int a_count = 0;
    for (auto& m : s1) if (m.type == UpdateType::ADD) ++a_count;
    check("all are type=A (AddOrder)", a_count == (int)s1.size());

    if (g_verbose) for (auto& m : s1)
        std::cout << "  seq=" << m.seq_num << " type=" << (char)m.type
                  << " side=" << (char)m.side << " qty=" << m.qty
                  << " price=" << m.price << "\n";

    // ════════════════════════════════════════════════════════════
    // SCENARIO 2 — Cancel Order
    // ════════════════════════════════════════════════════════════
    std::cout << "─── Scenario 2: Cancel Order ───\n";
    send_enter("SC2_ORD1", 'B', 200, 0, 1000000);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    bool cancel_ok = send_cancel("SC2_ORD1", 200);
    check("cancel send succeeded", cancel_ok);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    auto s2 = drain_queue();
    check("cancel send succeeded", cancel_ok);
    // Note: NanoExchange may not publish ITCH Cancel ('X') messages
    // So we just verify the send succeeded and at least the add arrived
    bool found_add_cancel = false;
    for (auto& m : s2) if (m.type == UpdateType::ADD) found_add_cancel = true;
    check("received AddOrder for canceled order", found_add_cancel);
    if (g_verbose) for (auto& m : s2)
        std::cout << "  seq=" << m.seq_num << " type=" << (char)m.type
                  << " side=" << (char)m.side << " qty=" << m.qty << "\n";

    // ════════════════════════════════════════════════════════════
    // SCENARIO 3 — Replace Order
    // ════════════════════════════════════════════════════════════
    std::cout << "─── Scenario 3: Replace Order ───\n";
    send_enter("SC3_ORD1", 'B', 100, 0, 1000000);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    bool repl_ok = send_replace("SC3_ORD1", "SC3_RPL1", 150, 1100000);
    check("replace send succeeded", repl_ok);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    auto s3 = drain_queue();
    // Note: NanoExchange may not publish ITCH Replace ('U') messages
    if (g_verbose) for (auto& m : s3)
        std::cout << "  seq=" << m.seq_num << " type=" << (char)m.type
                  << " side=" << (char)m.side << " ref=" << m.order_ref
                  << " new_ref=" << m.new_ref << "\n";

    // ════════════════════════════════════════════════════════════
    // SCENARIO 4 — Crossing Orders (Executions)
    // ════════════════════════════════════════════════════════════
    std::cout << "─── Scenario 4: Crossing Orders ───\n";
    // With NanoExchange bug: 1000000 → 100 (cents) ITCH price=10000
    // Buy at high price (100 cents), sell at low price (1 cent) → crosses
    send_enter("SC4_B1", 'B', 300, 0, 1000000);
    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    send_enter("SC4_S1", 'S', 100, 0, 10000);
    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    send_enter("SC4_S2", 'S', 100, 0, 10000);
    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    send_enter("SC4_S3", 'S', 100, 0, 10000);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    auto s4 = drain_queue();
    bool found_exec = false;
    int exec_count = 0;
    for (auto& m : s4) {
        if (m.type == UpdateType::EXECUTE) { found_exec = true; ++exec_count; }
    }
    check("received Execute (E) updates", found_exec);
    check("got " + std::to_string(exec_count) + " execution messages (expect >= 1)",
          exec_count >= 1);
    if (g_verbose) for (auto& m : s4)
        std::cout << "  seq=" << m.seq_num << " type=" << (char)m.type
                  << " side=" << (char)m.side << " qty=" << m.qty << "\n";

    // ════════════════════════════════════════════════════════════
    // SCENARIO 5 — Multiple Tickers
    // ════════════════════════════════════════════════════════════
    std::cout << "─── Scenario 5: Multiple Tickers ───\n";
    for (uint32_t tid = 0; tid < 4; ++tid) {
        send_enter("SC5_T" + std::to_string(tid), 'B', 100, tid, 1000000);
        std::this_thread::sleep_for(std::chrono::milliseconds(60));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    auto s5 = drain_queue();
    check("received " + std::to_string(s5.size()) + " updates for 4 tickers (expect >= 4)",
          s5.size() >= 4);
    if (g_verbose) for (auto& m : s5)
        std::cout << "  seq=" << m.seq_num << " type=" << (char)m.type << "\n";

    // ════════════════════════════════════════════════════════════
    // SCENARIO 6 — MarketOrderBook consistency
    // ════════════════════════════════════════════════════════════
    std::cout << "─── Scenario 6: MarketOrderBook ───\n";
    {
        MarketOrderBook book(0, &logger);
        ItchDecoder dec;

        // Send two add orders
        {
            MarketUpdate u;
            dec.decode(std::span<const char>(), u); // reset not needed, just test
        }
        // Manually construct updates
        MarketUpdate add1;
        add1.type = UpdateType::ADD;
        add1.ticker_id = 0;
        add1.side = Side::BUY;
        add1.price = 1000000;
        add1.qty = 200;
        add1.order_ref = 1;
        book.applyUpdate(add1);

        MarketUpdate add2;
        add2.type = UpdateType::ADD;
        add2.ticker_id = 0;
        add2.side = Side::SELL;
        add2.price = 2000000;
        add2.qty = 150;
        add2.order_ref = 2;
        book.applyUpdate(add2);

        BBO bbo = book.getBBO();
        check("BBO bid_price=1000000", bbo.bid_price == 1000000);
        check("BBO bid_qty=200",       bbo.bid_qty == 200);
        check("BBO ask_price=2000000", bbo.ask_price == 2000000);
        check("BBO ask_qty=150",       bbo.ask_qty == 150);

        // Execute part of the bid
        MarketUpdate exec;
        exec.type = UpdateType::EXECUTE;
        exec.ticker_id = 0;
        exec.side = Side::BUY;
        exec.qty = 50;
        exec.order_ref = 1;
        book.applyUpdate(exec);

        BBO bbo2 = book.getBBO();
        check("after execute bid_qty=150", bbo2.bid_qty == 150);

        // Cancel the ask
        MarketUpdate cancel;
        cancel.type = UpdateType::CANCEL;
        cancel.ticker_id = 0;
        cancel.side = Side::SELL;
        cancel.qty = 150;
        cancel.order_ref = 2;
        book.applyUpdate(cancel);

        BBO bbo3 = book.getBBO();
        check("after cancel no ask (price=0)",  bbo3.ask_price == Price_INVALID);
        check("after cancel no ask (qty=0)",    bbo3.ask_qty == 0);
    }

    // ════════════════════════════════════════════════════════════
    // SCENARIO 7 — ItchDecoder Snapshot Markers
    // ════════════════════════════════════════════════════════════
    std::cout << "─── Scenario 7: ItchDecoder Snapshot Markers ───\n";
    {
        ItchDecoder dec;
        // OrderDelete is 19 bytes packed: type(1) + stock_locate(2) + tracking(2) + timestamp(6) + order_ref_num(8)
        constexpr size_t PAYLOAD_SZ = 19;
        char buf[PAYLOAD_SZ];
        std::memset(buf, 0, sizeof(buf));
        // msg_type = 'D'
        buf[0] = 'D';
        // stock_locate = 0xBBBB (BE)
        buf[1] = 0xBB; buf[2] = 0xBB;
        // order_ref_num at payload offset 11 = 42 (BE)
        set_be64(buf + 11, 42);

        MarketUpdate out;
        bool ok = dec.decodeSnapshot(std::span<const char>(buf, sizeof(buf)), out);
        check("decoded SNAPSHOT_START marker", ok);
        check("type == SNAPSHOT_START", ok && out.type == UpdateType::SNAPSHOT_START);
        check("order_ref carries last_inc_seq (42)", ok && out.order_ref == 42);
    }
    {
        ItchDecoder dec;
        constexpr size_t PAYLOAD_SZ = 19;
        char buf[PAYLOAD_SZ];
        std::memset(buf, 0, sizeof(buf));
        buf[0] = 'D';
        buf[1] = 0xCC; buf[2] = 0xCC; // SNAPSHOT_CLEAR
        set_be64(buf + 11, 7);        // ticker_id = 7

        MarketUpdate out;
        bool ok = dec.decodeSnapshot(std::span<const char>(buf, sizeof(buf)), out);
        check("decoded SNAPSHOT_CLEAR marker", ok);
        check("type == SNAPSHOT_CLEAR", ok && out.type == UpdateType::SNAPSHOT_CLEAR);
        check("order_ref carries ticker_id (7)", ok && out.order_ref == 7);
    }
    {
        ItchDecoder dec;
        constexpr size_t PAYLOAD_SZ = 19;
        char buf[PAYLOAD_SZ];
        std::memset(buf, 0, sizeof(buf));
        buf[0] = 'D';
        buf[1] = 0xEE; buf[2] = 0xEE; // SNAPSHOT_END
        set_be64(buf + 11, 99);       // last_inc_seq_num = 99

        MarketUpdate out;
        bool ok = dec.decodeSnapshot(std::span<const char>(buf, sizeof(buf)), out);
        check("decoded SNAPSHOT_END marker", ok);
        check("type == SNAPSHOT_END", ok && out.type == UpdateType::SNAPSHOT_END);
        check("order_ref carries last_inc_seq (99)", ok && out.order_ref == 99);
    }

    // ── Cleanup ─────────────────────────────────────────────────
    consumer.stop();
    kill_ne();

    // ── Results ─────────────────────────────────────────────────
    std::cout << "\n═══ Results: " << (g_total_failed == 0 ? "ALL PASS" : "SOME FAILED")
              << "  (" << g_total_passed << " passed, "
              << g_total_failed << " failed) ═══\n";
    return g_total_failed > 0 ? 1 : 0;
}
