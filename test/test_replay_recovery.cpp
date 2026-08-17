#include "QuantLink/Lib/logging/logger.h"
#include "market_data/itch_decoder.h"
#include "market_data/market_data.h"
#include "market_order_book.h"

#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace alphatrader;

// ── Helpers ────────────────────────────────────────────────────

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

static bool send_all(int fd, const char* data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        const ssize_t n = send(fd, data + sent, len - sent, MSG_NOSIGNAL);
        if (n <= 0)
            return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

// Incremental wire frame: seq(8 BE) + ITCH AddOrder(36)
static auto make_add_order(uint64_t seq, uint64_t order_ref, char side, uint32_t shares, uint32_t price_itch, uint32_t ticker_id) -> std::vector<char> {
    constexpr size_t  MSG_SZ = 8 + 36;
    std::vector<char> buf(MSG_SZ, 0);
    uint64_t          seq_be = htobe64(seq);
    std::memcpy(buf.data(), &seq_be, 8);
    buf[8]          = 'A';
    uint64_t ref_be = htobe64(order_ref);
    std::memcpy(buf.data() + 8 + 11, &ref_be, 8);
    buf[8 + 19]        = side;
    uint32_t shares_be = htonl(shares);
    std::memcpy(buf.data() + 8 + 20, &shares_be, 4);
    uint64_t ticker_be = htobe64(ticker_id);
    std::memcpy(buf.data() + 8 + 24, &ticker_be, 8);
    uint32_t price_be = htonl(price_itch);
    std::memcpy(buf.data() + 8 + 32, &price_be, 4);
    return buf;
}

// Snapshot frames: raw ITCH (no seq prefix)
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

// ── Scripted mock server ───────────────────────────────────────
// Handles a fixed sequence of expected requests ('S' or 'R'); each response
// can be gated behind an atomic flag and records the request bytes observed.
struct ScriptedServer {
    enum class Req {
        Snapshot,
        Replay
    };

    struct Handler {
        Req                                                   kind;
        std::atomic<bool>*                                    gate = nullptr;
        std::function<void(int client_fd, SeqNum client_seq)> respond;
    };

    std::vector<Handler> handlers;
    std::vector<char>    observed_request_bytes;
    std::atomic<bool>    started{false};
    std::thread          thread;
    int                  listen_fd = -1;

    explicit ScriptedServer(int port, std::vector<Handler> hs)
        : handlers(std::move(hs)) {
        listen_fd = socket(AF_INET, SOCK_STREAM, 0);
        int reuse = 1;
        setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port        = htons(port);
        if (bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            std::cerr << "  [server] bind failed\n";
            exit(1);
        }
        listen(listen_fd, 8);
    }

    auto start() -> void {
        thread = std::thread([this] {
            for (const Handler& h : handlers) {
                const int client     = accept(listen_fd, nullptr, nullptr);
                SeqNum    client_seq = 0;
                if (h.kind == Req::Snapshot) {
                    char request = 0;
                    if (!recv_exact(client, &request, 1)) {
                        close(client);
                        continue;
                    }
                    observed_request_bytes.push_back(request);
                } else {
                    char request[1 + sizeof(SeqNum)];
                    if (!recv_exact(client, request, sizeof(request))) {
                        close(client);
                        continue;
                    }
                    observed_request_bytes.push_back(request[0]);
                    std::memcpy(&client_seq, request + 1, sizeof(client_seq));
                    client_seq = quantlink::itch::swap64(client_seq);
                }
                if (h.gate != nullptr) {
                    while (!h.gate->load(std::memory_order_acquire)) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    }
                }
                h.respond(client, client_seq);
                close(client);
            }
            close(listen_fd);
        });
    }

    auto join() -> void {
        if (thread.joinable())
            thread.join();
    }
    ~ScriptedServer() { join(); }
};

// ── Scenario runner ────────────────────────────────────────────
struct Scenario {
    std::string                          name;
    std::vector<ScriptedServer::Handler> handlers;
    std::function<void()>                run;
};

int main() {
    std::cout << "\n═══ Replay Recovery Path Test ═══\n\n";

    const std::string iface    = "lo";
    const std::string inc_ip   = "127.0.0.1";
    constexpr int     TCP_PORT = 21004;
    constexpr int     INC_PORT = 21005;
    quantlink::Logger logger(1024 * 1024, "replay_test.log", -1);

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

    // Shared snapshot / replay wire content.
    std::atomic<bool> release_escalated_snapshot{false};
    const auto        snap_start    = make_snap_order_delete(0xBBBB, 7);
    const auto        snap_clear    = make_snap_order_delete(0xCCCC, 0);
    const auto        snap_add      = make_snap_add_order(200, 'B', 500, 1200000, 0);
    const auto        snap_end      = make_snap_order_delete(0xEEEE, 7);
    const auto        initial_start = make_snap_order_delete(0xBBBB, 0);
    const auto        initial_clear = make_snap_order_delete(0xCCCC, 0);
    const auto        initial_end   = make_snap_order_delete(0xEEEE, 0);

    auto initial_snapshot = [&](int client_fd, SeqNum) {
        send_all(client_fd, initial_start.data(), initial_start.size());
        send_all(client_fd, initial_clear.data(), initial_clear.size());
        send_all(client_fd, initial_end.data(), initial_end.size());
    };

    std::vector<Scenario> scenarios;

    // ──────────────────────────────────────────────────────────
    // A: mid-stream cut — server closes with a partial frame;
    //    client re-requests from the last applied seq.
    // ──────────────────────────────────────────────────────────
    scenarios.push_back({"A: mid-stream cut (re-request from last applied)",
                         {
                             {ScriptedServer::Req::Snapshot, nullptr, initial_snapshot},
                             {ScriptedServer::Req::Replay, nullptr,
                              [](int client_fd, SeqNum) {
                                  std::vector<char> reply{'R'};
                                  auto              f3 = make_add_order(3, 103, 'B', 100, 1100000, 0);
                                  reply.insert(reply.end(), f3.begin(), f3.end());
                                  auto f4 = make_add_order(4, 104, 'B', 200, 1050000, 0);
                                  reply.insert(reply.end(), f4.begin(), f4.begin() + 20); // partial frame
                                  send_all(client_fd, reply.data(), reply.size());
                                  // close → client sees EOF with a partial frame → re-requests
                              }},
                             {ScriptedServer::Req::Replay, nullptr,
                              [](int client_fd, SeqNum /*client_seq*/) {
                                  std::vector<char> reply{'R'};
                                  for (uint64_t seq = 4; seq <= 6; ++seq) {
                                      auto f = make_add_order(seq, 100 + seq, 'B', 100, 1000000, 0);
                                      reply.insert(reply.end(), f.begin(), f.end());
                                  }
                                  send_all(client_fd, reply.data(), reply.size());
                              }},
                         },
                         [&]() {
                             quantlink::SPSCQueue<MarketUpdate> md_queue(4096);
                             MarketDataConsumer                 consumer(&md_queue, &logger, iface, "127.0.0.1", TCP_PORT, inc_ip, INC_PORT, INC_PORT);
                             consumer.start(-1);
                             auto drain = [&]() {
                                 std::vector<MarketUpdate> out;
                                 MarketUpdate              u;
                                 while (md_queue.pop(u))
                                     out.push_back(u);
                                 return out;
                             };

                             std::this_thread::sleep_for(std::chrono::milliseconds(300));
                             drain();
                             check("initial snapshot synchronizes", market_data_synchronized.load());

                             auto p1 = make_add_order(1, 100, 'B', 200, 1000000, 0);
                             auto p2 = make_add_order(2, 101, 'S', 150, 2000000, 0);
                             udp_send(inc_ip, INC_PORT, p1.data(), p1.size());
                             udp_send(inc_ip, INC_PORT, p2.data(), p2.size());
                             std::this_thread::sleep_for(std::chrono::milliseconds(200));
                             check("normal incrementals 1,2", drain().size() == 2);

                             auto p7 = make_add_order(7, 102, 'B', 300, 1500000, 0);
                             udp_send(inc_ip, INC_PORT, p7.data(), p7.size());
                             std::this_thread::sleep_for(std::chrono::milliseconds(800));

                             auto s3 = drain();
                             check("recovery completed via re-request (5 updates)", s3.size() == 5);
                             bool contiguous = s3.size() == 5;
                             for (size_t i = 0; i < s3.size(); ++i)
                                 contiguous = contiguous && (s3[i].seq_num == static_cast<SeqNum>(3 + i));
                             check("replayed updates contiguous (seq 3..7)", contiguous);
                             check("market data synchronized after recovery", market_data_synchronized.load());
                             MarketOrderBook book(0, &logger);
                             for (const auto& u : s3)
                                 book.applyUpdate(u);
                             check("book rebuilt (5 orders, best bid 1.5M/300)", book.trackedOrders() == 5 && book.getBBO().bid_price == 1500000 && book.getBBO().bid_qty == 300);
                             consumer.stop();
                         }});

    // ──────────────────────────────────────────────────────────
    // B: replay denied ('N') → client escalates to snapshot.
    // ──────────────────────────────────────────────────────────
    scenarios.push_back({"B: 'N' denied → escalates to snapshot",
                         {
                             {ScriptedServer::Req::Snapshot, nullptr, initial_snapshot},
                             {ScriptedServer::Req::Replay, nullptr,
                              [](int client_fd, SeqNum) {
                                  const char status = 'N';
                                  send_all(client_fd, &status, 1);
                              }},
                             {ScriptedServer::Req::Snapshot, &release_escalated_snapshot,
                              [&](int client_fd, SeqNum) {
                                  send_all(client_fd, snap_start.data(), snap_start.size());
                                  send_all(client_fd, snap_clear.data(), snap_clear.size());
                                  send_all(client_fd, snap_add.data(), snap_add.size());
                                  send_all(client_fd, snap_end.data(), snap_end.size());
                              }},
                         },
                         [&]() {
                             quantlink::SPSCQueue<MarketUpdate> md_queue(4096);
                             MarketDataConsumer                 consumer(&md_queue, &logger, iface, "127.0.0.1", TCP_PORT, inc_ip, INC_PORT, INC_PORT);
                             consumer.start(-1);
                             auto drain = [&]() {
                                 std::vector<MarketUpdate> out;
                                 MarketUpdate              u;
                                 while (md_queue.pop(u))
                                     out.push_back(u);
                                 return out;
                             };

                             std::this_thread::sleep_for(std::chrono::milliseconds(300));
                             drain();
                             auto p1 = make_add_order(1, 100, 'B', 200, 1000000, 0);
                             auto p2 = make_add_order(2, 101, 'S', 150, 2000000, 0);
                             udp_send(inc_ip, INC_PORT, p1.data(), p1.size());
                             udp_send(inc_ip, INC_PORT, p2.data(), p2.size());
                             std::this_thread::sleep_for(std::chrono::milliseconds(200));
                             drain();

                             auto p7 = make_add_order(7, 102, 'B', 300, 1500000, 0);
                             udp_send(inc_ip, INC_PORT, p7.data(), p7.size());
                             std::this_thread::sleep_for(std::chrono::milliseconds(400));
                             check("no updates pushed during denied-then-snapshot recovery", drain().empty());
                             check("market data unavailable while snapshot withheld", !market_data_synchronized.load());

                             release_escalated_snapshot.store(true, std::memory_order_release);
                             std::this_thread::sleep_for(std::chrono::milliseconds(600));
                             auto s      = drain();
                             int  starts = 0, clears = 0, ends = 0, ref200 = 0, ref102 = 0;
                             for (const auto& u : s) {
                                 if (u.type == UpdateType::SNAPSHOT_START)
                                     ++starts;
                                 if (u.type == UpdateType::SNAPSHOT_CLEAR)
                                     ++clears;
                                 if (u.type == UpdateType::SNAPSHOT_END)
                                     ++ends;
                                 if (u.order_ref == 200)
                                     ++ref200;
                                 if (u.order_ref == 102)
                                     ++ref102;
                             }
                             check("snapshot controls forwarded", starts == 1 && clears == 1 && ends == 1);
                             check("snapshot AddOrder (ref=200) applied", ref200 >= 1);
                             check("queued incremental (seq<=last_inc_seq) discarded", ref102 == 0);
                             check("market data synchronized after escalation", market_data_synchronized.load());
                             consumer.stop();
                         }});

    // ──────────────────────────────────────────────────────────
    // C: server already current ('C') with queued increments →
    //    contiguity fails → abort → next packet re-triggers replay.
    // ──────────────────────────────────────────────────────────
    scenarios.push_back({"C: 'C' aborts, next packet recovers via replay",
                         {
                             {ScriptedServer::Req::Snapshot, nullptr, initial_snapshot},
                             {ScriptedServer::Req::Replay, nullptr,
                              [](int client_fd, SeqNum) {
                                  const char status = 'C';
                                  send_all(client_fd, &status, 1);
                              }},
                             {ScriptedServer::Req::Replay, nullptr,
                              [](int client_fd, SeqNum client_seq) {
                                  std::vector<char> reply{'R'};
                                  for (uint64_t seq = client_seq + 1; seq <= client_seq + 5; ++seq) {
                                      auto f = make_add_order(seq, 100 + seq, 'B', 100, 1000000, 0);
                                      reply.insert(reply.end(), f.begin(), f.end());
                                  }
                                  send_all(client_fd, reply.data(), reply.size());
                              }},
                         },
                         [&]() {
                             quantlink::SPSCQueue<MarketUpdate> md_queue(4096);
                             MarketDataConsumer                 consumer(&md_queue, &logger, iface, "127.0.0.1", TCP_PORT, inc_ip, INC_PORT, INC_PORT);
                             consumer.start(-1);
                             auto drain = [&]() {
                                 std::vector<MarketUpdate> out;
                                 MarketUpdate              u;
                                 while (md_queue.pop(u))
                                     out.push_back(u);
                                 return out;
                             };

                             std::this_thread::sleep_for(std::chrono::milliseconds(300));
                             drain();
                             auto p1 = make_add_order(1, 100, 'B', 200, 1000000, 0);
                             auto p2 = make_add_order(2, 101, 'S', 150, 2000000, 0);
                             udp_send(inc_ip, INC_PORT, p1.data(), p1.size());
                             udp_send(inc_ip, INC_PORT, p2.data(), p2.size());
                             std::this_thread::sleep_for(std::chrono::milliseconds(200));
                             drain();

                             auto p7 = make_add_order(7, 102, 'B', 300, 1500000, 0);
                             udp_send(inc_ip, INC_PORT, p7.data(), p7.size());
                             std::this_thread::sleep_for(std::chrono::milliseconds(400));
                             check("'C' with queued increments leaves data unsynchronized", !market_data_synchronized.load());

                             auto p8 = make_add_order(8, 400, 'S', 100, 2600000, 0);
                             udp_send(inc_ip, INC_PORT, p8.data(), p8.size());
                             std::this_thread::sleep_for(std::chrono::milliseconds(600));

                             auto s = drain();
                             check("replay recovered on next gap (6 updates seq 3..8)", s.size() == 6);
                             bool contiguous = s.size() == 6;
                             for (size_t i = 0; i < s.size(); ++i)
                                 contiguous = contiguous && (s[i].seq_num == static_cast<SeqNum>(3 + i));
                             check("recovered updates contiguous", contiguous);
                             check("market data synchronized after recovery", market_data_synchronized.load());
                             consumer.stop();
                         }});

    // ──────────────────────────────────────────────────────────
    // D: REPLAY_CAPACITY boundary — gap == 8192 → 'R' (full replay
    //    of 8192 frames); gap == 8193 → 'S' (snapshot fallback).
    // ──────────────────────────────────────────────────────────
    scenarios.push_back({"D: capacity boundary (8192 → replay, 8193 → snapshot)",
                         {
                             {ScriptedServer::Req::Snapshot, nullptr, initial_snapshot},
                             {ScriptedServer::Req::Replay, nullptr,
                              [](int client_fd, SeqNum) {
                                  std::vector<char> reply;
                                  reply.reserve(1 + 8192 * 44);
                                  reply.push_back('R');
                                  for (uint64_t seq = 1; seq <= 8192; ++seq) {
                                      auto f = make_add_order(seq, 1000 + seq, 'B', 100, 1000000, 0);
                                      reply.insert(reply.end(), f.begin(), f.end());
                                  }
                                  send_all(client_fd, reply.data(), reply.size());
                              }},
                             {ScriptedServer::Req::Replay, nullptr,
                              [](int client_fd, SeqNum client_seq) {
                                  std::vector<char> reply;
                                  reply.reserve(1 + 8192 * 44);
                                  reply.push_back('R');
                                  for (uint64_t seq = client_seq + 1; seq <= client_seq + 8192; ++seq) {
                                      auto f = make_add_order(seq, 1000 + seq, 'B', 100, 1000000, 0);
                                      reply.insert(reply.end(), f.begin(), f.end());
                                  }
                                  send_all(client_fd, reply.data(), reply.size());
                              }},
                             {ScriptedServer::Req::Snapshot, nullptr,
                              [&](int client_fd, SeqNum) {
                                  const auto end = make_snap_order_delete(0xEEEE, 24580);
                                  send_all(client_fd, snap_start.data(), snap_start.size());
                                  send_all(client_fd, snap_clear.data(), snap_clear.size());
                                  send_all(client_fd, snap_add.data(), snap_add.size());
                                  send_all(client_fd, end.data(), end.size());
                              }},
                         },
                         [&]() {
                             quantlink::SPSCQueue<MarketUpdate> md_queue(16384);
                             MarketDataConsumer                 consumer(&md_queue, &logger, iface, "127.0.0.1", TCP_PORT, inc_ip, INC_PORT, INC_PORT);
                             consumer.start(-1);
                             auto drain = [&]() {
                                 std::vector<MarketUpdate> out;
                                 MarketUpdate              u;
                                 while (md_queue.pop(u))
                                     out.push_back(u);
                                 return out;
                             };

                             std::this_thread::sleep_for(std::chrono::milliseconds(300));
                             drain();

                             // gap == exactly REPLAY_CAPACITY → replay request
                             auto p_gap = make_add_order(8193, 2000, 'B', 100, 1000000, 0);
                             udp_send(inc_ip, INC_PORT, p_gap.data(), p_gap.size());
                             std::this_thread::sleep_for(std::chrono::milliseconds(1000));

                             auto s = drain();
                             check("8192-gap recovered via replay (8193 updates)", s.size() == 8193);
                             bool contiguous = true;
                             for (size_t i = 0; i < s.size(); ++i)
                                 contiguous = contiguous && (s[i].seq_num == static_cast<SeqNum>(1 + i));
                             check("all replayed updates contiguous (seq 1..8193)", contiguous);
                             check("market data synchronized after replay", market_data_synchronized.load());

                             // gap == REPLAY_CAPACITY again (expected advanced past the queued packet) → replay
                             auto p_gap2 = make_add_order(16386, 2001, 'B', 100, 1000000, 0);
                             udp_send(inc_ip, INC_PORT, p_gap2.data(), p_gap2.size());
                             std::this_thread::sleep_for(std::chrono::milliseconds(1000));

                             auto s2 = drain();
                             check("second 8192-gap recovered via replay (8193 updates)", s2.size() == 8193);
                             bool contiguous2 = true;
                             for (size_t i = 0; i < s2.size(); ++i)
                                 contiguous2 = contiguous2 && (s2[i].seq_num == static_cast<SeqNum>(8194 + i));
                             check("second replay contiguous (seq 8194..16386)", contiguous2);
                             check("market data synchronized after second replay", market_data_synchronized.load());

                             // gap == REPLAY_CAPACITY + 1 → snapshot fallback
                             auto p_big = make_add_order(24580, 2002, 'S', 100, 2000000, 0);
                             udp_send(inc_ip, INC_PORT, p_big.data(), p_big.size());
                             std::this_thread::sleep_for(std::chrono::milliseconds(800));

                             auto s3   = drain();
                             int  ends = 0, ref200 = 0;
                             for (const auto& u : s3) {
                                 if (u.type == UpdateType::SNAPSHOT_END)
                                     ++ends;
                                 if (u.order_ref == 200)
                                     ++ref200;
                             }
                             check("8193-gap recovered via snapshot fallback", ends == 1);
                             check("snapshot AddOrder (ref=200) applied", ref200 >= 1);
                             check("market data synchronized after fallback", market_data_synchronized.load());
                             consumer.stop();
                         }});

    // ── Execute scenarios sequentially ────────────────────────
    for (Scenario& sc : scenarios) {
        std::cout << "─── " << sc.name << " ───\n";
        ScriptedServer server(TCP_PORT, std::move(sc.handlers));
        server.start();
        sc.run();
        server.join();
        for (char c : server.observed_request_bytes) {
            const std::string desc = "server observed request '" + std::string(1, c) + "'";
            check(desc, c == 'S' || c == 'R');
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::cout << "\n═══ Results: " << (failed == 0 ? "ALL PASS" : "SOME FAILED") << "  (" << passed << " passed, " << failed << " failed) ═══\n";
    return failed > 0 ? 1 : 0;
}
