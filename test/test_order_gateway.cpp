#include "QuantLink/Lib/common/macros.h"
#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "QuantLink/Lib/logging/logger.h"
#include "QuantLink/Lib/protocol/ouch_messages.h"
#include "order_gateway/order_gateway.h"

#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

using namespace alphatrader;
using namespace quantlink::ouch;

namespace {

int passes   = 0;
int failures = 0;

auto check(const char* description, bool condition) -> void {
    std::cout << (condition ? "PASS: " : "FAIL: ") << description << '\n';
    condition ? ++passes : ++failures;
}

auto fromWire32(uint32_t value) noexcept -> uint32_t {
    return swap32(value);
}
auto toWire64(uint64_t value) noexcept -> uint64_t {
    return swap64(value);
}

auto readAll(int fd, void* buffer, size_t length) -> bool {
    auto*  bytes    = static_cast<char*>(buffer);
    size_t received = 0;
    while (received < length) {
        const ssize_t n = recv(fd, bytes + received, length - received, 0);
        if (n <= 0)
            return false;
        received += static_cast<size_t>(n);
    }
    return true;
}

auto writeAll(int fd, const void* buffer, size_t length) -> bool {
    const auto* bytes = static_cast<const char*>(buffer);
    size_t      sent  = 0;
    while (sent < length) {
        const ssize_t n = send(fd, bytes + sent, length - sent, MSG_NOSIGNAL);
        if (n <= 0)
            return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

auto parseToken(const char (&token)[14]) noexcept -> OrderId {
    uint64_t wire_id = 0;
    std::memcpy(&wire_id, token, sizeof(wire_id));
    return toWire64(wire_id);
}

auto tickerFromStock(const char (&stock)[8]) noexcept -> TickerId {
    uint64_t ticker = 0;
    std::memcpy(&ticker, stock, sizeof(ticker));
    return static_cast<TickerId>(swap64(ticker));
}

auto testServer(int port, std::atomic<bool>& ready, std::atomic<bool>& protocol_ok) -> void {
    const int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT(listen_fd >= 0, "test server socket");

    int one = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in address{};
    address.sin_family      = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port        = htons(port);
    ASSERT(bind(listen_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0, "test server bind");
    ASSERT(listen(listen_fd, 1) == 0, "test server listen");
    ready.store(true, std::memory_order_release);

    const int client_fd = accept(listen_fd, nullptr, nullptr);
    if (client_fd < 0) {
        close(listen_fd);
        return;
    }

    EnterOrder   enter{};
    CancelOrder  cancel{};
    ReplaceOrder replace{};
    const bool   read_ok = readAll(client_fd, &enter, sizeof(enter)) && readAll(client_fd, &cancel, sizeof(cancel)) && readAll(client_fd, &replace, sizeof(replace));

    const bool input_ok = read_ok && enter.type == enums::MsgType::ENTER_ORDER && parseToken(enter.order_token) == 101 && enter.buy_sell_indicator == 'B' && fromWire32(enter.shares) == 25 &&
                          tickerFromStock(enter.stock) == 1 && fromWire32(enter.price) == 1234567 && fromWire32(enter.time_in_force) == 99998 && cancel.type == enums::MsgType::CANCEL_ORDER &&
                          parseToken(cancel.order_token) == 101 && fromWire32(cancel.shares) == 10 && replace.type == enums::MsgType::REPLACE_ORDER &&
                          parseToken(replace.existing_order_token) == 101 && parseToken(replace.replacement_order_token) == 102 && fromWire32(replace.shares) == 15 &&
                          fromWire32(replace.price) == 1234568 && fromWire32(replace.time_in_force) == 0;
    protocol_ok.store(input_ok, std::memory_order_release);

    if (input_ok) {
        OrderAccepted accepted{};
        std::memset(&accepted, ' ', sizeof(accepted));
        accepted.type = enums::MsgType::ORDER_ACCEPTED;
        std::memcpy(accepted.order_token, enter.order_token, sizeof(accepted.order_token));
        accepted.buy_sell_indicator     = 'B';
        accepted.shares                 = enter.shares;
        accepted.price                  = swap32(1234567);
        accepted.order_reference_number = toWire64(9001);

        OrderCanceled canceled{};
        std::memset(&canceled, ' ', sizeof(canceled));
        canceled.type = enums::MsgType::ORDER_CANCELED;
        std::memcpy(canceled.order_token, cancel.order_token, sizeof(canceled.order_token));
        canceled.decrement_shares = swap32(15);

        OrderReplaced replaced{};
        std::memset(&replaced, ' ', sizeof(replaced));
        replaced.type = enums::MsgType::REPLACE_ORDER;
        std::memcpy(replaced.replacement_order_token, replace.replacement_order_token, sizeof(replaced.replacement_order_token));
        replaced.buy_sell_indicator     = 'B';
        replaced.shares                 = replace.shares;
        replaced.price                  = swap32(1234568);
        replaced.order_reference_number = toWire64(9002);

        // Split the first message, then coalesce the remaining two.
        const size_t split = 7;
        writeAll(client_fd, &accepted, split);
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        writeAll(client_fd, reinterpret_cast<const char*>(&accepted) + split, sizeof(accepted) - split);
        writeAll(client_fd, &canceled, sizeof(canceled));
        writeAll(client_fd, &replaced, sizeof(replaced));
    }

    close(client_fd);
    close(listen_fd);
}

} // namespace

int main(int argc, char* argv[]) {
    constexpr int     port          = 22002;
    const bool        live_exchange = argc == 4 && std::strcmp(argv[1], "--nanoexchange") == 0;
    const std::string host          = live_exchange ? argv[2] : "127.0.0.1";
    const int         gateway_port  = live_exchange ? std::atoi(argv[3]) : port;
    std::atomic<bool> ready{false};
    std::atomic<bool> protocol_ok{false};
    std::thread       server;
    if (!live_exchange) {
        server = std::thread(testServer, port, std::ref(ready), std::ref(protocol_ok));
        while (!ready.load(std::memory_order_acquire))
            std::this_thread::yield();
    }

    quantlink::SPSCQueue<OrderRequest>  requests(16);
    quantlink::SPSCQueue<OrderResponse> responses(16);
    quantlink::Logger                   logger(1024, "order_gateway_test.log", -1);

    OrderGateway gateway(&requests, &responses, &logger, host, gateway_port, "lo");
    gateway.start();

    requests.push(OrderRequest{OrderRequestType::NEW, 101, 0, 1, Side::BUY, 1234567, 25, false});
    requests.push(OrderRequest{OrderRequestType::CANCEL, 101, 0, TickerId_INVALID, Side::BUY, 0, 10, false});
    if (!live_exchange) {
        requests.push(OrderRequest{OrderRequestType::REPLACE, 101, 102, TickerId_INVALID, Side::BUY, 1234568, 15, true});
    }

    OrderResponse received[3]{};
    size_t        count    = 0;
    const size_t  expected = live_exchange ? 2 : 3;
    const auto    deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (count < expected && std::chrono::steady_clock::now() < deadline) {
        if (!responses.pop(received[count])) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        ++count;
    }

    gateway.stop();
    if (server.joinable())
        server.join();

    if (!live_exchange)
        check("NanoExchange inbound OUCH frames", protocol_ok.load(std::memory_order_acquire));
    check(live_exchange ? "received two NanoExchange responses" : "received three OUCH responses", count == expected);
    check("accepted response", count > 0 && received[0].client_order_id == 101 && received[0].market_order_id != 0 && received[0].price == 1234567 && received[0].qty == 25 &&
                                   received[0].status == OrderResponse::ACCEPTED && (live_exchange || received[0].market_order_id == 9001));
    check("canceled response", count > 1 && received[1].client_order_id == 101 && received[1].status == OrderResponse::CANCELED && (live_exchange || received[1].canceled_qty == 15));
    if (!live_exchange) {
        check("replaced response", count > 2 && received[2].client_order_id == 102 && received[2].market_order_id == 9002 && received[2].price == 1234568 && received[2].qty == 15 &&
                                       received[2].status == OrderResponse::ACCEPTED);
    }

    unlink("order_gateway_test.log");
    std::cout << "Results: " << passes << " passed, " << failures << " failed\n";
    return failures == 0 ? 0 : 1;
}
