#pragma once

#include "types.h"
#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "QuantLink/Lib/network/tcp_socket.h"
#include "QuantLink/Lib/logging/logger.h"
#include "QuantLink/Lib/protocol/ouch_messages.h"

#include <string>
#include <atomic>
#include <thread>

namespace alphatrader {

class OrderGateway {
public:
    OrderGateway(quantlink::SPSCQueue<OrderRequest>* outgoing,
                 quantlink::SPSCQueue<OrderResponse>* incoming,
                 quantlink::Logger* logger,
                 const std::string& ip, int port,
                 const std::string& iface);

    ~OrderGateway();

    OrderGateway(const OrderGateway&) = delete;
    OrderGateway& operator=(const OrderGateway&) = delete;

    auto start() -> void;
    auto stop() -> void;

private:
    auto run() noexcept -> void;
    auto recvCallback(quantlink::TCPSocket* socket, quantlink::Nanos rx_time) noexcept -> void;

    auto sendEnter(const OrderRequest& req) -> void;
    auto sendCancel(const OrderRequest& req) -> void;
    auto sendReplace(const OrderRequest& req) -> void;

    static auto makeToken(OrderId client_order_id, char (&token)[14]) noexcept -> void;
    static auto parseToken(const char (&token)[14]) noexcept -> OrderId;
    static auto writeTicker(TickerId ticker_id, char (&stock)[8]) noexcept -> void;
    static auto messageSize(char type) noexcept -> size_t;

    quantlink::SPSCQueue<OrderRequest>* outgoing_requests_ = nullptr;
    quantlink::SPSCQueue<OrderResponse>* incoming_responses_ = nullptr;
    quantlink::Logger* logger_ = nullptr;

    std::string ip_;
    int port_ = 0;
    std::string iface_;

    quantlink::TCPSocket tcp_socket_;
    std::atomic<bool> run_{false};
    std::thread thread_;
};

} // namespace alphatrader
