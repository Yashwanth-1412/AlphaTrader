#include "order_gateway.h"

#include <cstring>

namespace alphatrader {

namespace {

using namespace quantlink::ouch;

auto toWire32(uint32_t value) noexcept -> uint32_t {
    return swap32(value);
}
auto toWire64(uint64_t value) noexcept -> uint64_t {
    return swap64(value);
}

} // namespace

OrderGateway::OrderGateway(quantlink::SPSCQueue<OrderRequest>* outgoing, quantlink::SPSCQueue<OrderResponse>* incoming, quantlink::Logger* logger, const std::string& ip, int port,
                           const std::string& iface)
    : outgoing_requests_(outgoing),
      incoming_responses_(incoming),
      logger_(logger),
      ip_(ip),
      port_(port),
      iface_(iface),
      tcp_socket_(*logger_) {
    tcp_socket_.recv_callback_ = [this](auto* socket, auto rx_time) { recvCallback(socket, rx_time); };
}

OrderGateway::~OrderGateway() {
    stop();
}

auto OrderGateway::start(int core_id) -> void {
    if (run_.exchange(true, std::memory_order_acq_rel))
        return;

    if (tcp_socket_.connect(ip_, iface_, port_, false) < 0) {
        run_.store(false, std::memory_order_release);
        logger_->log("OrderGateway: failed to connect to ip=% port=%\n", ntohl(inet_addr(ip_.c_str())), port_);
        return;
    }

    thread_ = quantlink::utils::create_and_pin_thread(core_id, "order_gateway", [this]() { run(); });
}

auto OrderGateway::stop() -> void {
    run_.store(false, std::memory_order_release);
    if (thread_.joinable())
        thread_.join();
}

auto OrderGateway::makeToken(OrderId client_order_id, char (&token)[14]) noexcept -> void {
    const uint64_t wire_id = toWire64(client_order_id);
    std::memcpy(token, &wire_id, sizeof(wire_id));
    std::memset(token + sizeof(wire_id), 0, sizeof(token) - sizeof(wire_id));
}

auto OrderGateway::parseToken(const char (&token)[14]) noexcept -> OrderId {
    uint64_t wire_id = 0;
    std::memcpy(&wire_id, token, sizeof(wire_id));
    return toWire64(wire_id);
}

auto OrderGateway::writeTicker(TickerId ticker_id, char (&stock)[8]) noexcept -> void {
    const uint64_t wire_ticker = toWire64(ticker_id);
    std::memcpy(stock, &wire_ticker, sizeof(wire_ticker));
}

auto OrderGateway::sendEnter(const OrderRequest& req) -> void {
    ASSERT(req.ticker_id != TickerId_INVALID, "new order requires a ticker id");

    EnterOrder msg{};
    std::memset(&msg, ' ', sizeof(msg));
    msg.type = enums::MsgType::ENTER_ORDER;
    makeToken(req.client_order_id, msg.order_token);
    msg.buy_sell_indicator = static_cast<char>(req.side);
    msg.shares             = toWire32(req.qty);
    writeTicker(req.ticker_id, msg.stock);

    ASSERT(req.price >= 0, "order price must be non-negative");
    msg.price         = toWire32(static_cast<uint32_t>(req.price));
    msg.time_in_force = toWire32(req.ioc ? 0U : 99998U);
    std::memcpy(msg.firm, "ALPH", sizeof(msg.firm));
    msg.display           = 'Y';
    msg.capacity          = 'A';
    msg.intermarket_sweep = 'N';
    msg.cross_type        = 'N';
    msg.customer_type     = 'R';
    tcp_socket_.send(&msg, sizeof(msg));
}

auto OrderGateway::sendCancel(const OrderRequest& req) -> void {
    CancelOrder msg{};
    msg.type = enums::MsgType::CANCEL_ORDER;
    makeToken(req.client_order_id, msg.order_token);
    msg.shares = toWire32(req.qty);
    tcp_socket_.send(&msg, sizeof(msg));
}

auto OrderGateway::sendReplace(const OrderRequest& req) -> void {
    ASSERT(req.new_client_order_id != 0, "replace order requires a replacement client order id");
    ASSERT(req.price >= 0, "order price must be non-negative");

    ReplaceOrder msg{};
    std::memset(&msg, ' ', sizeof(msg));
    msg.type = enums::MsgType::REPLACE_ORDER;
    makeToken(req.client_order_id, msg.existing_order_token);
    makeToken(req.new_client_order_id, msg.replacement_order_token);
    msg.shares            = toWire32(req.qty);
    msg.price             = toWire32(static_cast<uint32_t>(req.price));
    msg.time_in_force     = toWire32(req.ioc ? 0U : 99998U);
    msg.display           = 'Y';
    msg.intermarket_sweep = 'N';
    tcp_socket_.send(&msg, sizeof(msg));
}

auto OrderGateway::run() noexcept -> void {
    while (run_.load(std::memory_order_acquire)) {
        tcp_socket_.sendAndRecv();

        OrderRequest req;
        while (outgoing_requests_->pop(req)) {
            switch (req.type) {
            case OrderRequestType::NEW:
                sendEnter(req);
                break;
            case OrderRequestType::CANCEL:
                sendCancel(req);
                break;
            case OrderRequestType::REPLACE:
                sendReplace(req);
                break;
            }
        }
    }
}

auto OrderGateway::messageSize(char type) noexcept -> size_t {
    switch (static_cast<enums::MsgType>(type)) {
    case enums::MsgType::ORDER_ACCEPTED:
        return sizeof(OrderAccepted);
    case enums::MsgType::ORDER_EXECUTED:
        return sizeof(OrderExecuted);
    case enums::MsgType::ORDER_CANCELED:
        return sizeof(OrderCanceled);
    case enums::MsgType::REPLACE_ORDER:
        return sizeof(OrderReplaced);
    case enums::MsgType::CANCEL_REJECTED:
        return sizeof(CancelRejected);
    default:
        return 0;
    }
}

auto OrderGateway::recvCallback(quantlink::TCPSocket* socket, quantlink::Nanos) noexcept -> void {
    char*   data      = socket->inbound_data_.data();
    size_t& available = socket->next_rcv_valid_index_;
    size_t  offset    = 0;

    while (offset < available) {
        const char   type = data[offset];
        const size_t size = messageSize(type);
        if (size == 0) {
            logger_->log("OrderGateway: unknown OUCH response type=%\n", static_cast<int>(type));
            ++offset;
            continue;
        }
        if (available - offset < size)
            break;

        OrderResponse response{};
        response.status = OrderResponse::REJECTED;

        switch (static_cast<enums::MsgType>(type)) {
        case enums::MsgType::ORDER_ACCEPTED: {
            const auto& msg          = *reinterpret_cast<const OrderAccepted*>(data + offset);
            response.client_order_id = parseToken(msg.order_token);
            response.market_order_id = toWire64(msg.order_reference_number);
            response.side            = static_cast<Side>(msg.buy_sell_indicator);
            response.price           = static_cast<Price>(toWire32(msg.price));
            response.qty             = toWire32(msg.shares);
            response.status          = OrderResponse::ACCEPTED;
            break;
        }
        case enums::MsgType::ORDER_EXECUTED: {
            const auto& msg          = *reinterpret_cast<const OrderExecuted*>(data + offset);
            response.client_order_id = parseToken(msg.order_token);
            response.market_order_id = toWire64(msg.match_number);
            response.executed_qty    = toWire32(msg.executed_shares);
            response.execution_price = static_cast<Price>(toWire32(msg.execution_price));
            response.status          = OrderResponse::EXECUTED;
            break;
        }
        case enums::MsgType::ORDER_CANCELED: {
            const auto& msg          = *reinterpret_cast<const OrderCanceled*>(data + offset);
            response.client_order_id = parseToken(msg.order_token);
            response.canceled_qty    = toWire32(msg.decrement_shares);
            response.status          = OrderResponse::CANCELED;
            break;
        }
        case enums::MsgType::REPLACE_ORDER: {
            const auto& msg          = *reinterpret_cast<const OrderReplaced*>(data + offset);
            response.client_order_id = parseToken(msg.replacement_order_token);
            response.market_order_id = toWire64(msg.order_reference_number);
            response.side            = static_cast<Side>(msg.buy_sell_indicator);
            response.price           = static_cast<Price>(toWire32(msg.price));
            response.qty             = toWire32(msg.shares);
            response.status          = OrderResponse::ACCEPTED;
            break;
        }
        case enums::MsgType::CANCEL_REJECTED: {
            const auto& msg          = *reinterpret_cast<const CancelRejected*>(data + offset);
            response.client_order_id = parseToken(msg.order_token);
            response.status          = OrderResponse::CANCEL_REJECTED;
            break;
        }
        default:
            break;
        }

        if (!incoming_responses_->push(response)) {
            logger_->log("OrderGateway: response queue full, dropping order=%\n", response.client_order_id);
        }
        offset += size;
    }

    if (offset > 0) {
        std::memmove(data, data + offset, available - offset);
        available -= offset;
    }
}

} // namespace alphatrader
