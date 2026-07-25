#pragma once

#include <cstdint>
#include <limits>

namespace alphatrader {

using Price      = int32_t; // ITCH scale: 1/10000 dollar (e.g., 1500000 = $150.0000)
using Quantity   = uint32_t;
using OrderId    = uint64_t;
using SeqNum     = uint64_t;
using TickerId   = uint32_t;

constexpr Price Price_INVALID = std::numeric_limits<Price>::min();
constexpr OrderId OrderId_INVALID = std::numeric_limits<OrderId>::max();
constexpr TickerId TickerId_INVALID = std::numeric_limits<TickerId>::max();

enum class Side : char {
    BUY  = 'B',
    SELL = 'S'
};

enum class UpdateType : char {
    ADD            = 'A',
    ADD_MPID       = 'F',
    EXECUTE        = 'E',
    CANCEL         = 'X',
    DELETE         = 'D',
    REPLACE        = 'U',
    SNAPSHOT_START = 'S',
    SNAPSHOT_CLEAR = 'C',
    SNAPSHOT_END   = 'N'
};

struct MarketUpdate {
    SeqNum     seq_num   = 0;
    TickerId   ticker_id = TickerId_INVALID;
    UpdateType type      = UpdateType::ADD;
    Side       side      = Side::BUY;
    Price      price     = 0;
    Quantity   qty       = 0;
    OrderId    order_ref = 0;
    OrderId    new_ref   = 0;
};

struct OrderRequest {
    OrderId  client_order_id = 0;
    Side     side            = Side::BUY;
    Price    price           = 0;
    Quantity qty             = 0;
    bool     ioc             = false;
};

struct OrderResponse {
    OrderId  client_order_id = 0;
    OrderId  market_order_id = 0;
    Side     side            = Side::BUY;
    Price    price           = 0;
    Quantity qty             = 0;
    Quantity executed_qty    = 0;
    Price    execution_price = 0;
    Quantity leaves_qty      = 0;

    enum Status {
        ACCEPTED,
        EXECUTED,
        CANCELED,
        REJECTED
    } status = Status::REJECTED;
};

struct Position {
    int64_t net_qty      = 0;
    int64_t realized_pnl = 0;
};

struct BookOrder {
    OrderId  order_ref = 0;
    Quantity qty       = 0;
    BookOrder* next    = nullptr;
    BookOrder* prev    = nullptr;
};

struct PriceLevel {
    Price      price     = 0;
    Quantity   total_qty = 0;
    BookOrder* head      = nullptr;
    BookOrder* tail      = nullptr;
};

struct BBO {
    Price    bid_price = 0;
    Quantity bid_qty   = 0;
    Price    ask_price = 0;
    Quantity ask_qty   = 0;
};

namespace SnapshotControl {
    constexpr uint16_t SNAPSHOT_START = 0xBBBB;
    constexpr uint16_t SNAPSHOT_CLEAR = 0xCCCC;
    constexpr uint16_t SNAPSHOT_END   = 0xEEEE;
} // namespace SnapshotControl

} // namespace alphatrader
