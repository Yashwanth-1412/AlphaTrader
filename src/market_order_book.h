#pragma once

#include "QuantLink/Lib/common/macros.h"
#include "QuantLink/Lib/logging/logger.h"
#include "QuantLink/Lib/memory/object_pool.h"
#include "types.h"

#include <map>
#include <unordered_map>

namespace alphatrader {

class MarketOrderBook final {
  public:
    explicit MarketOrderBook(TickerId ticker_id, quantlink::Logger* logger, size_t pool_size = 65536) noexcept;

    MarketOrderBook()                                  = delete;
    MarketOrderBook(const MarketOrderBook&)            = delete;
    MarketOrderBook& operator=(const MarketOrderBook&) = delete;
    MarketOrderBook(MarketOrderBook&&)                 = delete;
    MarketOrderBook& operator=(MarketOrderBook&&)      = delete;

    auto applyUpdate(const MarketUpdate& update) noexcept -> void;

    auto clear() noexcept -> void;
    auto onSnapshotStart(SeqNum last_inc_seq_num) noexcept -> void;
    auto onSnapshotClear() noexcept -> void;
    auto onSnapshotEnd() noexcept -> void;

    auto getBBO() const noexcept -> const BBO& { return bbo_; }
    auto getBestBid() const noexcept -> const PriceLevel* { return best_bid_; }
    auto getBestAsk() const noexcept -> const PriceLevel* { return best_ask_; }
    auto empty() const noexcept -> bool { return bids_.empty() && asks_.empty(); }
    auto getTickerId() const noexcept -> TickerId { return ticker_id_; }
    auto trackedOrders() const noexcept -> size_t { return order_index_.size(); }
    auto bidLevels() const noexcept -> size_t { return bids_.size(); }
    auto askLevels() const noexcept -> size_t { return asks_.size(); }
    auto isInSnapshot() const noexcept -> bool { return in_snapshot_; }

  private:
    auto getOrCreateLevel(Side side, Price price) noexcept -> PriceLevel*;
    auto allocateOrder(OrderId order_ref, Quantity qty) noexcept -> BookOrder*;
    auto deallocateOrder(BookOrder* order) noexcept -> void;

    auto linkOrder(PriceLevel* level, BookOrder* order) noexcept -> void;
    auto unlinkOrder(PriceLevel* level, BookOrder* order) noexcept -> void;

    auto updateBBO() noexcept -> void;

    struct OrderLocation {
        Side        side;
        PriceLevel* level;
        BookOrder*  order;
    };

    auto eraseOrder(OrderId order_ref) noexcept -> bool;
    auto reduceOrder(const MarketUpdate& update) noexcept -> bool;
    auto replaceOrder(const MarketUpdate& update) noexcept -> bool;

    TickerId ticker_id_ = TickerId_INVALID;

    std::map<Price, PriceLevel, std::greater<>> bids_;
    std::map<Price, PriceLevel, std::less<>>    asks_;

    std::unordered_map<OrderId, OrderLocation> order_index_;

    BBO         bbo_;
    PriceLevel* best_bid_ = nullptr;
    PriceLevel* best_ask_ = nullptr;

    quantlink::ObjectPool<BookOrder>  order_pool_;
    quantlink::ObjectPool<PriceLevel> level_pool_;

    bool in_snapshot_ = false;
};

} // namespace alphatrader
