#include "market_order_book.h"

namespace alphatrader {

using Logger = quantlink::Logger;

MarketOrderBook::MarketOrderBook(TickerId ticker_id, Logger* logger, size_t pool_size) noexcept
    : ticker_id_(ticker_id),
      logger_(logger),
      order_pool_(pool_size),
      level_pool_(pool_size) {}

auto MarketOrderBook::getOrCreateLevel(Side side, Price price) noexcept -> PriceLevel* {
    if (side == Side::BUY) {
        const auto [it, inserted] = bids_.emplace(price, PriceLevel{});
        PriceLevel* level = &it->second;
        if (inserted) level->price = price;
        return level;
    }
    const auto [it, inserted] = asks_.emplace(price, PriceLevel{});
    PriceLevel* level = &it->second;
    if (inserted) level->price = price;
    return level;
}

auto MarketOrderBook::allocateOrder(OrderId order_ref, Quantity qty) noexcept -> BookOrder* {
    BookOrder* order = order_pool_.allocate();
    order->order_ref = order_ref;
    order->qty = qty;
    order->next = nullptr;
    order->prev = nullptr;
    return order;
}

auto MarketOrderBook::deallocateOrder(BookOrder* order) noexcept -> void {
    order_pool_.deallocate(order);
}

auto MarketOrderBook::linkOrder(PriceLevel* level, BookOrder* order) noexcept -> void {
    if (UNLIKELY(level->head == nullptr)) {
        level->head = order;
        level->tail = order;
    }
    else {
        order->prev = level->tail;
        level->tail->next = order;
        level->tail = order;
    }
    level->total_qty += order->qty;
}

auto MarketOrderBook::unlinkOrder(PriceLevel* level, BookOrder* order) noexcept -> void {
    if (order->prev != nullptr) {
        order->prev->next = order->next;
    } else {
        level->head = order->next;
    }

    if (order->next != nullptr) {
        order->next->prev = order->prev;
    } else {
        level->tail = order->prev;
    }

    level->total_qty -= order->qty;
}

auto MarketOrderBook::updateBBO() noexcept -> void {
    bbo_.bid_price = Price_INVALID;
    bbo_.bid_qty = 0;
    bbo_.ask_price = Price_INVALID;
    bbo_.ask_qty = 0;

    if (!bids_.empty()) {
        best_bid_ = &bids_.begin()->second;
        bbo_.bid_price = best_bid_->price;
        bbo_.bid_qty = best_bid_->total_qty;
    } else {
        best_bid_ = nullptr;
    }

    if (!asks_.empty()) {
        best_ask_ = &asks_.begin()->second;
        bbo_.ask_price = best_ask_->price;
        bbo_.ask_qty = best_ask_->total_qty;
    } else {
        best_ask_ = nullptr;
    }
}

auto MarketOrderBook::eraseOrder(OrderId order_ref) noexcept -> bool {
    const auto it = order_index_.find(order_ref);
    if (UNLIKELY(it == order_index_.end())) return false;

    const OrderLocation location = it->second;
    unlinkOrder(location.level, location.order);
    deallocateOrder(location.order);
    if (location.level->head == nullptr) {
        if (location.side == Side::BUY) bids_.erase(location.level->price);
        else asks_.erase(location.level->price);
    }
    order_index_.erase(it);
    return true;
}

auto MarketOrderBook::reduceOrder(const MarketUpdate& update) noexcept -> bool {
    const auto it = order_index_.find(update.order_ref);
    if (UNLIKELY(it == order_index_.end())) return false;
    if (update.qty >= it->second.order->qty) return eraseOrder(update.order_ref);
    it->second.order->qty -= update.qty;
    it->second.level->total_qty -= update.qty;
    return true;
}

auto MarketOrderBook::replaceOrder(const MarketUpdate& update) noexcept -> bool {
    const auto it = order_index_.find(update.order_ref);
    if (UNLIKELY(it == order_index_.end() || order_index_.contains(update.new_ref))) return false;
    BookOrder* order = it->second.order;
    PriceLevel* old_level = it->second.level;
    const Side old_side = it->second.side;
    unlinkOrder(old_level, order);
    if (old_level->head == nullptr) {
        if (old_side == Side::BUY) bids_.erase(old_level->price);
        else asks_.erase(old_level->price);
    }
    PriceLevel* new_level = getOrCreateLevel(update.side, update.price);
    order->order_ref = update.new_ref;
    order->qty = update.qty;
    linkOrder(new_level, order);

    order_index_.erase(it);
    order_index_[update.new_ref] = {update.side, new_level, order};
    return true;
}

auto MarketOrderBook::applyUpdate(const MarketUpdate& update) noexcept -> void {
    if (update.type == UpdateType::SNAPSHOT_START) {
        onSnapshotStart(update.order_ref);
        return;
    }
    if (update.type == UpdateType::SNAPSHOT_CLEAR) {
        if (update.ticker_id == ticker_id_) onSnapshotClear();
        return;
    }
    if (update.type == UpdateType::SNAPSHOT_END) {
        onSnapshotEnd();
        return;
    }
    if (update.ticker_id != ticker_id_) return;

    switch (update.type) {
        case UpdateType::ADD:
        case UpdateType::ADD_MPID: {
            if (UNLIKELY(order_index_.contains(update.order_ref))) break;
            PriceLevel* level = getOrCreateLevel(update.side, update.price);
            BookOrder* order = allocateOrder(update.order_ref, update.qty);
            linkOrder(level, order);
            order_index_[update.order_ref] = {update.side, level, order};
            updateBBO();
            break;
        }

        case UpdateType::DELETE:
            if (eraseOrder(update.order_ref)) {
                updateBBO();
            }
            break;

        case UpdateType::CANCEL:
            if (reduceOrder(update)) {
                updateBBO();
            }
            break;

        case UpdateType::EXECUTE:
            if (reduceOrder(update)) {
                updateBBO();
            }
            break;

        case UpdateType::REPLACE:
            if (replaceOrder(update)) {
                updateBBO();
            }
            break;

        default:
            break;
    }
}

auto MarketOrderBook::clear() noexcept -> void {
    for (auto& [price, level] : bids_) {
        auto* order = level.head;
        while (order != nullptr) {
            BookOrder* next = order->next;
            deallocateOrder(order);
            order = next;
        }
    }
    for (auto& [price, level] : asks_) {
        auto* order = level.head;
        while (order != nullptr) {
            BookOrder* next = order->next;
            deallocateOrder(order);
            order = next;
        }
    }

    bids_.clear();
    asks_.clear();
    order_index_.clear();
    best_bid_ = nullptr;
    best_ask_ = nullptr;
    bbo_ = BBO{};
}

auto MarketOrderBook::onSnapshotStart(SeqNum last_inc_seq_num) noexcept -> void {
    clear();
    in_snapshot_ = true;
    (void)last_inc_seq_num;
}

auto MarketOrderBook::onSnapshotClear() noexcept -> void {
    clear();
}

auto MarketOrderBook::onSnapshotEnd() noexcept -> void {
    in_snapshot_ = false;
    updateBBO();
}

} // namespace alphatrader
