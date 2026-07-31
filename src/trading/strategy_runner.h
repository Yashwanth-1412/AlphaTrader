#pragma once

#include "src/market_order_book.h"
#include "src/trading/features/feature_engine.h"
#include "src/trading/features/mkt_price_feature.h"
#include "src/trading/features/order_book_imbalance_feature.h"
#include "src/trading/pnl_tracker.h"
#include "src/trading/risk_manager.h"

#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "QuantLink/Lib/logging/logger.h"

#include <cstddef>
#include <optional>

namespace alphatrader {

struct IocSignalConfig {
    Quantity order_qty = 0;
    Ratio entry_imbalance = Ratio_SCALE;
};

struct StrategyRunResult {
    size_t market_updates = 0;
    size_t responses = 0;
    size_t orders_submitted = 0;
    size_t rejected_responses = 0;
};

template <typename Features, size_t MaxTickers = 16, size_t MaxPendingOrders = 64>
class StrategyRunner {
public:
    StrategyRunner(quantlink::SPSCQueue<MarketUpdate>* market_updates,
                   quantlink::SPSCQueue<OrderRequest>* order_requests,
                   quantlink::SPSCQueue<OrderResponse>* order_responses,
                   quantlink::Logger* logger,
                   IocSignalConfig signal_config, size_t book_pool_size = 65'536) noexcept
        : market_updates_(market_updates)
        , order_requests_(order_requests)
        , order_responses_(order_responses)
        , logger_(logger)
        , signal_config_(signal_config)
        , book_pool_size_(book_pool_size) {}

    bool registerTicker(TickerId ticker_id, RiskLimits limits) noexcept {
        if (findEntry(ticker_id) || count_ == MaxTickers || !pnl_.registerTicker(ticker_id) ||
            !risk_.registerTicker(ticker_id, limits)) {
            return false;
        }
        entries_[count_].ticker_id = ticker_id;
        entries_[count_].book.emplace(ticker_id, logger_, book_pool_size_);
        entries_[count_].features.emplace();
        ++count_;
        return true;
    }

    StrategyRunResult runOnce() noexcept {
        StrategyRunResult result;
        drainResponses(result);
        drainMarketUpdates(result);
        return result;
    }

    PnLTracker<MaxTickers>& pnl() noexcept { return pnl_; }
    const PnLTracker<MaxTickers>& pnl() const noexcept { return pnl_; }
    RiskManager<MaxTickers, MaxPendingOrders>& risk() noexcept { return risk_; }
    const RiskManager<MaxTickers, MaxPendingOrders>& risk() const noexcept { return risk_; }

    Features* features(TickerId ticker_id) noexcept {
        Entry* entry = findEntry(ticker_id);
        return entry ? &*entry->features : nullptr;
    }

private:
    struct Entry {
        TickerId ticker_id = TickerId_INVALID;
        std::optional<MarketOrderBook> book;
        std::optional<Features> features;
    };

    void drainResponses(StrategyRunResult& result) noexcept {
        OrderResponse response;
        while (order_responses_->pop(response)) {
            ++result.responses;
            if (!risk_.onOrderResponse(response, pnl_)) ++result.rejected_responses;
        }
    }

    void drainMarketUpdates(StrategyRunResult& result) noexcept {
        MarketUpdate update;
        while (market_updates_->pop(update)) {
            ++result.market_updates;
            if (update.type == UpdateType::SNAPSHOT_START || update.type == UpdateType::SNAPSHOT_CLEAR ||
                update.type == UpdateType::SNAPSHOT_END) {
                for (size_t i = 0; i < count_; ++i) processUpdate(entries_[i], update, result);
                continue;
            }

            Entry* entry = findEntry(update.ticker_id);
            if (entry) processUpdate(*entry, update, result);
        }
    }

    void processUpdate(Entry& entry, const MarketUpdate& update, StrategyRunResult& result) noexcept {
        entry.book->applyUpdate(update);
        entry.features->onMarketUpdate(update, *entry.book);

        const Price mark = entry.features->template get<MktPriceFeature>().value();
        if (mark != Price_INVALID) pnl_.onMark(entry.ticker_id, mark);
        if (entry.book->isInSnapshot()) return;

        const Ratio imbalance = entry.features->template get<OrderBookImbalanceFeature>().value();
        if (imbalance == Ratio_INVALID || signal_config_.order_qty == 0) return;

        const BBO& bbo = entry.book->getBBO();
        if (imbalance >= signal_config_.entry_imbalance)
            submit(entry.ticker_id, Side::BUY, bbo.ask_price, result);
        else if (imbalance <= -signal_config_.entry_imbalance)
            submit(entry.ticker_id, Side::SELL, bbo.bid_price, result);
    }

    void submit(TickerId ticker_id, Side side, Price price, StrategyRunResult& result) noexcept {
        Position* position = pnl_.get(ticker_id);
        if (!position || price == Price_INVALID) return;

        const OrderRequest request{OrderRequestType::NEW, next_client_order_id_, 0, ticker_id,
                                   side, price, signal_config_.order_qty, true};
        if (risk_.canSubmit(request, *position) != RiskResult::ACCEPTED) return;
        if (!order_requests_->push(request)) return;
        if (!risk_.onSubmitted(request)) return;
        ++next_client_order_id_;
        ++result.orders_submitted;
    }

    Entry* findEntry(TickerId ticker_id) noexcept {
        for (size_t i = 0; i < count_; ++i)
            if (entries_[i].ticker_id == ticker_id)
                return &entries_[i];
        return nullptr;
    }

    quantlink::SPSCQueue<MarketUpdate>* market_updates_ = nullptr;
    quantlink::SPSCQueue<OrderRequest>* order_requests_ = nullptr;
    quantlink::SPSCQueue<OrderResponse>* order_responses_ = nullptr;
    quantlink::Logger* logger_ = nullptr;
    IocSignalConfig signal_config_;
    size_t book_pool_size_ = 0;
    PnLTracker<MaxTickers> pnl_;
    RiskManager<MaxTickers, MaxPendingOrders> risk_;
    Entry entries_[MaxTickers];
    size_t count_ = 0;
    OrderId next_client_order_id_ = 1;
};

}
