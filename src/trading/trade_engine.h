#pragma once

#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "QuantLink/Lib/concurrency/thread_utils.h"
#include "QuantLink/Lib/logging/logger.h"
#include "src/market_order_book.h"
#include "src/trading/features/feature_engine.h"
#include "src/trading/features/mkt_price_feature.h"
#include "src/trading/order_id_allocator.h"
#include "src/trading/order_manager.h"
#include "src/trading/pnl_tracker.h"
#include "src/trading/risk_manager.h"
#include "src/trading/strategy.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <optional>
#include <thread>

namespace alphatrader {

struct StrategyRunResult {
    size_t market_updates     = 0;
    size_t responses          = 0;
    size_t orders_submitted   = 0;
    size_t rejected_responses = 0;
};

// TradeEngine — one event-loop engine per process: owns the shared per-ticker
// books and drives N strategy slots on a single thread.
//
// Public API:
//   ctor(md_q, req_q, resp_q, logger, strategy, slot_count,
//        sync_gate = nullptr, book_pool_size = 65536)
//   bool registerTicker(slot, ticker_id, RiskLimits)  share / create the ticker book
//   void setStrategy(slot, strategy)                  per-slot strategy config
//   StrategyRunResult runOnce()                       drain responses + market updates (pull)
//   void start(core_id = -1) / stop()       pinned thread loop, 1ms backoff, sync gate
//   allocator() / slotCount()                         shared id allocator
//   pnl(slot) / risk(slot) / orders(slot)             per-slot state
//   features(slot, ticker_id) / book(ticker_id)       lookups (book = shared, const)
//   bookCount()                                       number of ticker books
template<typename Features, typename StrategyT, size_t MaxSlots = 4, size_t MaxTickers = 16, size_t MaxPendingOrders = 64>
    requires Strategy<StrategyT, Features, OrderManager<RiskManager<MaxTickers, MaxPendingOrders>, PnLTracker<MaxTickers>>, PnLTracker<MaxTickers>>
class TradeEngine {
  public:
    using RiskT         = RiskManager<MaxTickers, MaxPendingOrders>;
    using PnlT          = PnLTracker<MaxTickers>;
    using OrderManagerT = OrderManager<RiskT, PnlT>;

    TradeEngine(quantlink::SPSCQueue<MarketUpdate>* market_updates, quantlink::SPSCQueue<OrderRequest>* order_requests, quantlink::SPSCQueue<OrderResponse>* order_responses, quantlink::Logger* logger,
                StrategyT strategy, size_t slot_count, const std::atomic<bool>* sync_gate = nullptr, size_t book_pool_size = 65'536) noexcept
        : market_updates_(market_updates),
          order_requests_(order_requests),
          order_responses_(order_responses),
          logger_(logger),
          allocator_(slot_count),
          sync_gate_(sync_gate),
          slot_count_(slot_count < MaxSlots ? slot_count : MaxSlots),
          book_pool_size_(book_pool_size) {
        for (size_t i = 0; i < slot_count_; ++i) {
            slots_[i].strategy = strategy;
            slots_[i].om.initialize(&slots_[i].risk, &slots_[i].pnl, order_requests_, &allocator_, i, onOrderUpdateForwarder, &forwarder_ctx_[i]);
            forwarder_ctx_[i].engine = this;
            forwarder_ctx_[i].slot   = i;
        }
    }

    // Registers a ticker for a strategy slot. The book is created once per
    // ticker and shared by every slot that registers it.
    bool registerTicker(size_t slot, TickerId ticker_id, RiskLimits limits) noexcept {
        if (slot >= MaxSlots || slot >= slot_count_)
            return false;
        Slot& slot_ref = slots_[slot];
        if (findEntry(slot_ref, ticker_id) || slot_ref.count_ == MaxTickers)
            return false;

        TickerBook* book = findOrCreateTicker(ticker_id);
        if (!book)
            return false;
        if (!slot_ref.pnl.registerTicker(ticker_id) || !slot_ref.risk.registerTicker(ticker_id, limits)) {
            return false;
        }

        Entry<Features>* entry = &slot_ref.entries_[slot_ref.count_];
        entry->ticker_id       = ticker_id;
        entry->book            = &*book->book;
        entry->features.emplace();
        ++slot_ref.count_;
        return true;
    }

    // Replaces the strategy of one slot (per-slot configuration).
    void setStrategy(size_t slot, StrategyT strategy) noexcept {
        if (slot < MaxSlots && slot < slot_count_)
            slots_[slot].strategy = strategy;
    }

    StrategyRunResult runOnce() noexcept {
        StrategyRunResult result;
        drainResponses(result);
        drainMarketUpdates(result);
        return result;
    }

    void start(int core_id = -1) noexcept {
        run_.store(true, std::memory_order_release);
        thread_ = quantlink::utils::create_and_pin_thread(core_id, "trade_engine", [this]() { run(); });
    }

    void stop() noexcept {
        run_.store(false, std::memory_order_release);
        if (thread_.joinable())
            thread_.join();
    }

    const OrderIdAllocator& allocator() const noexcept { return allocator_; }
    size_t                  slotCount() const noexcept { return slot_count_; }

    PnlT&                pnl(size_t slot) noexcept { return slots_[slot].pnl; }
    const PnlT&          pnl(size_t slot) const noexcept { return slots_[slot].pnl; }
    RiskT&               risk(size_t slot) noexcept { return slots_[slot].risk; }
    const RiskT&         risk(size_t slot) const noexcept { return slots_[slot].risk; }
    OrderManagerT&       orders(size_t slot) noexcept { return slots_[slot].om; }
    const OrderManagerT& orders(size_t slot) const noexcept { return slots_[slot].om; }

    Features* features(size_t slot, TickerId ticker_id) noexcept {
        if (slot >= MaxSlots || slot >= slot_count_)
            return nullptr;
        Entry<Features>* entry = findEntry(slots_[slot], ticker_id);
        return entry ? &*entry->features : nullptr;
    }

    const MarketOrderBook* book(TickerId ticker_id) const noexcept {
        for (size_t i = 0; i < book_count_; ++i)
            if (tickers_[i].ticker_id == ticker_id)
                return &*tickers_[i].book;
        return nullptr;
    }

    size_t bookCount() const noexcept { return book_count_; }

  private:
    struct TickerBook {
        TickerId                       ticker_id = TickerId_INVALID;
        std::optional<MarketOrderBook> book;
    };

    struct Slot {
        StrategyT       strategy;
        PnlT            pnl;
        RiskT           risk;
        OrderManagerT   om;
        Entry<Features> entries_[MaxTickers];
        size_t          count_ = 0;
    };

    struct ForwarderContext {
        TradeEngine* engine = nullptr;
        size_t       slot   = 0;
    };

    static void onOrderUpdateForwarder(const OMOrder& order, void* user_data) noexcept {
        const ForwarderContext* ctx = static_cast<const ForwarderContext*>(user_data);
        ctx->engine->slots_[ctx->slot].strategy.onOrderUpdate(order);
    }

    void run() noexcept {
        while (run_.load(std::memory_order_acquire)) {
            if (sync_gate_ && !sync_gate_->load(std::memory_order_acquire)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            runOnce();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    void drainResponses(StrategyRunResult& result) noexcept {
        OrderResponse response;
        while (order_responses_->pop(response)) {
            ++result.responses;
            const size_t slot = allocator_.ownerOf(response.client_order_id);
            if (slot < slot_count_ && slots_[slot].om.onOrderResponse(response))
                continue;
            ++result.rejected_responses;
        }
    }

    void drainMarketUpdates(StrategyRunResult& result) noexcept {
        MarketUpdate update;
        while (market_updates_->pop(update)) {
            ++result.market_updates;
            if (update.type == UpdateType::SNAPSHOT_START || update.type == UpdateType::SNAPSHOT_CLEAR || update.type == UpdateType::SNAPSHOT_END) {
                for (size_t i = 0; i < book_count_; ++i)
                    processBookUpdate(tickers_[i], update, result);
                continue;
            }

            TickerBook* ticker = findTicker(update.ticker_id);
            if (ticker)
                processBookUpdate(*ticker, update, result);
        }
    }

    void processBookUpdate(TickerBook& ticker, const MarketUpdate& update, StrategyRunResult& result) noexcept {
        ticker.book->applyUpdate(update);
        const bool in_snapshot = ticker.book->isInSnapshot();
        for (size_t i = 0; i < slot_count_; ++i)
            processSlotUpdate(slots_[i], ticker.ticker_id, update, in_snapshot, result);
    }

    void processSlotUpdate(Slot& slot, TickerId ticker_id, const MarketUpdate& update, bool in_snapshot, StrategyRunResult& result) noexcept {
        Entry<Features>* entry = findEntry(slot, ticker_id);
        if (!entry)
            return;

        entry->features->onMarketUpdate(update, *entry->book);
        const Price mark = entry->features->template get<MktPriceFeature>().value();
        if (mark != Price_INVALID)
            slot.pnl.onMark(ticker_id, mark);
        if (in_snapshot)
            return;

        result.orders_submitted += update.type == UpdateType::EXECUTE ? slot.strategy.onTrade(*entry, update, slot.om, slot.pnl) : slot.strategy.onBookUpdate(*entry, update, slot.om, slot.pnl);
    }

    TickerBook* findTicker(TickerId ticker_id) noexcept {
        for (size_t i = 0; i < book_count_; ++i)
            if (tickers_[i].ticker_id == ticker_id)
                return &tickers_[i];
        return nullptr;
    }

    TickerBook* findOrCreateTicker(TickerId ticker_id) noexcept {
        if (TickerBook* existing = findTicker(ticker_id))
            return existing;
        if (book_count_ == MaxTickers)
            return nullptr;
        tickers_[book_count_].ticker_id = ticker_id;
        tickers_[book_count_].book.emplace(ticker_id, logger_, book_pool_size_);
        ++book_count_;
        return &tickers_[book_count_ - 1];
    }

    Entry<Features>* findEntry(Slot& slot, TickerId ticker_id) noexcept {
        for (size_t i = 0; i < slot.count_; ++i)
            if (slot.entries_[i].ticker_id == ticker_id)
                return &slot.entries_[i];
        return nullptr;
    }

    quantlink::SPSCQueue<MarketUpdate>*  market_updates_  = nullptr;
    quantlink::SPSCQueue<OrderRequest>*  order_requests_  = nullptr;
    quantlink::SPSCQueue<OrderResponse>* order_responses_ = nullptr;
    quantlink::Logger*                   logger_          = nullptr;
    OrderIdAllocator                     allocator_{1};
    const std::atomic<bool>*             sync_gate_      = nullptr;
    size_t                               slot_count_     = 0;
    size_t                               book_pool_size_ = 0;
    std::atomic<bool>                    run_{false};
    std::thread                          thread_;
    ForwarderContext                     forwarder_ctx_[MaxSlots];
    Slot                                 slots_[MaxSlots];
    TickerBook                           tickers_[MaxTickers];
    size_t                               book_count_ = 0;
};

} // namespace alphatrader
