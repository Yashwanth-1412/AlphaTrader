#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "QuantLink/Lib/logging/logger.h"
#include "trading/features/feature_engine.h"
#include "trading/features/mkt_price_feature.h"
#include "trading/features/order_book_imbalance_feature.h"
#include "trading/features/spread_feature.h"
#include "trading/strategy.h"
#include "trading/trade_engine.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

using namespace alphatrader;

namespace {

int passes   = 0;
int failures = 0;

void check(const char* description, bool condition) {
    std::printf("%s: %s\n", condition ? "PASS" : "FAIL", description);
    condition ? ++passes : ++failures;
}

MarketUpdate add(TickerId ticker_id, Side side, Price price, Quantity qty, OrderId order_ref) {
    return {0, ticker_id, UpdateType::ADD, side, price, qty, order_ref, 0};
}

using EngineFeatures = FeatureEngine<MktPriceFeature, SpreadFeature, OrderBookImbalanceFeature>;
using Engine         = TradeEngine<EngineFeatures, LiquidityTaker, 2, 2, 8>;

struct Rig {
    quantlink::Logger                   logger{64, "trade_engine_test.log", -1};
    quantlink::SPSCQueue<MarketUpdate>  market_updates{32};
    quantlink::SPSCQueue<OrderRequest>  order_requests{32};
    quantlink::SPSCQueue<OrderResponse> order_responses{32};
    Engine                              engine;

    Rig()
        : engine(&market_updates, &order_requests, &order_responses, &logger, LiquidityTaker({10, 300'000}), 2, nullptr, 64) {}
};

} // namespace

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    // Order ids encode the owning slot in the high bits; build them the same
    // way the allocator does so the tests track the encoding.
    constexpr auto orderId = [](OrderId slot, OrderId counter) {
        return (slot << OrderIdAllocator::COUNTER_BITS) | counter;
    };

    // ── Shared book + fan-out + routing ─────────────────────────
    Rig rig;
    rig.engine.setStrategy(0, LiquidityTaker({10, 900'000}));
    check("slot 0 registers ticker", rig.engine.registerTicker(0, 1, {200, 100, 200'000'000}));
    check("slot 1 registers same ticker", rig.engine.registerTicker(1, 1, {200, 100, 200'000'000}));
    check("duplicate ticker in slot rejected", !rig.engine.registerTicker(0, 1, {200, 100, 200'000'000}));
    check("out-of-range slot rejected", !rig.engine.registerTicker(9, 1, {200, 100, 200'000'000}));
    check("one shared book for both slots", rig.engine.bookCount() == 1 && rig.engine.book(1) != nullptr);
    check("no book for unknown ticker", rig.engine.book(2) == nullptr);

    rig.market_updates.push(add(1, Side::BUY, 1'000'000, 200, 10));
    rig.market_updates.push(add(1, Side::SELL, 1'001'000, 100, 11));
    const StrategyRunResult decision = rig.engine.runOnce();
    check("both updates consumed", decision.market_updates == 2);

    const MarketOrderBook* shared = rig.engine.book(1);
    check("book applied once (qty not doubled)", shared->getBBO().bid_qty == 200 && shared->getBBO().ask_qty == 100 && shared->bidLevels() == 1 && shared->askLevels() == 1);

    check("both slots compute from the same book", rig.engine.features(0, 1)->get<MktPriceFeature>().value() == 1'000'666 && rig.engine.features(1, 1)->get<MktPriceFeature>().value() == 1'000'666);

    OrderRequest sent{};
    check("only slot 1 fires (slot 0 above threshold)", decision.orders_submitted == 1 && rig.order_requests.pop(sent) && sent.client_order_id == orderId(1, 1) && sent.ticker_id == 1 &&
                                                            sent.side == Side::BUY && sent.price == 1'001'000 && sent.qty == 10);
    check("reservation held by slot 1 only", rig.engine.risk(1).pendingExposure(1) == 10 && rig.engine.risk(0).pendingExposure(1) == 0);

    rig.order_responses.push({orderId(1, 1), 0, Side::BUY, 0, 0, 10, 1'000'500, 0, OrderResponse::EXECUTED});
    const StrategyRunResult execution = rig.engine.runOnce();
    check("fill routed to slot 1", execution.responses == 1 && rig.engine.pnl(1).get(1)->net_qty == 10 && rig.engine.pnl(1).get(1)->avg_entry_price == 1'000'500);
    check("slot 0 untouched", rig.engine.pnl(0).get(1)->net_qty == 0);
    check("slot 1 reservation released", rig.engine.risk(1).pendingCount() == 0);

    rig.order_responses.push({orderId(2, 1), 0, Side::BUY, 0, 0, 0, 0, 0, OrderResponse::REJECTED});
    rig.order_responses.push({orderId(0, 99), 0, Side::BUY, 0, 0, 0, 0, 0, OrderResponse::REJECTED});
    const StrategyRunResult bad = rig.engine.runOnce();
    check("out-of-range slot and unknown order rejected", bad.responses == 2 && bad.rejected_responses == 2);

    // ── Snapshot gate ───────────────────────────────────────────
    Rig gate;
    gate.engine.registerTicker(0, 1, {200, 100, 200'000'000});
    gate.market_updates.push({0, 1, UpdateType::SNAPSHOT_START, Side::BUY, 0, 0, 0, 0});
    gate.market_updates.push(add(1, Side::BUY, 1'000'000, 200, 10));
    gate.market_updates.push(add(1, Side::SELL, 1'001'000, 100, 11));
    const StrategyRunResult during_snapshot = gate.engine.runOnce();
    check("no signals while in snapshot", during_snapshot.orders_submitted == 0 && gate.engine.book(1) && gate.engine.book(1)->isInSnapshot());
    gate.market_updates.push({0, 1, UpdateType::SNAPSHOT_END, Side::BUY, 0, 0, 0, 0});
    const StrategyRunResult after_snapshot = gate.engine.runOnce();
    check("signal fires after snapshot end", after_snapshot.orders_submitted == 1);
    check("book left snapshot state", !gate.engine.book(1)->isInSnapshot());

    // ── Multi-ticker isolation ──────────────────────────────────
    Rig multi;
    multi.engine.setStrategy(1, LiquidityTaker({10, 900'000}));
    check("slot 0 registers ticker 1", multi.engine.registerTicker(0, 1, {200, 100, 200'000'000}));
    check("first book created", multi.engine.bookCount() == 1 && multi.engine.book(1) != nullptr);
    check("slot 0 registers ticker 2", multi.engine.registerTicker(0, 2, {200, 100, 200'000'000}));
    check("second ticker gets its own book", multi.engine.bookCount() == 2 && multi.engine.book(2) != nullptr);
    check("slot 1 registers both tickers", multi.engine.registerTicker(1, 1, {200, 100, 200'000'000}) && multi.engine.registerTicker(1, 2, {200, 100, 200'000'000}));
    check("both slots share the two books", multi.engine.bookCount() == 2);

    multi.market_updates.push(add(2, Side::BUY, 500'000, 300, 20));
    multi.market_updates.push(add(2, Side::SELL, 501'000, 150, 21));
    const StrategyRunResult ticker2 = multi.engine.runOnce();
    OrderRequest            ticker2_sent{};
    check("ticker 2 update drives only slot 0",
          ticker2.orders_submitted == 1 && multi.order_requests.pop(ticker2_sent) && ticker2_sent.ticker_id == 2 && ticker2_sent.client_order_id == orderId(0, 1));
    check("ticker 1 features untouched", multi.engine.features(0, 1)->get<MktPriceFeature>().value() == Price_INVALID && multi.engine.features(1, 1)->get<MktPriceFeature>().value() == Price_INVALID);

    // ── Thread + sync gate ──────────────────────────────────────
    std::atomic<bool>                   sync{false};
    quantlink::Logger                   logger2(64, "trade_engine_thread_test.log", -1);
    quantlink::SPSCQueue<MarketUpdate>  thread_updates(32);
    quantlink::SPSCQueue<OrderRequest>  thread_requests(32);
    quantlink::SPSCQueue<OrderResponse> thread_responses(32);
    Engine                              threaded(&thread_updates, &thread_requests, &thread_responses, &logger2, LiquidityTaker({10, 300'000}), 1, &sync, 64);
    threaded.registerTicker(0, 1, {200, 100, 200'000'000});
    threaded.start();

    thread_updates.push(add(1, Side::BUY, 1'000'000, 200, 30));
    thread_updates.push(add(1, Side::SELL, 1'001'000, 100, 31));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    check("trading gated until synchronized", threaded.risk(0).pendingExposure(1) == 0);

    sync.store(true, std::memory_order_release);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    check("trading resumes after sync flag", threaded.risk(0).pendingExposure(1) == 10);

    threaded.stop();
    check("thread stopped cleanly", true);

    std::remove("trade_engine_test.log");
    std::remove("trade_engine_thread_test.log");
    std::printf("Results: %d passed, %d failed\n", passes, failures);
    return failures == 0 ? 0 : 1;
}
