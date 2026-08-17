#include "trading/features/agg_trade_ratio_feature.h"
#include "trading/features/feature_engine.h"
#include "trading/features/mkt_price_feature.h"
#include "trading/features/order_book_imbalance_feature.h"
#include "trading/features/spread_feature.h"
#include "trading/pnl_tracker.h"
#include "trading/risk_manager.h"
#include "trading/strategy_runner.h"

#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "QuantLink/Lib/logging/logger.h"

#include <cstdio>

using namespace alphatrader;

namespace {

int passes = 0;
int failures = 0;

void check(const char* description, bool condition) {
    std::printf("%s: %s\n", condition ? "PASS" : "FAIL", description);
    condition ? ++passes : ++failures;
}

MarketUpdate add(TickerId ticker_id, Side side, Price price, Quantity qty, OrderId order_ref) {
    return {0, ticker_id, UpdateType::ADD, side, price, qty, order_ref, 0};
}

}

int main() {
    quantlink::Logger logger(64, "trading_test.log", -1);
    MarketOrderBook book(1, &logger, 64);
    using Engine = FeatureEngine<MktPriceFeature, SpreadFeature,
                                 OrderBookImbalanceFeature, AggTradeRatioFeature>;
    Engine features;

    MarketUpdate bid = add(1, Side::BUY, 1'000'000, 200, 1);
    book.applyUpdate(bid);
    features.onMarketUpdate(bid, book);
    check("features invalid without two-sided BBO",
          features.get<MktPriceFeature>().value() == Price_INVALID &&
          features.get<SpreadFeature>().value() == Price_INVALID &&
          features.get<OrderBookImbalanceFeature>().value() == Ratio_INVALID);

    MarketUpdate ask = add(1, Side::SELL, 1'001'000, 100, 2);
    book.applyUpdate(ask);
    features.onMarketUpdate(ask, book);
    check("microprice is liquidity weighted", features.get<MktPriceFeature>().value() == 1'000'666);
    check("spread remains fixed point", features.get<SpreadFeature>().value() == 1'000);
    check("imbalance remains scaled integer",
          features.get<OrderBookImbalanceFeature>().value() == 333'333);

    MarketUpdate passive_sell_execution{0, 1, UpdateType::EXECUTE, Side::SELL, 1'001'000, 60, 2, 0};
    features.onMarketUpdate(passive_sell_execution, book);
    MarketUpdate passive_buy_execution{0, 1, UpdateType::EXECUTE, Side::BUY, 1'000'000, 40, 1, 0};
    features.onMarketUpdate(passive_buy_execution, book);
    check("trade ratio infers aggressor from resting side",
          features.get<AggTradeRatioFeature>().value() == 600'000);

    PnLTracker<2> pnl;
    check("register first ticker", pnl.registerTicker(1));
    check("register second ticker", pnl.registerTicker(2));
    check("reject ticker beyond capacity", !pnl.registerTicker(3));
    check("reject execution for unregistered ticker", !pnl.onExecution(3, Side::BUY, 1'000'000, 1));

    check("open long", pnl.onExecution(1, Side::BUY, 1'000'000, 100));
    check("mark long", pnl.onMark(1, 1'010'000));
    check("long mark to market", pnl.get(1)->unrealized_pnl == 1'000'000);
    check("partial long close", pnl.onExecution(1, Side::SELL, 1'020'000, 40));
    check("partial close PnL", pnl.get(1)->net_qty == 60 &&
          pnl.get(1)->realized_pnl == 800'000 && pnl.get(1)->unrealized_pnl == 600'000);
    check("flip from long to short", pnl.onExecution(1, Side::SELL, 1'030'000, 100));
    check("flip resets entry and recalculates mark", pnl.get(1)->net_qty == -40 &&
          pnl.get(1)->avg_entry_price == 1'030'000 && pnl.get(1)->realized_pnl == 2'600'000 &&
          pnl.get(1)->unrealized_pnl == 800'000);
    check("close position", pnl.onExecution(1, Side::BUY, 1'020'000, 40));
    check("flat position clears entry and unrealized", pnl.get(1)->net_qty == 0 &&
          pnl.get(1)->avg_entry_price == Price_INVALID && pnl.get(1)->unrealized_pnl == 0 &&
          pnl.get(1)->realized_pnl == 3'000'000);
    check("reject invalid mark", !pnl.onMark(1, Price_INVALID));
    check("total PnL", pnl.totalPnL().has_value() && *pnl.totalPnL() == 3'000'000);

    RiskManager<2, 2> risk;
    const RiskLimits limits{100, 60, 60'000'000};
    check("register risk limits", risk.registerTicker(1, limits));

    const OrderRequest first{OrderRequestType::NEW, 101, 0, 1, Side::BUY, 1'000'000, 60, true};
    const OrderRequest too_much{OrderRequestType::NEW, 102, 0, 1, Side::BUY, 1'000'000, 50, true};
    const OrderRequest too_large{OrderRequestType::NEW, 103, 0, 1, Side::BUY, 1'000'000, 61, true};
    const OrderRequest unknown_ticker{OrderRequestType::NEW, 104, 0, 3, Side::BUY, 1'000'000, 1, true};
    check("reject unknown risk ticker",
          risk.canSubmit(unknown_ticker, *pnl.get(1)) == RiskResult::UNKNOWN_TICKER);
    check("accept first order", risk.canSubmit(first, *pnl.get(1)) == RiskResult::ACCEPTED);
    check("reserve only after queue push", risk.onSubmitted(first) && risk.pendingExposure(1) == 60);
    check("reject projected position breach",
          risk.canSubmit(too_much, *pnl.get(1)) == RiskResult::POSITION_LIMIT_EXCEEDED);
    check("reject per-order quantity breach",
          risk.canSubmit(too_large, *pnl.get(1)) == RiskResult::ORDER_QTY_EXCEEDED);

    const OrderResponse partial_fill{101, 0, Side::BUY, 0, 0, 30, 1'000'000, 0,
                                     OrderResponse::EXECUTED};
    check("process partial execution", risk.onOrderResponse(partial_fill, pnl));
    check("execution transfers reserved exposure to confirmed position",
          pnl.get(1)->net_qty == 30 && risk.pendingExposure(1) == 30);
    check("still reject with partial reservation",
          risk.canSubmit(too_much, *pnl.get(1)) == RiskResult::POSITION_LIMIT_EXCEEDED);

    const OrderResponse canceled{101, 0, Side::BUY, 0, 0, 0, 0, 30,
                                 OrderResponse::CANCELED};
    check("process canceled remainder", risk.onOrderResponse(canceled, pnl));
    check("cancel releases reservation", risk.pendingCount() == 0 && risk.pendingExposure(1) == 0);
    check("accept after reservation release", risk.canSubmit(too_much, *pnl.get(1)) == RiskResult::ACCEPTED);
    check("reserve second order", risk.onSubmitted(too_much));
    const OrderResponse rejected{102, 0, Side::BUY, 0, 0, 0, 0, 0,
                                 OrderResponse::REJECTED};
    check("rejection releases full reservation", risk.onOrderResponse(rejected, pnl) &&
          risk.pendingCount() == 0);
    risk.disableTrading();
    check("kill switch blocks trading", risk.canSubmit(too_much, *pnl.get(1)) == RiskResult::TRADING_DISABLED);

    quantlink::SPSCQueue<MarketUpdate> market_updates(16);
    quantlink::SPSCQueue<OrderRequest> order_requests(16);
    quantlink::SPSCQueue<OrderResponse> order_responses(16);
    using RunnerFeatures = FeatureEngine<MktPriceFeature, SpreadFeature,
                                         OrderBookImbalanceFeature, AggTradeRatioFeature>;
    using Runner = StrategyRunner<RunnerFeatures, 1, 4>;
    Runner runner(&market_updates, &order_requests, &order_responses, &logger,
                  {10, 300'000}, 64);
    check("register strategy ticker", runner.registerTicker(1, {20, 10, 20'000'000}));

    market_updates.push(add(1, Side::BUY, 1'000'000, 200, 10));
    market_updates.push(add(1, Side::SELL, 1'001'000, 100, 11));
    const StrategyRunResult decision = runner.runOnce();
    OrderRequest sent{};
    check("runner consumes market queue", decision.market_updates == 2);
    check("runner emits IOC after signal and risk check", decision.orders_submitted == 1 &&
          order_requests.pop(sent) && sent.client_order_id == 1 && sent.ticker_id == 1 &&
          sent.side == Side::BUY && sent.price == 1'001'000 && sent.qty == 10 && sent.ioc);
    check("runner reserves submitted order", runner.risk().pendingExposure(1) == 10);

    order_responses.push({1, 0, Side::BUY, 0, 0, 10, 1'000'500, 0, OrderResponse::EXECUTED});
    const StrategyRunResult execution = runner.runOnce();
    check("runner consumes execution response", execution.responses == 1 &&
          runner.pnl().get(1)->net_qty == 10 && runner.pnl().get(1)->avg_entry_price == 1'000'500);
    check("execution releases IOC reservation", runner.risk().pendingCount() == 0);

    std::remove("trading_test.log");
    std::printf("Results: %d passed, %d failed\n", passes, failures);
    return failures == 0 ? 0 : 1;
}
