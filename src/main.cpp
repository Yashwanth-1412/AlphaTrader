#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "QuantLink/Lib/logging/logger.h"
#include "market_data/market_data.h"
#include "order_gateway/order_gateway.h"
#include "trading/features/agg_trade_ratio_feature.h"
#include "trading/features/feature_engine.h"
#include "trading/features/mkt_price_feature.h"
#include "trading/features/order_book_imbalance_feature.h"
#include "trading/features/spread_feature.h"
#include "trading/strategy.h"
#include "trading/trade_engine.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

using namespace alphatrader;

namespace {

volatile std::sig_atomic_t g_shutdown = 0;

void handleSignal(int) {
    g_shutdown = 1;
}

struct Config {
    std::string           gateway_ip       = "127.0.0.1";
    int                   gateway_port     = 11000;
    std::string           mcast_iface      = "lo";
    std::string           snapshot_ip      = "127.0.0.1";
    int                   snapshot_port    = 21003;
    std::string           incremental_ip   = "127.0.0.1";
    int                   incremental_port = 21001;
    int                   receive_port     = 22001;
    std::vector<TickerId> tickers{1};
    RiskLimits            limits{100, 10, 100'000'000};
    IocSignalConfig       signal{10, 300'000};
    int                   core_consumer = -1;
    int                   core_engine   = -1;
    int                   core_gateway  = -1;
};

const char* nextArg(int& i, int argc, char** argv, const char* name) {
    if (i + 1 >= argc) {
        std::fprintf(stderr, "missing value for %s\n", name);
        std::exit(1);
    }
    return argv[++i];
}

Config parseArgs(int argc, char** argv) {
    Config cfg;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--gateway-ip")
            cfg.gateway_ip = nextArg(i, argc, argv, arg.c_str());
        else if (arg == "--gateway-port")
            cfg.gateway_port = std::atoi(nextArg(i, argc, argv, arg.c_str()));
        else if (arg == "--mcast-iface")
            cfg.mcast_iface = nextArg(i, argc, argv, arg.c_str());
        else if (arg == "--snapshot-ip")
            cfg.snapshot_ip = nextArg(i, argc, argv, arg.c_str());
        else if (arg == "--snapshot-port")
            cfg.snapshot_port = std::atoi(nextArg(i, argc, argv, arg.c_str()));
        else if (arg == "--incremental-ip")
            cfg.incremental_ip = nextArg(i, argc, argv, arg.c_str());
        else if (arg == "--incremental-port")
            cfg.incremental_port = std::atoi(nextArg(i, argc, argv, arg.c_str()));
        else if (arg == "--receive-port")
            cfg.receive_port = std::atoi(nextArg(i, argc, argv, arg.c_str()));
        else if (arg == "--ticker")
            cfg.tickers.push_back(std::atoi(nextArg(i, argc, argv, arg.c_str())));
        else if (arg == "--position-limit")
            cfg.limits.max_abs_position = std::atoll(nextArg(i, argc, argv, arg.c_str()));
        else if (arg == "--order-limit")
            cfg.limits.max_order_qty = std::atoi(nextArg(i, argc, argv, arg.c_str()));
        else if (arg == "--notional-limit")
            cfg.limits.max_order_notional = std::atoll(nextArg(i, argc, argv, arg.c_str()));
        else if (arg == "--ioc-qty")
            cfg.signal.order_qty = std::atoi(nextArg(i, argc, argv, arg.c_str()));
        else if (arg == "--imbalance")
            cfg.signal.entry_imbalance = std::atoi(nextArg(i, argc, argv, arg.c_str()));
        else if (arg == "--core-consumer")
            cfg.core_consumer = std::atoi(nextArg(i, argc, argv, arg.c_str()));
        else if (arg == "--core-engine")
            cfg.core_engine = std::atoi(nextArg(i, argc, argv, arg.c_str()));
        else if (arg == "--core-gateway")
            cfg.core_gateway = std::atoi(nextArg(i, argc, argv, arg.c_str()));
        else if (arg == "--help") {
            std::printf("AlphaTrader trading main\n"
                        "  --gateway-ip/--gateway-port   OUCH session (default 127.0.0.1:11000)\n"
                        "  --mcast-iface                 incremental multicast interface (default lo)\n"
                        "  --snapshot-ip/--snapshot-port TCP snapshot/replay (default 127.0.0.1:21003)\n"
                        "  --incremental-ip/--incremental-port  exchange feed gateway (default 127.0.0.1:21001)\n"
                        "  --receive-port                 local UDP port for the incremental feed (default 22001)\n"
                        "  --ticker <id>                 register ticker (repeatable, default 1)\n"
                        "  --position-limit/--order-limit/--notional-limit  per-ticker risk limits\n"
                        "  --ioc-qty/--imbalance         liquidity taker signal config\n"
                        "  --core-consumer/--core-engine/--core-gateway  core pinning (-1 = unpinned)\n");
            std::exit(0);
        } else {
            std::fprintf(stderr, "unknown argument: %s\n", arg.c_str());
            std::exit(1);
        }
    }
    return cfg;
}

using TradingFeatures = FeatureEngine<MktPriceFeature, SpreadFeature, OrderBookImbalanceFeature, AggTradeRatioFeature>;
using Engine          = TradeEngine<TradingFeatures, LiquidityTaker, 4, 16, 64>;

} // namespace

int main(int argc, char** argv) {
    const Config cfg = parseArgs(argc, argv);

    std::signal(SIGINT, handleSignal);
    std::signal(SIGTERM, handleSignal);

    quantlink::Logger                   logger(8 * 1024 * 1024, "alpha_trader.log", -1);
    quantlink::SPSCQueue<MarketUpdate>  md_queue(1 << 16);
    quantlink::SPSCQueue<OrderRequest>  request_queue(1 << 14);
    quantlink::SPSCQueue<OrderResponse> response_queue(1 << 14);

    MarketDataConsumer consumer(&md_queue, &logger, cfg.mcast_iface, cfg.snapshot_ip, cfg.snapshot_port, cfg.incremental_ip, cfg.incremental_port, cfg.receive_port);

    Engine engine(&md_queue, &request_queue, &response_queue, &logger, LiquidityTaker(cfg.signal), 1, &market_data_synchronized);
    for (TickerId ticker : cfg.tickers) {
        if (!engine.registerTicker(0, ticker, cfg.limits)) {
            std::fprintf(stderr, "failed to register ticker %u\n", ticker);
            return 1;
        }
    }

    OrderGateway gateway(&request_queue, &response_queue, &logger, cfg.gateway_ip, cfg.gateway_port, cfg.mcast_iface);

    gateway.start(cfg.core_gateway);
    consumer.start(cfg.core_consumer);

    bool synced = false;
    for (int i = 0; i < 100 && !g_shutdown; ++i) {
        if (market_data_synchronized.load(std::memory_order_acquire)) {
            synced = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    std::printf("%s\n", synced ? "market data synchronized, trading enabled" : "WARNING: market data not synchronized, continuing");

    engine.start(cfg.core_engine);

    while (!g_shutdown)
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::printf("shutting down...\n");
    consumer.stop();
    engine.stop();
    gateway.stop();

    std::printf("─── session summary ───\n");
    std::printf("orders sent: %zu  replaces: %zu  cancels: %zu  risk-rejected: %zu\n", engine.orders(0).newOrderCount(), engine.orders(0).replaceCount(), engine.orders(0).cancelCount(),
                engine.orders(0).rejectCount());
    std::printf("fills: %zu  filled qty: %llu  live slots: %zu\n", engine.orders(0).fillCount(), static_cast<unsigned long long>(engine.orders(0).filledQty()), engine.orders(0).orderCount());
    for (TickerId ticker : cfg.tickers) {
        const Position* position = engine.pnl(0).get(ticker);
        if (!position)
            continue;
        std::printf("ticker %u: net_qty=%lld entry=%lld realized=%lld unrealized=%lld\n", ticker, static_cast<long long>(position->net_qty), static_cast<long long>(position->avg_entry_price),
                    static_cast<long long>(position->realized_pnl), static_cast<long long>(position->unrealized_pnl));
    }
    if (const std::optional<int64_t> total = engine.pnl(0).totalPnL())
        std::printf("total PnL: %lld\n", static_cast<long long>(*total));

    return 0;
}
