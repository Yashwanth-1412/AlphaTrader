#pragma once

#include "src/market_order_book.h"

namespace alphatrader {

struct AggTradeRatioFeature {
    void onMarketUpdate(const MarketUpdate& update, const MarketOrderBook&) noexcept {
        if (update.type != UpdateType::EXECUTE) return;
        if (update.side == Side::SELL)
            buy_qty_ += update.qty;
        else
            sell_qty_ += update.qty;
    }

    Ratio value() const noexcept {
        int64_t total = buy_qty_ + sell_qty_;
        if (UNLIKELY(total == 0)) return Ratio_INVALID;
        return static_cast<Ratio>(buy_qty_ * Ratio_SCALE / total);
    }

    const char* name() const noexcept { return "agg_trade_ratio"; }

    void reset() noexcept {
        buy_qty_ = 0;
        sell_qty_ = 0;
    }

private:
    int64_t buy_qty_ = 0;
    int64_t sell_qty_ = 0;
};

}
