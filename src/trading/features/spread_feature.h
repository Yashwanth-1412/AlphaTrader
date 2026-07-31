#pragma once

#include "src/market_order_book.h"

namespace alphatrader {

struct SpreadFeature {
    void onMarketUpdate(const MarketUpdate&, const MarketOrderBook& book) noexcept {
        const BBO& bbo = book.getBBO();
        if (UNLIKELY(bbo.bid_price == Price_INVALID || bbo.ask_price == Price_INVALID)) {
            spread_ = Price_INVALID;
            return;
        }
        spread_ = bbo.ask_price - bbo.bid_price;
    }

    Price value() const noexcept { return spread_; }
    const char* name() const noexcept { return "spread"; }

private:
    Price spread_ = Price_INVALID;
};

}
