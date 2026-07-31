#pragma once

#include "src/market_order_book.h"

namespace alphatrader {

struct MktPriceFeature {
    void onMarketUpdate(const MarketUpdate&, const MarketOrderBook& book) noexcept {
        const BBO& bbo = book.getBBO();
        if (UNLIKELY(bbo.bid_price == Price_INVALID || bbo.ask_price == Price_INVALID ||
                     bbo.bid_qty == 0 || bbo.ask_qty == 0)) {
            price_ = Price_INVALID;
            return;
        }
        price_ = static_cast<Price>(
            (static_cast<int64_t>(bbo.ask_price) * bbo.bid_qty +
             static_cast<int64_t>(bbo.bid_price) * bbo.ask_qty) /
            (static_cast<int64_t>(bbo.bid_qty) + bbo.ask_qty));
    }

    Price value() const noexcept { return price_; }
    const char* name() const noexcept { return "mkt_price"; }

private:
    Price price_ = Price_INVALID;
};

}
