#pragma once

#include "src/market_order_book.h"

namespace alphatrader {

struct OrderBookImbalanceFeature {
    void onMarketUpdate(const MarketUpdate&, const MarketOrderBook& book) noexcept {
        const BBO& bbo = book.getBBO();
        int64_t total = static_cast<int64_t>(bbo.bid_qty) + bbo.ask_qty;
        if (UNLIKELY(bbo.bid_price == Price_INVALID || bbo.ask_price == Price_INVALID || total == 0)) {
            imbalance_ = Ratio_INVALID;
            return;
        }
        imbalance_ = static_cast<Ratio>(
            (static_cast<int64_t>(bbo.bid_qty) - bbo.ask_qty) * Ratio_SCALE / total);
    }

    Ratio value() const noexcept { return imbalance_; }
    const char* name() const noexcept { return "order_book_imbalance"; }

private:
    Ratio imbalance_ = Ratio_INVALID;
};

}
