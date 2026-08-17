#pragma once

#include "src/market_order_book.h"

#include <cstddef>

namespace alphatrader {

template<typename Feature>
concept FeaturePlugin = requires(Feature feature, const MarketUpdate& update, const MarketOrderBook& book) { feature.onMarketUpdate(update, book); };

template<typename... Features>
    requires(FeaturePlugin<Features> && ...)
struct FeatureEngine : public Features... {
    void onMarketUpdate(const MarketUpdate& update, const MarketOrderBook& book) noexcept { (Features::onMarketUpdate(update, book), ...); }

    template<typename T>
    T& get() noexcept {
        return *static_cast<T*>(this);
    }

    template<typename T>
    const T& get() const noexcept {
        return *static_cast<const T*>(this);
    }

    static constexpr size_t count() noexcept { return sizeof...(Features); }
};

} // namespace alphatrader
