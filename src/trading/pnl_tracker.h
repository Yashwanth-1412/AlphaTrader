#pragma once

#include "src/types.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>

namespace alphatrader {

template <size_t MaxTickers = 16>
class PnLTracker {
public:
    bool registerTicker(TickerId ticker_id) noexcept {
        if (get(ticker_id)) return true;
        if (count_ == MaxTickers) return false;
        entries_[count_++].ticker_id = ticker_id;
        return true;
    }

    Position* get(TickerId ticker_id) noexcept {
        for (size_t i = 0; i < count_; ++i)
            if (entries_[i].ticker_id == ticker_id)
                return &entries_[i].position;
        return nullptr;
    }

    const Position* get(TickerId ticker_id) const noexcept {
        for (size_t i = 0; i < count_; ++i)
            if (entries_[i].ticker_id == ticker_id)
                return &entries_[i].position;
        return nullptr;
    }

    bool onExecution(TickerId ticker_id, Side side, Price price, Quantity qty) noexcept {
        Position* current = get(ticker_id);
        if (!current || price == Price_INVALID || qty == 0) return false;

        Position next = *current;
        const int64_t signed_qty = side == Side::BUY ? static_cast<int64_t>(qty) : -static_cast<int64_t>(qty);

        if (next.net_qty == 0) {
            next.net_qty = signed_qty;
            next.avg_entry_price = price;
        } else if ((next.net_qty > 0) == (signed_qty > 0)) {
            const int64_t old_qty = absQty(next.net_qty);
            const int64_t new_qty = old_qty + static_cast<int64_t>(qty);
            const __int128 weighted_price = static_cast<__int128>(next.avg_entry_price) * old_qty +
                                            static_cast<__int128>(price) * qty;
            next.avg_entry_price = static_cast<Price>(weighted_price / new_qty);
            next.net_qty += signed_qty;
        } else {
            const int64_t open_qty = absQty(next.net_qty);
            const int64_t closing_qty = open_qty < static_cast<int64_t>(qty) ? open_qty : qty;
            const int64_t pnl_per_unit = next.net_qty > 0
                ? static_cast<int64_t>(price) - next.avg_entry_price
                : static_cast<int64_t>(next.avg_entry_price) - price;
            const __int128 realized = static_cast<__int128>(next.realized_pnl) +
                                      static_cast<__int128>(pnl_per_unit) * closing_qty;
            if (!fitsInt64(realized)) return false;
            next.realized_pnl = static_cast<int64_t>(realized);

            if (static_cast<int64_t>(qty) > open_qty) {
                next.net_qty = side == Side::BUY
                    ? static_cast<int64_t>(qty) - open_qty
                    : open_qty - static_cast<int64_t>(qty);
                next.avg_entry_price = price;
            } else {
                next.net_qty += signed_qty;
                if (next.net_qty == 0) next.avg_entry_price = Price_INVALID;
            }
        }

        if (!updateUnrealized(next)) return false;
        *current = next;
        return true;
    }

    bool onMark(TickerId ticker_id, Price mark_price) noexcept {
        Position* position = get(ticker_id);
        if (!position || mark_price == Price_INVALID) return false;

        Position next = *position;
        next.mark_price = mark_price;
        if (!updateUnrealized(next)) return false;
        *position = next;
        return true;
    }

    std::optional<int64_t> totalPnL() const noexcept {
        __int128 total = 0;
        for (size_t i = 0; i < count_; ++i) {
            total += entries_[i].position.realized_pnl;
            total += entries_[i].position.unrealized_pnl;
        }
        if (!fitsInt64(total)) return std::nullopt;
        return static_cast<int64_t>(total);
    }

    size_t count() const noexcept { return count_; }

private:
    struct Entry {
        TickerId ticker_id = TickerId_INVALID;
        Position position;
    };

    static int64_t absQty(int64_t qty) noexcept {
        return qty < 0 ? -qty : qty;
    }

    static bool fitsInt64(__int128 value) noexcept {
        return value >= std::numeric_limits<int64_t>::min() &&
               value <= std::numeric_limits<int64_t>::max();
    }

    static bool updateUnrealized(Position& position) noexcept {
        if (position.net_qty == 0 || position.mark_price == Price_INVALID) {
            position.unrealized_pnl = 0;
            return true;
        }

        const __int128 unrealized = static_cast<__int128>(position.net_qty) *
            (static_cast<int64_t>(position.mark_price) - position.avg_entry_price);
        if (!fitsInt64(unrealized)) return false;
        position.unrealized_pnl = static_cast<int64_t>(unrealized);
        return true;
    }

    Entry entries_[MaxTickers];
    size_t count_ = 0;
};

}
