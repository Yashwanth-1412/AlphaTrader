#pragma once

#include "src/market_order_book.h"
#include "src/trading/features/order_book_imbalance_feature.h"
#include "src/trading/order_manager.h"
#include "src/types.h"

#include <cstddef>
#include <optional>

namespace alphatrader {

// Strategy interfaces — one per strategy slot.
//
// IocSignalConfig      {order_qty, entry_imbalance} — threshold config
// Entry<Features>      per-ticker state handed to hooks: {ticker_id, book* (shared),
//                       features (per-slot)}; book is owned by the TradeEngine
// Strategy concept     requires onBookUpdate/onTrade (return orders submitted)
//                      and onOrderUpdate(const OMOrder&) on a strategy type
//
// LiquidityTaker — fires IOC orders at the best quote when book imbalance
// crosses the threshold. Public API:
//   ctor(IocSignalConfig)
//   onBookUpdate(entry, update, om, pnl)   re-evaluate signal, return orders sent
//   onTrade(entry, update, om, pnl)        same, on trade prints
//   onOrderUpdate(order)                   notified on every order state change

struct IocSignalConfig {
    Quantity order_qty       = 0;
    Ratio    entry_imbalance = Ratio_SCALE;
};

// Per-ticker state the engine maintains and hands to strategies.
template<typename Features>
struct Entry {
    TickerId                ticker_id = TickerId_INVALID;
    MarketOrderBook*        book      = nullptr; // shared, owned by the engine's ticker registry
    std::optional<Features> features;
};

// A strategy observes market updates per ticker and manages orders through the
// order manager. Hooks return the number of orders submitted.
template<typename S, typename Features, typename OrderManagerT, typename PnlT>
concept Strategy = requires(S s, Entry<Features>& entry, const MarketUpdate& update, const OMOrder& order, OrderManagerT& om, PnlT& pnl) {
    { s.onBookUpdate(entry, update, om, pnl) } -> std::convertible_to<size_t>;
    { s.onTrade(entry, update, om, pnl) } -> std::convertible_to<size_t>;
    s.onOrderUpdate(order);
};

// Fires IOC orders at the best quote when order book imbalance crosses a
// threshold. Sits on the inside of the book and pays the spread.
class LiquidityTaker {
  public:
    LiquidityTaker() = default;
    explicit LiquidityTaker(IocSignalConfig config) noexcept
        : config_(config) {}

    template<typename Features, typename OrderManagerT, typename PnlT>
    size_t onBookUpdate(Entry<Features>& entry, const MarketUpdate& update, OrderManagerT& om, PnlT& pnl) noexcept {
        (void)update;
        return evaluate(entry, om, pnl);
    }

    template<typename Features, typename OrderManagerT, typename PnlT>
    size_t onTrade(Entry<Features>& entry, const MarketUpdate& update, OrderManagerT& om, PnlT& pnl) noexcept {
        (void)update;
        return evaluate(entry, om, pnl);
    }

    void onOrderUpdate(const OMOrder& order) noexcept { (void)order; }

  private:
    template<typename Features, typename OrderManagerT, typename PnlT>
    size_t evaluate(Entry<Features>& entry, OrderManagerT& om, PnlT& pnl) noexcept {
        const Ratio imbalance = entry.features->template get<OrderBookImbalanceFeature>().value();
        if (imbalance == Ratio_INVALID || config_.order_qty == 0)
            return 0;

        const BBO& bbo = entry.book->getBBO();
        Side       side;
        Price      price;
        if (imbalance >= config_.entry_imbalance) {
            side  = Side::BUY;
            price = bbo.ask_price;
        } else if (imbalance <= -config_.entry_imbalance) {
            side  = Side::SELL;
            price = bbo.bid_price;
        } else {
            return 0;
        }

        Position* position = pnl.get(entry.ticker_id);
        if (!position || price == Price_INVALID)
            return 0;

        const OrderRequest request{OrderRequestType::NEW, 0, 0, entry.ticker_id, side, price, config_.order_qty, true};
        return om.sendNewOrder(request, *position) ? 1 : 0;
    }

    IocSignalConfig config_;
};

} // namespace alphatrader
