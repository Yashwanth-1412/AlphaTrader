#pragma once

#include "src/trading/pnl_tracker.h"
#include "src/types.h"

#include <cstddef>
#include <cstdint>

namespace alphatrader {

enum class RiskResult : uint8_t {
    ACCEPTED,
    TRADING_DISABLED,
    INVALID_REQUEST,
    UNKNOWN_TICKER,
    DUPLICATE_ORDER,
    PENDING_CAPACITY_EXCEEDED,
    ORDER_QTY_EXCEEDED,
    ORDER_NOTIONAL_EXCEEDED,
    POSITION_LIMIT_EXCEEDED
};

struct RiskLimits {
    int64_t max_abs_position = 0;
    Quantity max_order_qty = 0;
    int64_t max_order_notional = 0;
};

template <size_t MaxTickers = 16, size_t MaxPendingOrders = 64>
class RiskManager {
public:
    bool registerTicker(TickerId ticker_id, RiskLimits limits) noexcept {
        if (limits.max_abs_position < 0 || limits.max_order_notional < 0) return false;
        if (findTicker(ticker_id)) return false;
        if (ticker_count_ == MaxTickers) return false;
        tickers_[ticker_count_++] = {ticker_id, limits};
        return true;
    }

    void disableTrading() noexcept { trading_enabled_ = false; }
    void enableTrading() noexcept { trading_enabled_ = true; }
    bool tradingEnabled() const noexcept { return trading_enabled_; }

    RiskResult canSubmit(const OrderRequest& request, const Position& position) const noexcept {
        if (!trading_enabled_) return RiskResult::TRADING_DISABLED;
        if (request.type != OrderRequestType::NEW || request.client_order_id == 0 ||
            request.ticker_id == TickerId_INVALID || request.price == Price_INVALID ||
            request.price < 0 || request.qty == 0) {
            return RiskResult::INVALID_REQUEST;
        }

        const TickerLimits* ticker = findTicker(request.ticker_id);
        if (!ticker) return RiskResult::UNKNOWN_TICKER;
        if (findPending(request.client_order_id)) return RiskResult::DUPLICATE_ORDER;
        if (pending_count_ == MaxPendingOrders) return RiskResult::PENDING_CAPACITY_EXCEEDED;
        if (request.qty > ticker->limits.max_order_qty) return RiskResult::ORDER_QTY_EXCEEDED;

        const __int128 notional = static_cast<__int128>(request.price) * request.qty;
        if (notional > ticker->limits.max_order_notional) return RiskResult::ORDER_NOTIONAL_EXCEEDED;

        const int64_t requested_qty = request.side == Side::BUY
            ? static_cast<int64_t>(request.qty)
            : -static_cast<int64_t>(request.qty);
        const __int128 projected = static_cast<__int128>(position.net_qty) +
            pendingExposure(request.ticker_id) + requested_qty;
        const __int128 max_position = ticker->limits.max_abs_position;
        if (projected < -max_position || projected > max_position)
            return RiskResult::POSITION_LIMIT_EXCEEDED;
        return RiskResult::ACCEPTED;
    }

    bool onSubmitted(const OrderRequest& request) noexcept {
        if (findPending(request.client_order_id) || pending_count_ == MaxPendingOrders) return false;
        pending_[pending_count_++] = {request.client_order_id, request.ticker_id, request.side, request.qty};
        return true;
    }

    template <size_t PnLMaxTickers>
    bool onOrderResponse(const OrderResponse& response, PnLTracker<PnLMaxTickers>& pnl) noexcept {
        const size_t index = findPendingIndex(response.client_order_id);
        if (index == MaxPendingOrders) return false;
        PendingOrder& pending = pending_[index];

        switch (response.status) {
            case OrderResponse::ACCEPTED:
            case OrderResponse::CANCEL_REJECTED:
                return true;

            case OrderResponse::EXECUTED:
                if (response.executed_qty == 0 || response.executed_qty > pending.reserved_qty ||
                    !pnl.onExecution(pending.ticker_id, pending.side, response.execution_price,
                                     response.executed_qty)) {
                    return false;
                }
                pending.reserved_qty -= response.executed_qty;
                if (pending.reserved_qty == 0) erasePending(index);
                return true;

            case OrderResponse::CANCELED:
                if (response.canceled_qty == 0 || response.canceled_qty > pending.reserved_qty)
                    return false;
                pending.reserved_qty -= response.canceled_qty;
                if (pending.reserved_qty == 0) erasePending(index);
                return true;

            case OrderResponse::REJECTED:
                erasePending(index);
                return true;
        }
        return false;
    }

    int64_t pendingExposure(TickerId ticker_id) const noexcept {
        int64_t exposure = 0;
        for (size_t i = 0; i < pending_count_; ++i) {
            if (pending_[i].ticker_id != ticker_id) continue;
            exposure += pending_[i].side == Side::BUY
                ? static_cast<int64_t>(pending_[i].reserved_qty)
                : -static_cast<int64_t>(pending_[i].reserved_qty);
        }
        return exposure;
    }

    size_t pendingCount() const noexcept { return pending_count_; }

private:
    struct TickerLimits {
        TickerId ticker_id = TickerId_INVALID;
        RiskLimits limits;
    };

    struct PendingOrder {
        OrderId client_order_id = 0;
        TickerId ticker_id = TickerId_INVALID;
        Side side = Side::BUY;
        Quantity reserved_qty = 0;
    };

    const TickerLimits* findTicker(TickerId ticker_id) const noexcept {
        for (size_t i = 0; i < ticker_count_; ++i)
            if (tickers_[i].ticker_id == ticker_id)
                return &tickers_[i];
        return nullptr;
    }

    const PendingOrder* findPending(OrderId client_order_id) const noexcept {
        const size_t index = findPendingIndex(client_order_id);
        return index == MaxPendingOrders ? nullptr : &pending_[index];
    }

    size_t findPendingIndex(OrderId client_order_id) const noexcept {
        for (size_t i = 0; i < pending_count_; ++i)
            if (pending_[i].client_order_id == client_order_id)
                return i;
        return MaxPendingOrders;
    }

    void erasePending(size_t index) noexcept {
        pending_[index] = pending_[--pending_count_];
    }

    TickerLimits tickers_[MaxTickers];
    PendingOrder pending_[MaxPendingOrders];
    size_t ticker_count_ = 0;
    size_t pending_count_ = 0;
    bool trading_enabled_ = true;
};

}
