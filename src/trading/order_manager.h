#pragma once

#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "src/trading/order_id_allocator.h"
#include "src/trading/risk_manager.h"
#include "src/types.h"

#include <cstddef>
#include <cstdint>

namespace alphatrader {

// Order lifecycle — one OrderManager per strategy slot.
//
// OMOrderState       NOT_ACTIVE -> ACTIVE/PARTIALLY_FILLED -> COMPLETED,
//                    CANCELLED, DEAD (rejected/inconsistent)
// OMOrder            {request, state, is_cancel_requested, is_cancel_rejected,
//                     filled_qty, avg_fill_price}
// OrderUpdateCallback  void(const OMOrder&, void* user_data)
//
// Public API:
//   initialize(risk, pnl, outgoing_q, allocator, slot,
//              callback = nullptr, user_data = nullptr)
//   bool sendNewOrder(request, position)   risk-checked NEW, assigns id from allocator
//   bool sendCancel(client_order_id, qty)  no-op if duplicate/completed
//   bool sendReplace(client_order_id, price, qty)  re-keys order under a new id
//   bool onOrderResponse(response)         state machine; keeps risk + pnl in sync
//   get(client_order_id)                   OMOrder* lookup (find)
//   activeCount() / orderCount()           live orders / total recorded slots
//   newOrderCount() / replaceCount() / cancelCount()   cumulative requests sent
//   fillCount() / filledQty() / rejectCount()          cumulative fills and rejects
enum class OMOrderState : uint8_t {
    INVALID,
    NOT_ACTIVE,       // created, not yet confirmed by the exchange
    ACTIVE,           // working on the book
    PARTIALLY_FILLED, // working on the book with some fills
    COMPLETED,        // fully filled
    CANCELLED,        // fully cancelled, may have fills
    DEAD              // rejected or inconsistent, no longer usable
};

struct OMOrder {
    OrderRequest request;
    OMOrderState state               = OMOrderState::INVALID;
    bool         is_cancel_requested = false;
    bool         is_cancel_rejected  = false;
    Quantity     filled_qty          = 0;
    Price        avg_fill_price      = Price_INVALID;
};

template<typename RiskT, typename PnlT, size_t MaxOrders = 256>
class OrderManager {
  public:
    using OrderUpdateCallback = void (*)(const OMOrder& order, void* user_data);

    OrderManager() = default;

    OrderManager(const OrderManager&)            = delete;
    OrderManager& operator=(const OrderManager&) = delete;

    auto initialize(RiskT* risk, PnlT* pnl, quantlink::SPSCQueue<OrderRequest>* outgoing, OrderIdAllocator* allocator, size_t slot, OrderUpdateCallback callback = nullptr,
                    void* user_data = nullptr) noexcept -> void {
        risk_      = risk;
        pnl_       = pnl;
        outgoing_  = outgoing;
        allocator_ = allocator;
        slot_      = slot;
        callback_  = callback;
        user_data_ = user_data;
    }

    // Risk-checked NEW order. Assigns client_order_id, pushes to the gateway queue,
    // reserves exposure with the risk manager, and records the order.
    auto sendNewOrder(const OrderRequest& request, const Position& position) noexcept -> bool {
        if (!outgoing_ || !risk_ || !allocator_)
            return false;
        const size_t slot = allocSlot();
        if (slot == MaxOrders)
            return false;

        OMOrder order;
        order.request                     = request;
        order.request.client_order_id     = allocator_->next(slot_);
        order.request.new_client_order_id = 0;

        if (risk_->canSubmit(order.request, position) != RiskResult::ACCEPTED) {
            ++rejected_;
            return false;
        }
        if (!outgoing_->push(order.request))
            return false;
        if (!risk_->onSubmitted(order.request))
            return false;

        ++new_orders_;
        order.state   = OMOrderState::NOT_ACTIVE;
        orders_[slot] = order;
        if (slot == count_)
            ++count_;
        notify(orders_[slot]);
        return true;
    }

    auto sendCancel(OrderId client_order_id, Quantity qty) noexcept -> bool {
        OMOrder* order = find(client_order_id);
        if (!order || order->state == OMOrderState::COMPLETED || order->state == OMOrderState::CANCELLED || order->state == OMOrderState::DEAD || order->is_cancel_requested) {
            return false;
        }

        OrderRequest cancel{OrderRequestType::CANCEL, client_order_id, 0, order->request.ticker_id, order->request.side, order->request.price, qty, order->request.ioc};
        if (!outgoing_ || !outgoing_->push(cancel))
            return false;
        ++cancels_;
        order->is_cancel_requested = true;
        return true;
    }

    auto sendReplace(OrderId client_order_id, Price price, Quantity qty) noexcept -> bool {
        OMOrder* order = find(client_order_id);
        if (!order || order->state == OMOrderState::COMPLETED || order->state == OMOrderState::CANCELLED || order->state == OMOrderState::DEAD || order->is_cancel_requested || !outgoing_ ||
            !allocator_) {
            return false;
        }

        const OrderId new_id = allocator_->next(slot_);
        OrderRequest  replace{OrderRequestType::REPLACE, client_order_id, new_id, order->request.ticker_id, order->request.side, price, qty, order->request.ioc};
        if (!outgoing_->push(replace))
            return false;
        if (!risk_ || !risk_->onReplaced(client_order_id, new_id))
            return false;
        ++replaces_;

        OMOrder updated                     = *order;
        updated.request                     = replace;
        updated.request.client_order_id     = new_id; // continue under the new id
        updated.request.new_client_order_id = 0;
        *order                              = updated;
        return true;
    }

    // Applies a gateway response, updates the order state, and keeps the risk
    // manager's pending exposure and PnL in sync. Returns false on inconsistency.
    auto onOrderResponse(const OrderResponse& response) noexcept -> bool {
        OMOrder* order = find(response.client_order_id);
        if (!order)
            return false;

        switch (response.status) {
        case OrderResponse::ACCEPTED:
            if (order->state == OMOrderState::NOT_ACTIVE)
                order->state = OMOrderState::ACTIVE;
            else if (order->state != OMOrderState::ACTIVE && order->state != OMOrderState::PARTIALLY_FILLED)
                return false;
            notify(*order);
            return true;

        case OrderResponse::EXECUTED: {
            if (response.executed_qty == 0 || order->state == OMOrderState::COMPLETED || order->state == OMOrderState::CANCELLED || order->state == OMOrderState::DEAD) {
                return false;
            }
            const uint64_t new_filled = static_cast<uint64_t>(order->filled_qty) + response.executed_qty;
            if (new_filled > order->request.qty)
                return false; // overfill, corrupt state

            if (!risk_ || !pnl_ || !risk_->onOrderResponse(response, *pnl_)) {
                order->state = OMOrderState::DEAD;
                notify(*order);
                return false;
            }

            order->filled_qty = static_cast<Quantity>(new_filled);
            ++fills_;
            filled_qty_ += response.executed_qty;
            order->avg_fill_price = order->filled_qty == response.executed_qty ? response.execution_price
                                                                               : static_cast<Price>((static_cast<__int128>(order->avg_fill_price) * (order->filled_qty - response.executed_qty) +
                                                                                                     static_cast<__int128>(response.execution_price) * response.executed_qty) /
                                                                                                    order->filled_qty);
            order->state          = order->filled_qty == order->request.qty ? OMOrderState::COMPLETED : OMOrderState::PARTIALLY_FILLED;
            notify(*order);
            return true;
        }

        case OrderResponse::CANCELED: {
            if (order->state == OMOrderState::COMPLETED || order->state == OMOrderState::DEAD)
                return false;
            if (response.canceled_qty != 0 && (!risk_ || !risk_->onOrderResponse(response, *pnl_))) {
                return false;
            }
            order->is_cancel_requested = false;
            if (order->filled_qty == order->request.qty)
                order->state = OMOrderState::COMPLETED;
            else
                order->state = OMOrderState::CANCELLED;
            notify(*order);
            return true;
        }

        case OrderResponse::CANCEL_REJECTED:
            if (order->state == OMOrderState::COMPLETED || order->state == OMOrderState::DEAD)
                return false;
            order->is_cancel_requested = false;
            order->is_cancel_rejected  = true;
            notify(*order);
            return true;

        case OrderResponse::REJECTED:
            if (!risk_ || !risk_->onOrderResponse(response, *pnl_))
                return false;
            ++rejected_;
            order->state = OMOrderState::DEAD;
            notify(*order);
            return true;
        }
        return false;
    }

    auto get(OrderId client_order_id) noexcept -> OMOrder* { return find(client_order_id); }
    auto get(OrderId client_order_id) const noexcept -> const OMOrder* { return find(client_order_id); }

    auto activeCount() const noexcept -> size_t {
        size_t n = 0;
        for (size_t i = 0; i < count_; ++i)
            if (orders_[i].state == OMOrderState::ACTIVE || orders_[i].state == OMOrderState::PARTIALLY_FILLED || orders_[i].state == OMOrderState::NOT_ACTIVE)
                ++n;
        return n;
    }

    auto orderCount() const noexcept -> size_t { return count_; }

    auto newOrderCount() const noexcept -> size_t { return new_orders_; }
    auto replaceCount() const noexcept -> size_t { return replaces_; }
    auto cancelCount() const noexcept -> size_t { return cancels_; }
    auto fillCount() const noexcept -> size_t { return fills_; }
    auto filledQty() const noexcept -> uint64_t { return filled_qty_; }
    auto rejectCount() const noexcept -> size_t { return rejected_; }

  private:
    auto find(OrderId client_order_id) noexcept -> OMOrder* {
        for (size_t i = 0; i < count_; ++i)
            if (orders_[i].request.client_order_id == client_order_id)
                return &orders_[i];
        return nullptr;
    }

    auto find(OrderId client_order_id) const noexcept -> const OMOrder* {
        for (size_t i = 0; i < count_; ++i)
            if (orders_[i].request.client_order_id == client_order_id)
                return &orders_[i];
        return nullptr;
    }

    auto notify(const OMOrder& order) noexcept -> void {
        if (callback_)
            callback_(order, user_data_);
    }

    // Reuse slots of finished orders; otherwise append at count_.
    auto allocSlot() noexcept -> size_t {
        for (size_t i = 0; i < count_; ++i) {
            const OMOrderState state = orders_[i].state;
            if (state == OMOrderState::COMPLETED || state == OMOrderState::CANCELLED || state == OMOrderState::DEAD)
                return i;
        }
        return count_;
    }

    OMOrder                             orders_[MaxOrders];
    size_t                              count_      = 0;
    size_t                              new_orders_ = 0;
    size_t                              replaces_   = 0;
    size_t                              cancels_    = 0;
    size_t                              fills_      = 0;
    uint64_t                            filled_qty_ = 0;
    size_t                              rejected_   = 0;
    OrderIdAllocator*                   allocator_  = nullptr;
    size_t                              slot_       = 0;
    RiskT*                              risk_       = nullptr;
    PnlT*                               pnl_        = nullptr;
    quantlink::SPSCQueue<OrderRequest>* outgoing_   = nullptr;
    OrderUpdateCallback                 callback_   = nullptr;
    void*                               user_data_  = nullptr;
};

} // namespace alphatrader
