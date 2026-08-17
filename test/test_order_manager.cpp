#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "QuantLink/Lib/logging/logger.h"
#include "trading/order_id_allocator.h"
#include "trading/order_manager.h"
#include "trading/pnl_tracker.h"
#include "trading/risk_manager.h"

#include <cstdio>

using namespace alphatrader;

namespace {

int passes   = 0;
int failures = 0;

void check(const char* description, bool condition) {
    std::printf("%s: %s\n", condition ? "PASS" : "FAIL", description);
    condition ? ++passes : ++failures;
}

using OM = OrderManager<RiskManager<4, 8>, PnLTracker<4>, 4>;

struct CallbackLog {
    size_t       notifications = 0;
    OMOrderState last_state    = OMOrderState::INVALID;
    OrderId      last_order_id = 0;
};

void onOrderUpdate(const OMOrder& order, void* user_data) {
    auto* log = static_cast<CallbackLog*>(user_data);
    ++log->notifications;
    log->last_state    = order.state;
    log->last_order_id = order.request.client_order_id;
}

} // namespace

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    quantlink::Logger                   logger(64, "order_manager_test.log", -1);
    quantlink::SPSCQueue<OrderRequest>  outgoing(32);
    quantlink::SPSCQueue<OrderResponse> responses(32);

    PnLTracker<4> pnl;
    check("register pnl ticker", pnl.registerTicker(1));
    RiskManager<4, 8> risk;
    const RiskLimits  limits{200, 100, 200'000'000};
    check("register risk ticker", risk.registerTicker(1, limits));

    OM               om;
    CallbackLog      log;
    OrderIdAllocator allocator(1);
    om.initialize(&risk, &pnl, &outgoing, &allocator, 0, onOrderUpdate, &log);

    // ── New orders ──────────────────────────────────────────────
    const Position*    pos = pnl.get(1);
    const OrderRequest buy{OrderRequestType::NEW, 0, 0, 1, Side::BUY, 1'000'000, 40, true};
    const OrderRequest breach{OrderRequestType::NEW, 0, 0, 1, Side::SELL, 1'001'000, 250, true};

    check("send new order assigns client id 1", om.sendNewOrder(buy, *pos));
    check("next id is 2", allocator.peek() == 2);
    OrderRequest sent{};
    check("request pushed to gateway queue", outgoing.pop(sent));
    check("pushed request carries assigned id", sent.client_order_id == 1 && sent.qty == 40 && sent.side == Side::BUY);
    check("exposure reserved on submit", risk.pendingExposure(1) == 40);
    check("order recorded as NOT_ACTIVE", om.get(1) && om.get(1)->state == OMOrderState::NOT_ACTIVE);
    check("callback fired on submit", log.notifications == 1 && log.last_order_id == 1 && log.last_state == OMOrderState::NOT_ACTIVE);

    check("risk rejection blocks order (position breach)", !om.sendNewOrder(breach, *pos) && risk.pendingExposure(1) == 40);
    OrderRequest stray{};
    check("blocked order not pushed", !outgoing.pop(stray));

    // ── Accept ──────────────────────────────────────────────────
    const OrderResponse accepted{1, 100, Side::BUY, 1'000'000, 40, 0, 0, 0, OrderResponse::ACCEPTED};
    check("accept transitions to ACTIVE", om.onOrderResponse(accepted));
    check("state is ACTIVE", om.get(1) && om.get(1)->state == OMOrderState::ACTIVE);
    check("active count is 1", om.activeCount() == 1);
    check("callback fired on accept", log.last_state == OMOrderState::ACTIVE);

    // ── Partial then full fill ──────────────────────────────────
    const OrderResponse partial{1, 0, Side::BUY, 0, 0, 25, 1'000'000, 0, OrderResponse::EXECUTED};
    check("partial fill accepted", om.onOrderResponse(partial));
    check("state is PARTIALLY_FILLED", om.get(1)->state == OMOrderState::PARTIALLY_FILLED);
    check("filled quantity tracked", om.get(1)->filled_qty == 25);
    check("avg fill price tracked", om.get(1)->avg_fill_price == 1'000'000);
    check("position updated through risk", pnl.get(1)->net_qty == 25);
    check("reservation reduced", risk.pendingExposure(1) == 15);

    const OrderResponse rest{1, 0, Side::BUY, 0, 0, 15, 1'002'000, 0, OrderResponse::EXECUTED};
    check("final fill completes order", om.onOrderResponse(rest));
    check("state is COMPLETED", om.get(1) && om.get(1)->state == OMOrderState::COMPLETED);
    check("filled == requested", om.get(1)->filled_qty == 40);
    check("avg price weighted across fills", om.get(1)->avg_fill_price == 1'000'750);
    check("reservation released", risk.pendingExposure(1) == 0);
    check("active count is 0", om.activeCount() == 0);

    check("overfill after completion rejected", !om.onOrderResponse(rest));

    // ── Cancel flows ────────────────────────────────────────────
    const OrderRequest more{OrderRequestType::NEW, 0, 0, 1, Side::BUY, 1'000'000, 50, true};
    check("send second order", om.sendNewOrder(more, *pos) && allocator.peek() == 4);
    OrderRequest more_pushed{};
    check("second order pushed", outgoing.pop(more_pushed) && more_pushed.client_order_id == 3);
    check("completed slot reused", !om.get(1) && om.get(3) && om.get(3)->request.client_order_id == 3);
    check("second order is NOT_ACTIVE", om.get(3)->state == OMOrderState::NOT_ACTIVE);
    OrderRequest cancel_payload{};
    check("cancel sends request", om.sendCancel(3, 50));
    check("cancel request pushed", outgoing.pop(cancel_payload));
    check("cancel carries order id and qty", cancel_payload.type == OrderRequestType::CANCEL && cancel_payload.client_order_id == 3 && cancel_payload.qty == 50);
    check("cancel marked requested", om.get(3) && om.get(3)->is_cancel_requested);
    check("duplicate cancel rejected", !om.sendCancel(3, 50));

    const OrderResponse cancel_rejected{3, 0, Side::BUY, 0, 0, 0, 0, 0, OrderResponse::CANCEL_REJECTED};
    check("cancel rejection keeps order alive", om.onOrderResponse(cancel_rejected));
    check("cancel flag cleared", om.get(3) && !om.get(3)->is_cancel_requested);
    check("cancel rejected flagged", om.get(3) && om.get(3)->is_cancel_rejected);

    check("cancel retry allowed after rejection", om.sendCancel(3, 50));
    OrderRequest ignored{};
    outgoing.pop(ignored);
    const OrderResponse cancel_confirmed{3, 0, Side::BUY, 0, 0, 0, 0, 50, OrderResponse::CANCELED};
    check("cancel confirmation accepted", om.onOrderResponse(cancel_confirmed));
    check("state is CANCELLED", om.get(3) && om.get(3)->state == OMOrderState::CANCELLED);
    check("cancel releases reservation", risk.pendingExposure(1) == 0);

    // ── Replace ─────────────────────────────────────────────────
    const OrderRequest repl{OrderRequestType::NEW, 0, 0, 1, Side::SELL, 1'000'000, 20, true};
    check("send replace-able order", om.sendNewOrder(repl, *pos));
    OrderRequest repl_pushed{};
    outgoing.pop(repl_pushed);
    const OrderId replaced_id = repl_pushed.client_order_id;
    check("replaced order reuses cancelled slot", !om.get(3) && om.get(replaced_id) && om.get(replaced_id)->request.client_order_id == replaced_id);
    const OrderId new_id = allocator.peek();
    check("replace assigns new client id", om.sendReplace(replaced_id, 1'005'000, 20) && allocator.peek() == new_id + 1);
    OrderRequest replace_payload{};
    check("replace request pushed", outgoing.pop(replace_payload));
    check("replace carries old and new ids",
          replace_payload.type == OrderRequestType::REPLACE && replace_payload.client_order_id == replaced_id && replace_payload.new_client_order_id == new_id && replace_payload.price == 1'005'000);
    check("order re-keyed under new id", !om.get(replaced_id) && om.get(new_id) && om.get(new_id)->request.price == 1'005'000);

    const OrderResponse replace_confirmed{new_id, 300, Side::SELL, 1'005'000, 20, 0, 0, 0, OrderResponse::ACCEPTED};
    check("replaced order accepts under new id", om.onOrderResponse(replace_confirmed));
    check("replaced order active", om.get(new_id) && om.get(new_id)->state == OMOrderState::ACTIVE);

    // ── Reject ──────────────────────────────────────────────────
    check("cleanup replaced order", om.sendCancel(new_id, 20));
    OrderRequest cleanup_cancel{};
    outgoing.pop(cleanup_cancel);
    const OrderResponse cleanup{new_id, 0, Side::SELL, 0, 0, 0, 0, 20, OrderResponse::CANCELED};
    check("cleanup cancel confirmed", om.onOrderResponse(cleanup));
    check("cleanup releases reservation", risk.pendingExposure(1) == 0);

    check("slot reuse keeps count bounded", om.sendNewOrder(more, *pos) && allocator.peek() == new_id + 2 && om.orderCount() == 1);
    check("final order present", !om.get(new_id) && om.get(new_id + 1) && om.get(new_id + 1)->state == OMOrderState::NOT_ACTIVE);
    const OrderId       rejected_id = om.get(new_id + 1)->request.client_order_id;
    const OrderResponse rejected{rejected_id, 0, Side::BUY, 0, 0, 0, 0, 0, OrderResponse::REJECTED};
    check("rejection accepted", om.onOrderResponse(rejected));
    check("state is DEAD", om.get(rejected_id) && om.get(rejected_id)->state == OMOrderState::DEAD);
    check("rejection releases reservation", risk.pendingExposure(1) == 0);

    check("unknown response id rejected", !om.onOrderResponse({999, 0, Side::BUY, 0, 0, 0, 0, 0, OrderResponse::ACCEPTED}));

    std::remove("order_manager_test.log");
    std::printf("Results: %d passed, %d failed\n", passes, failures);
    return failures == 0 ? 0 : 1;
}
