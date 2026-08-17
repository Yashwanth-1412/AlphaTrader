#pragma once

#include "../types.h"
#include "QuantLink/Lib/protocol/itch_messages.h"

#include <span>
#include <unordered_map>
#include <vector>

namespace alphatrader {

class ItchDecoder final {
  public:
    ItchDecoder() = default;

    ItchDecoder(const ItchDecoder&)            = delete;
    ItchDecoder& operator=(const ItchDecoder&) = delete;

    auto decode(std::span<const char> wire, MarketUpdate& out) noexcept -> bool;
    auto decodeSnapshot(std::span<const char> wire, MarketUpdate& out) noexcept -> bool;
    auto reset() noexcept -> void;

    [[nodiscard]] auto trackedOrders() const noexcept -> size_t { return tracked_order_count_; }
    [[nodiscard]] auto lastDecodedSize() const noexcept -> size_t { return last_decoded_size_; }

    // Distinguishes the two reasons decode() returns false: a truncated frame
    // (wait for more bytes) versus an unparseable one (resynchronise). Without
    // this the caller cannot tell them apart and stalls forever on garbage.
    [[nodiscard]] auto needsMoreData() const noexcept -> bool { return need_more_data_; }

  private:
    struct OrderState {
        Side     side;
        Price    price;
        Quantity qty;
        TickerId ticker_id;
    };

    auto decodeSystemEvent(const quantlink::itch::SystemEvent* msg, MarketUpdate& out) noexcept -> bool;
    auto decodeAddOrder(const quantlink::itch::AddOrder* msg, MarketUpdate& out) noexcept -> bool;
    auto decodeAddOrderMPID(const quantlink::itch::AddOrderMPID* msg, MarketUpdate& out) noexcept -> bool;
    auto decodeOrderExecuted(const quantlink::itch::OrderExecuted* msg, MarketUpdate& out) noexcept -> bool;
    auto decodeOrderCancel(const quantlink::itch::OrderCancel* msg, MarketUpdate& out) noexcept -> bool;
    auto decodeOrderDelete(const quantlink::itch::OrderDelete* msg, MarketUpdate& out) noexcept -> bool;
    auto decodeOrderReplace(const quantlink::itch::OrderReplace* msg, MarketUpdate& out) noexcept -> bool;

    std::unordered_map<TickerId, std::vector<OrderState>> order_ref_map_;
    size_t                                                tracked_order_count_ = 0;
    size_t                                                last_decoded_size_   = 0;
    bool                                                  need_more_data_      = false;
};

} // namespace alphatrader
