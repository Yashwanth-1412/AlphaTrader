#pragma once

#include "src/types.h"

#include <cstddef>

namespace alphatrader {

// OrderIdAllocator — one shared order id sequence across all strategy slots.
// The owning slot is encoded in the id itself so exchange responses can be
// routed back without a side table.
class OrderIdAllocator {
  public:
    static constexpr size_t  SLOT_BITS    = 16; // 65,536 slots in the high bits
    static constexpr size_t  COUNTER_BITS = 64 - SLOT_BITS;
    static constexpr OrderId COUNTER_MASK = (OrderId{1} << COUNTER_BITS) - 1;

    explicit OrderIdAllocator(size_t slot_count = 1) noexcept
        : slot_count_(slot_count) {}

    // Unique, monotonic id for the given slot: (slot << COUNTER_BITS) | counter.
    // The counter is masked so it can never bleed into the slot bits.
    OrderId next(size_t slot) noexcept {
        return (static_cast<OrderId>(slot) << COUNTER_BITS) | (counter_++ & COUNTER_MASK);
    }

    // Slot that owns the given id, or slot_count_ if the id is out of range.
    size_t ownerOf(OrderId id) const noexcept {
        const size_t slot = static_cast<size_t>(id >> COUNTER_BITS);
        return slot < slot_count_ ? slot : slot_count_;
    }

    // Next id that will be handed out (slot 0 view).
    OrderId peek() const noexcept { return counter_ & COUNTER_MASK; }

    size_t slotCount() const noexcept { return slot_count_; }

  private:
    size_t  slot_count_ = 1;
    OrderId counter_    = 1;
};

} // namespace alphatrader
