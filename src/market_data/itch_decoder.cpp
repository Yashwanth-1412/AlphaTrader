#include "itch_decoder.h"

#include "QuantLink/Lib/common/macros.h"

namespace alphatrader {

using namespace quantlink::itch;

namespace {
auto tickerId(const char* stock) noexcept -> TickerId {
    uint64_t value = 0;
    std::memcpy(&value, stock, sizeof(value));
    return static_cast<TickerId>(value);
}
}

auto ItchDecoder::decode(std::span<const char> wire, MarketUpdate& out) noexcept -> bool {
    last_decoded_size_ = 0;
    if (wire.size() < sizeof(SeqNum) + sizeof(char)) {
        return false;
    }

    SeqNum seq = 0;
    std::memcpy(&seq, wire.data(), sizeof(SeqNum));
    out.seq_num = seq;

    const char msg_type = wire[sizeof(SeqNum)];

    switch (msg_type) {
        case static_cast<char>(enums::MsgType::SYSTEM_EVENT):
            if (wire.size() < sizeof(SeqNum) + sizeof(SystemEvent)) return false;
            last_decoded_size_ = sizeof(SystemEvent);
            return decodeSystemEvent(reinterpret_cast<const SystemEvent*>(wire.data() + sizeof(SeqNum)), out);

        case static_cast<char>(enums::MsgType::ADD_ORDER):
            if (wire.size() < sizeof(SeqNum) + sizeof(AddOrder)) return false;
            last_decoded_size_ = sizeof(AddOrder);
            return decodeAddOrder(reinterpret_cast<const AddOrder*>(wire.data() + sizeof(SeqNum)), out);

        case static_cast<char>(enums::MsgType::ADD_ORDER_MPID):
            if (wire.size() < sizeof(SeqNum) + sizeof(AddOrderMPID)) return false;
            last_decoded_size_ = sizeof(AddOrderMPID);
            return decodeAddOrderMPID(reinterpret_cast<const AddOrderMPID*>(wire.data() + sizeof(SeqNum)), out);

        case static_cast<char>(enums::MsgType::ORDER_EXECUTED):
            if (wire.size() < sizeof(SeqNum) + sizeof(OrderExecuted)) return false;
            last_decoded_size_ = sizeof(OrderExecuted);
            return decodeOrderExecuted(reinterpret_cast<const OrderExecuted*>(wire.data() + sizeof(SeqNum)), out);

        case static_cast<char>(enums::MsgType::ORDER_CANCEL):
            if (wire.size() < sizeof(SeqNum) + sizeof(OrderCancel)) return false;
            last_decoded_size_ = sizeof(OrderCancel);
            return decodeOrderCancel(reinterpret_cast<const OrderCancel*>(wire.data() + sizeof(SeqNum)), out);

        case static_cast<char>(enums::MsgType::ORDER_DELETE):
            if (wire.size() < sizeof(SeqNum) + sizeof(OrderDelete)) return false;
            last_decoded_size_ = sizeof(OrderDelete);
            return decodeOrderDelete(reinterpret_cast<const OrderDelete*>(wire.data() + sizeof(SeqNum)), out);

        case static_cast<char>(enums::MsgType::ORDER_REPLACE):
            if (wire.size() < sizeof(SeqNum) + sizeof(OrderReplace)) return false;
            last_decoded_size_ = sizeof(OrderReplace);
            return decodeOrderReplace(reinterpret_cast<const OrderReplace*>(wire.data() + sizeof(SeqNum)), out);

        default:
            return false;
    }
}

auto ItchDecoder::decodeSnapshot(std::span<const char> wire, MarketUpdate& out) noexcept -> bool {
    last_decoded_size_ = 0;
    if (wire.empty()) return false;

    out.seq_num = 0;
    const char msg_type = wire[0];

    switch (msg_type) {
        case static_cast<char>(enums::MsgType::SYSTEM_EVENT):
            if (wire.size() < sizeof(SystemEvent)) return false;
            last_decoded_size_ = sizeof(SystemEvent);
            return decodeSystemEvent(reinterpret_cast<const SystemEvent*>(wire.data()), out);

        case static_cast<char>(enums::MsgType::ADD_ORDER):
            if (wire.size() < sizeof(AddOrder)) return false;
            last_decoded_size_ = sizeof(AddOrder);
            return decodeAddOrder(reinterpret_cast<const AddOrder*>(wire.data()), out);

        case static_cast<char>(enums::MsgType::ADD_ORDER_MPID):
            if (wire.size() < sizeof(AddOrderMPID)) return false;
            last_decoded_size_ = sizeof(AddOrderMPID);
            return decodeAddOrderMPID(reinterpret_cast<const AddOrderMPID*>(wire.data()), out);

        case static_cast<char>(enums::MsgType::ORDER_EXECUTED):
            if (wire.size() < sizeof(OrderExecuted)) return false;
            last_decoded_size_ = sizeof(OrderExecuted);
            return decodeOrderExecuted(reinterpret_cast<const OrderExecuted*>(wire.data()), out);

        case static_cast<char>(enums::MsgType::ORDER_CANCEL):
            if (wire.size() < sizeof(OrderCancel)) return false;
            last_decoded_size_ = sizeof(OrderCancel);
            return decodeOrderCancel(reinterpret_cast<const OrderCancel*>(wire.data()), out);

        case static_cast<char>(enums::MsgType::ORDER_DELETE):
            if (wire.size() < sizeof(OrderDelete)) return false;
            last_decoded_size_ = sizeof(OrderDelete);
            return decodeOrderDelete(reinterpret_cast<const OrderDelete*>(wire.data()), out);

        case static_cast<char>(enums::MsgType::ORDER_REPLACE):
            if (wire.size() < sizeof(OrderReplace)) return false;
            last_decoded_size_ = sizeof(OrderReplace);
            return decodeOrderReplace(reinterpret_cast<const OrderReplace*>(wire.data()), out);

        default:
            return false;
    }
}

auto ItchDecoder::decodeSystemEvent(const SystemEvent* msg, MarketUpdate& out) noexcept -> bool {
    (void)msg;
    (void)out;
    return false;
}

auto ItchDecoder::decodeAddOrder(const AddOrder* msg, MarketUpdate& out) noexcept -> bool {
    out.type = UpdateType::ADD;
    out.side = (msg->buy_sell == 'B') ? Side::BUY : Side::SELL;
    out.price = static_cast<Price>(swap32(msg->price));
    out.qty = msg->get_shares();
    out.order_ref = msg->get_ref();
    out.new_ref = 0;
    out.ticker_id = tickerId(msg->stock);

    order_ref_map_[out.order_ref] = {out.side, out.price, out.qty, out.ticker_id};
    return true;
}

auto ItchDecoder::decodeAddOrderMPID(const AddOrderMPID* msg, MarketUpdate& out) noexcept -> bool {
    out.type = UpdateType::ADD_MPID;
    out.side = (msg->buy_sell == 'B') ? Side::BUY : Side::SELL;
    out.price = static_cast<Price>(swap32(msg->price));
    out.qty = msg->get_shares();
    out.order_ref = msg->get_ref();
    out.new_ref = 0;
    out.ticker_id = tickerId(msg->stock);

    order_ref_map_[out.order_ref] = {out.side, out.price, out.qty, out.ticker_id};
    return true;
}

auto ItchDecoder::decodeOrderExecuted(const OrderExecuted* msg, MarketUpdate& out) noexcept -> bool {
    out.type = UpdateType::EXECUTE;
    out.qty = msg->get_exec_shares();
    out.order_ref = msg->get_ref();
    out.new_ref = msg->get_match();

    const auto it = order_ref_map_.find(out.order_ref);
    if (UNLIKELY(it == order_ref_map_.end())) {
        return false;
    }

    out.side = it->second.side;
    out.price = it->second.price;
    out.ticker_id = it->second.ticker_id;
    if (out.qty >= it->second.qty) order_ref_map_.erase(it);
    else it->second.qty -= out.qty;
    return true;
}

auto ItchDecoder::decodeOrderCancel(const OrderCancel* msg, MarketUpdate& out) noexcept -> bool {
    out.type = UpdateType::CANCEL;
    out.qty = msg->get_canceled();
    out.order_ref = msg->get_ref();
    out.new_ref = 0;

    const auto it = order_ref_map_.find(out.order_ref);
    if (UNLIKELY(it == order_ref_map_.end())) {
        return false;
    }

    out.side = it->second.side;
    out.price = it->second.price;
    out.ticker_id = it->second.ticker_id;
    if (out.qty >= it->second.qty) order_ref_map_.erase(it);
    else it->second.qty -= out.qty;
    return true;
}

auto ItchDecoder::decodeOrderDelete(const OrderDelete* msg, MarketUpdate& out) noexcept -> bool {
    const uint16_t stock_locate = swap16(msg->stock_locate);
    const OrderId ref = msg->get_ref_num();

    if (UNLIKELY(stock_locate == SnapshotControl::SNAPSHOT_START)) {
        out.type = UpdateType::SNAPSHOT_START;
        out.order_ref = ref; // ref carries last_inc_seq_num
        out.new_ref = 0;
        out.ticker_id = TickerId_INVALID;
        return true;
    }

    if (UNLIKELY(stock_locate == SnapshotControl::SNAPSHOT_CLEAR)) {
        out.type = UpdateType::SNAPSHOT_CLEAR;
        out.order_ref = ref; // ref carries ticker_id
        out.new_ref = 0;
        out.ticker_id = static_cast<TickerId>(ref);
        return true;
    }

    if (UNLIKELY(stock_locate == SnapshotControl::SNAPSHOT_END)) {
        out.type = UpdateType::SNAPSHOT_END;
        out.order_ref = ref; // ref carries last_inc_seq_num
        out.new_ref = 0;
        out.ticker_id = TickerId_INVALID;
        return true;
    }

    out.type = UpdateType::DELETE;
    out.qty = 0;
    out.order_ref = ref;
    out.new_ref = 0;

    const auto it = order_ref_map_.find(out.order_ref);
    if (UNLIKELY(it == order_ref_map_.end())) {
        return false;
    }

    out.side = it->second.side;
    out.price = it->second.price;
    out.ticker_id = it->second.ticker_id;
    order_ref_map_.erase(it);
    return true;
}

auto ItchDecoder::decodeOrderReplace(const OrderReplace* msg, MarketUpdate& out) noexcept -> bool {
    out.type = UpdateType::REPLACE;
    out.price = static_cast<Price>(swap32(msg->price));
    out.qty = msg->get_shares();
    out.order_ref = msg->get_old_ref();
    out.new_ref = msg->get_new_ref();

    const auto it = order_ref_map_.find(out.order_ref);
    if (UNLIKELY(it == order_ref_map_.end())) {
        return false;
    }

    out.side = it->second.side;
    out.ticker_id = it->second.ticker_id;
    order_ref_map_[out.new_ref] = {out.side, out.price, out.qty, out.ticker_id};
    order_ref_map_.erase(it);
    return true;
}

auto ItchDecoder::reset() noexcept -> void {
    order_ref_map_.clear();
}

} // namespace alphatrader
