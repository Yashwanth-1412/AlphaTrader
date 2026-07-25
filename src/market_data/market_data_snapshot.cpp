#include "market_data_consumer.h"

namespace alphatrader {

namespace {
auto isMulticast(const std::string& ip) noexcept -> bool {
    const in_addr addr{inet_addr(ip.c_str())};
    return (ntohl(addr.s_addr) & 0xF0000000) == 0xE0000000;
}
}

auto MarketDataConsumer::processSnapshot(const MarketUpdate& update) noexcept -> void {
    if (!in_recovery_.load(std::memory_order_relaxed)) return;

    if (update.type == UpdateType::SNAPSHOT_START) {
        snapshot_have_start_ = true;
        decoder_.reset();
        snapshot_queued_updates_.clear();
        snapshot_queued_updates_.push_back(update);
        return;
    }

    if (!snapshot_have_start_) {
        return;
    }

    snapshot_queued_updates_.push_back(update);
    if (update.type == UpdateType::SNAPSHOT_END) {
        snapshot_have_start_ = false;
        finishSnapshotSync(update.order_ref + 1);
    }
}

auto MarketDataConsumer::startSnapshotSync() noexcept -> void {
    market_data_synchronized.store(false, std::memory_order_release);
    in_recovery_.store(true, std::memory_order_release);
    snapshot_have_start_ = false;
    incremental_queued_updates_.clear();
    snapshot_queued_updates_.clear();
    snapshot_mcast_socket_.next_rcv_valid_index_ = 0;

    ASSERT(snapshot_mcast_socket_.init(snapshot_ip_, iface_, snapshot_port_, true) >= 0,
           "MarketDataConsumer::startSnapshotSync() failed to create snapshot socket");
    if (isMulticast(snapshot_ip_)) {
        ASSERT(snapshot_mcast_socket_.join(snapshot_ip_),
               "MarketDataConsumer::startSnapshotSync() failed to join snapshot group");
    }
}

auto MarketDataConsumer::finishSnapshotSync(SeqNum resume_seq) noexcept -> void {
    SeqNum expected = resume_seq;
    for (const auto& [seq_num, update] : incremental_queued_updates_) {
        if (seq_num < expected) continue;
        if (seq_num != expected) {
            snapshot_queued_updates_.clear();
            return;
        }
        ++expected;
    }

    for (const MarketUpdate& update : snapshot_queued_updates_) {
        ASSERT(incoming_md_updates_->push(update), "Market-data queue is full");
    }
    for (auto it = incremental_queued_updates_.begin(); it != incremental_queued_updates_.end();) {
        if (it->first < resume_seq) {
            it = incremental_queued_updates_.erase(it);
        } else {
            ASSERT(incoming_md_updates_->push(it->second), "Market-data queue is full");
            it = incremental_queued_updates_.erase(it);
        }
    }

    next_exp_inc_seq_num_ = expected;
    in_recovery_.store(false, std::memory_order_release);
    market_data_synchronized.store(true, std::memory_order_release);
    snapshot_queued_updates_.clear();
    snapshot_mcast_socket_.leave(snapshot_ip_, snapshot_port_);
    snapshot_mcast_socket_.next_rcv_valid_index_ = 0;
}

auto MarketDataConsumer::abortSnapshotSync() noexcept -> void {
    snapshot_have_start_ = false;
    incremental_queued_updates_.clear();
    snapshot_queued_updates_.clear();
    in_recovery_.store(false, std::memory_order_release);
    market_data_synchronized.store(false, std::memory_order_release);
    snapshot_mcast_socket_.leave(snapshot_ip_, snapshot_port_);
    snapshot_mcast_socket_.next_rcv_valid_index_ = 0;
}

} // namespace alphatrader
