#include "market_data_consumer.h"

namespace alphatrader {

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
    snapshot_tcp_buffer_.clear();
    snapshot_tcp_fd_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    ASSERT(snapshot_tcp_fd_ >= 0, "MarketDataConsumer::startSnapshotSync() failed to create TCP socket");
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(snapshot_tcp_port_);
    ASSERT(inet_pton(AF_INET, snapshot_tcp_ip_.c_str(), &addr.sin_addr) == 1,
           "MarketDataConsumer::startSnapshotSync() invalid TCP snapshot address");
    const int result = connect(snapshot_tcp_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    ASSERT(result == 0 || errno == EINPROGRESS, "MarketDataConsumer::startSnapshotSync() TCP connect failed");
    const char request = 'S';
    ASSERT(send(snapshot_tcp_fd_, &request, sizeof(request), MSG_NOSIGNAL) == 1,
           "MarketDataConsumer::startSnapshotSync() TCP request failed");
}

auto MarketDataConsumer::readSnapshotTcp() noexcept -> void {
    char buffer[4096];
    const ssize_t received = recv(snapshot_tcp_fd_, buffer, sizeof(buffer), MSG_DONTWAIT);
    if (received <= 0) return;
    snapshot_tcp_buffer_.insert(snapshot_tcp_buffer_.end(), buffer, buffer + received);

    size_t offset = 0;
    while (offset < snapshot_tcp_buffer_.size()) {
        MarketUpdate update;
        const auto wire = std::span<const char>(snapshot_tcp_buffer_.data() + offset,
                                                snapshot_tcp_buffer_.size() - offset);
        if (!decoder_.decodeSnapshot(wire, update)) break;
        offset += decoder_.lastDecodedSize();
        processSnapshot(update);
    }
    if (offset > 0) {
        snapshot_tcp_buffer_.erase(snapshot_tcp_buffer_.begin(), snapshot_tcp_buffer_.begin() + offset);
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
    close(snapshot_tcp_fd_);
    snapshot_tcp_fd_ = -1;
}

auto MarketDataConsumer::abortSnapshotSync() noexcept -> void {
    snapshot_have_start_ = false;
    incremental_queued_updates_.clear();
    snapshot_queued_updates_.clear();
    in_recovery_.store(false, std::memory_order_release);
    market_data_synchronized.store(false, std::memory_order_release);
    if (snapshot_tcp_fd_ != -1) close(snapshot_tcp_fd_);
    snapshot_tcp_fd_ = -1;
}

} // namespace alphatrader
