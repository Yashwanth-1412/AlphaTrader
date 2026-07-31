#include "market_data_consumer.h"

namespace alphatrader {

auto MarketDataConsumer::processIncremental(const MarketUpdate& update) noexcept -> void {
    if (in_recovery_.load(std::memory_order_relaxed)) {
        queueIncremental(update);
        return;
    }

    const SeqNum expected = next_exp_inc_seq_num_;

    if (expected == 0) {
        next_exp_inc_seq_num_ = update.seq_num + 1;
        ASSERT(incoming_md_updates_->push(update), "Market-data queue is full");
        return;
    }

    if (update.seq_num == expected) {
        ASSERT(incoming_md_updates_->push(update), "Market-data queue is full");
        ++next_exp_inc_seq_num_;
        return;
    }

    if (update.seq_num > expected) {
        detectGap(update.seq_num);
        queueIncremental(update);
    }
}

auto MarketDataConsumer::detectGap(SeqNum received_seq) noexcept -> void {
    if (!in_recovery_.exchange(true, std::memory_order_acq_rel)) {
        market_data_synchronized.store(false, std::memory_order_release);
        sendToLogger("MarketDataConsumer::detectGap() packet drop detected. expected=% received=%\n",
                     next_exp_inc_seq_num_, received_seq);
        const SeqNum gap = received_seq - next_exp_inc_seq_num_;
        if (gap <= REPLAY_CAPACITY) {
            sendToLogger("MarketDataConsumer::detectGap() requesting replay from seq=% gap=%\n",
                         next_exp_inc_seq_num_ - 1, gap);
            startReplaySync(next_exp_inc_seq_num_ - 1);
        } else {
            sendToLogger("MarketDataConsumer::detectGap() gap=% too large for replay, requesting snapshot\n", gap);
            startSnapshotSync();
        }
    }
}

auto MarketDataConsumer::queueIncremental(const MarketUpdate& update) noexcept -> void {
    incremental_queued_updates_[update.seq_num] = update;
}

} // namespace alphatrader
