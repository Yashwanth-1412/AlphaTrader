#include "market_data.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

namespace alphatrader {

namespace {
// Registration magic for the exchange's feed gateway (see
// QuantLink/Lib/network/udp_fanout_socket.h): sending this datagram to the
// exchange's incremental ip:port adds us to its subscriber registry; the
// exchange then unicasts every frame to our bound socket.
constexpr char    kRegistrationMagic[]    = "NEXSUB";
constexpr size_t  kRegistrationMagicLen   = sizeof(kRegistrationMagic) - 1;
constexpr int64_t kRegistrationIntervalNs = 1000LL * 1000LL * 1000LL;
} // namespace

MarketDataConsumer::MarketDataConsumer(quantlink::SPSCQueue<MarketUpdate>* market_updates, quantlink::Logger* logger, const std::string& iface, const std::string& snapshot_tcp_ip,
                                       int snapshot_tcp_port, const std::string& incremental_ip, int incremental_port, int receive_port)
    : incoming_md_updates_(market_updates),
      logger_(logger),
      iface_(iface),
      snapshot_tcp_ip_(snapshot_tcp_ip),
      snapshot_tcp_port_(snapshot_tcp_port),
      incremental_ip_(incremental_ip),
      incremental_port_(incremental_port),
      receive_port_(receive_port) {

    market_data_synchronized.store(false, std::memory_order_release);

    auto recv_cb = [this](auto* socket) { recvCallback(socket); };

    incremental_mcast_socket_.recv_callback_ = recv_cb;
    // Bind OUR OWN receive port: the exchange's incremental port is now owned
    // by the exchange's feed gateway, which unicasts to us after registration.
    ASSERT(incremental_mcast_socket_.init(incremental_ip_, iface_, receive_port_, true) >= 0,
           "MarketDataConsumer::MarketDataConsumer() failed to create incremental feed socket. error: " + std::string(std::strerror(errno)));
    sendRegistration();
    const in_addr addr{inet_addr(incremental_ip_.c_str())};
    if ((ntohl(addr.s_addr) & 0xF0000000) == 0xE0000000) {
        // Join the group on the configured interface: joining with INADDR_ANY
        // binds to the default-route interface, which misses loopback-delivered
        // copies of the feed (the exchange sends via 'lo' in this setup).
        const std::string iface_ip = quantlink::getIfaceIP(iface_);
        const ip_mreq     mreq{{addr}, {iface_ip.empty() ? htonl(INADDR_ANY) : inet_addr(iface_ip.c_str())}};
        ASSERT(setsockopt(incremental_mcast_socket_.socket_fd_, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) == 0,
               "MarketDataConsumer::MarketDataConsumer() failed to join incremental group: " + std::to_string(incremental_mcast_socket_.socket_fd_) + " error: " + std::string(std::strerror(errno)));
    }
}

auto MarketDataConsumer::sendRegistration() noexcept -> void {
    last_registration_ns_ = quantlink::utils::get_current_epoch_nanos();

    // If the gateway address is our own bound address the datagram loops
    // straight back into our receive buffer, where the magic is not a valid
    // ITCH frame and would corrupt the head of the stream. There is nothing to
    // register with in that setup, so skip it.
    if (incremental_port_ == receive_port_ && isOwnAddress(incremental_ip_))
        return;

    const sockaddr_in gateway{AF_INET, htons(static_cast<uint16_t>(incremental_port_)), {inet_addr(incremental_ip_.c_str())}, {}};
    ::sendto(incremental_mcast_socket_.socket_fd_, kRegistrationMagic, kRegistrationMagicLen, 0, reinterpret_cast<const sockaddr*>(&gateway), sizeof(gateway));
}

auto MarketDataConsumer::isOwnAddress(const std::string& ip) const noexcept -> bool {
    const uint32_t addr = ntohl(inet_addr(ip.c_str()));
    if ((addr & 0xFF000000u) == 0x7F000000u) // 127.0.0.0/8
        return true;
    return ip == quantlink::getIfaceIP(iface_);
}

auto MarketDataConsumer::start(int core_id) -> void {
    startSnapshotSync();
    run_.store(true, std::memory_order_release);
    thread_ = quantlink::utils::create_and_pin_thread(core_id, "MarketDataConsumer", [this]() { run(); });
}

auto MarketDataConsumer::stop() -> void {
    run_.store(false, std::memory_order_release);
    if (thread_.joinable()) {
        thread_.join();
    }
}

auto MarketDataConsumer::run() noexcept -> void {
    sendToLogger("MarketDataConsumer::run() started.\n");

    while (run_.load(std::memory_order_acquire)) {
        // Re-register with the exchange's feed gateway on a timer: the gateway
        // evicts subscribers that go silent, and a lost registration datagram
        // (UDP is unreliable) must not permanently drop us from the stream.
        if (quantlink::utils::get_current_epoch_nanos() - last_registration_ns_ > kRegistrationIntervalNs)
            sendRegistration();
        incremental_mcast_socket_.sendAndRecv();
        if (in_recovery_.load(std::memory_order_relaxed))
            readSnapshotTcp();
    }

    sendToLogger("MarketDataConsumer::run() exited.\n");
}

auto MarketDataConsumer::recvCallback(quantlink::McastSocket* socket) noexcept -> void {
    sendToLogger("MarketDataConsumer::recvCallback() fd=% bytes=%\n", socket->socket_fd_, socket->next_rcv_valid_index_);

    // Incremental feed: [8-byte seq] [ITCH struct]
    if (socket->next_rcv_valid_index_ < sizeof(SeqNum) + 1) {
        return;
    }

    std::size_t i = 0;
    for (; i + sizeof(SeqNum) + 1 <= socket->next_rcv_valid_index_;) {
        const char*       data      = socket->inbound_data_.data() + i;
        const std::size_t remaining = socket->next_rcv_valid_index_ - i;

        // A registration datagram echoed onto the feed is a known,
        // self-delimiting token, so drop it whole rather than byte-resyncing.
        if (remaining >= kRegistrationMagicLen && std::memcmp(data, kRegistrationMagic, kRegistrationMagicLen) == 0) {
            i += kRegistrationMagicLen;
            continue;
        }

        MarketUpdate update;
        if (decoder_.decode(std::span<const char>(data, remaining), update)) {
            processIncremental(update);
            i += sizeof(SeqNum) + decoder_.lastDecodedSize();
        } else if (decoder_.needsMoreData()) {
            break; // truncated frame — wait for the rest of it
        } else {
            // Unparseable at this offset. Skipping a byte and retrying stops a
            // single corrupt datagram from wedging the feed permanently.
            sendToLogger("MarketDataConsumer::recvCallback() resync, skipping byte at offset=%\n", i);
            ++i;
        }
    }

    if (i > 0) {
        std::memmove(socket->inbound_data_.data(), socket->inbound_data_.data() + i, socket->next_rcv_valid_index_ - i);
        socket->next_rcv_valid_index_ -= i;
    }
}

} // namespace alphatrader
