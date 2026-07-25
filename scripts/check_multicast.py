#!/usr/bin/env python3
"""Verify local IPv4 multicast send/receive on one interface."""

import argparse
import socket
import struct


def interface_address(interface: str) -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        probe.connect(("8.8.8.8", 80))
        return probe.getsockname()[0]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--interface-ip", default=None)
    parser.add_argument("--group", default="239.255.0.1")
    parser.add_argument("--port", type=int, default=21001)
    args = parser.parse_args()

    interface_ip = args.interface_ip or interface_address("")
    marker = b"alphatrader-multicast-probe"

    receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    receiver.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    receiver.bind(("", args.port))
    membership = struct.pack("=4s4s", socket.inet_aton(args.group), socket.inet_aton(interface_ip))
    receiver.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)
    receiver.settimeout(2)

    sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sender.bind((interface_ip, 0))
    sender.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(interface_ip))
    sender.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, struct.pack("B", 1))
    sender.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, struct.pack("B", 1))
    loopback = struct.unpack("B", sender.getsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1))[0]
    outgoing_ip = socket.inet_ntoa(sender.getsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, 4))
    print(f"sending {args.group}:{args.port} via {outgoing_ip}; multicast loopback={loopback}")
    sender.sendto(marker, (args.group, args.port))

    try:
        data, source = receiver.recvfrom(1024)
    except TimeoutError:
        print(f"FAIL: no multicast packet on {interface_ip} for {args.group}:{args.port}")
        return 1
    finally:
        sender.close()
        receiver.close()

    if data != marker:
        print(f"FAIL: unexpected payload from {source}: {data!r}")
        return 1

    print(f"PASS: received multicast on {interface_ip} from {source[0]}:{source[1]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
