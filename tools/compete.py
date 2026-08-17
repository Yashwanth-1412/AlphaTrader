#!/usr/bin/env python3
"""compete.py — race AlphaTrader (C++) against Python traders on NanoExchange.

Roles:
  seed   — resting MM: posts bid + ask, re-seeds the ask each round once consumed
  rival  — competing taker: 'speed' (blind IOC spam on a timer) or
           'signal' (joins the multicast ITCH feed, maintains a mini book, fires
           IOC on imbalance >= threshold — mirrors LiquidityTaker's trigger)
  demo   — orchestrates exchange + alpha_trader + seeder + N rivals, prints a
           per-participant fill scoreboard, cross-checks the exchange log.
           --pin spreads every participant across its own CPU core.

Protocol facts (packed, big-endian):
  OUCH enter  : 'O' + token(14) + side + shares(4) + ticker(8) + price(4) + tif(4)
                + firm(4) + 5 pad bytes            [tif=0 -> IOC/FillAndKill]
  OUCH resp   : 'A' 61B / 'E' 39B (executed_shares @23) / 'C' 28B / 'U' 61B / 'J' 23B
  ITCH frame  : seq(8) + payload; 'A'=36 'F'=40 'E'=31 'X'=23 'D'=19 'U'=34 'S'=12
  imbalance   : (bid_qty - ask_qty) * 1_000_000 / (bid_qty + ask_qty) @ top level

Feed distribution (feed-gateway pattern, no kernel multicast):
  the exchange's incremental socket is a per-subscriber unicast fan-out
  gateway: clients register by sending a 6-byte "NEXSUB" datagram to the
  gateway's ip:port, then receive every frame unicast on their own port.
  Clients re-register every 1s (gateway evicts silent subscribers after 5s).
"""

import argparse
import os
import re
import signal
import socket
import struct
import subprocess
import sys
import threading
import time

ASK_PRICE = 20000  # scaled by 10000 -> $2.00
BID_PRICE = 10000  # -> $1.00
THRESHOLD = 300000  # Ratio_SCALE = 1_000_000

RESP_SIZES = {ord("A"): 61, ord("U"): 61, ord("E"): 39, ord("C"): 28, ord("J"): 23}
# NOTE: 'A'/'U' are 61 bytes on the wire — the NanoExchange's OrderAccepted carries
# firm[4], display, ref(8), capacity, sweep, cross_type, customer_type after tif —
# NOT the minimal 52-byte layout. Verified against a live hexdump.
ITCH_SIZES = {ord("S"): 12, ord("A"): 36, ord("F"): 40, ord("E"): 31,
              ord("X"): 23, ord("D"): 19, ord("U"): 34}

VERBOSE = False


def log(msg):
    if VERBOSE:
        print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def pin_core(core):
    """Pin the current process to a single CPU (Python-affinity, no-op for -1)."""
    if core is not None and core >= 0:
        os.sched_setaffinity(0, {core})


# ── OUCH client ──────────────────────────────────────────────────
def ouch_enter(token, side, shares, ticker, price, tif=0, firm=b"TEST"):
    msg = bytearray(b"O")
    msg += token.encode()[:14].ljust(14, b"\0") + side.encode()
    msg += struct.pack(">I", shares) + struct.pack(">Q", ticker)
    msg += struct.pack(">I", price) + struct.pack(">I", tif)
    msg += firm[:4].ljust(4, b"\0") + b"\0" * 5
    return bytes(msg)


class OuchClient:
    def __init__(self, gateway_port, name):
        self.sock = socket.create_connection(("127.0.0.1", gateway_port), timeout=5)
        self.sock.settimeout(2.0)
        self.executed = {}
        self.total = 0
        self.orders_sent = 0
        self.stop = threading.Event()
        self.name = name

    def enter(self, token, side, shares, ticker, price, tif=0):
        self.sock.sendall(ouch_enter(token, side, shares, ticker, price, tif))
        self.orders_sent += 1

    def start_reader(self):
        threading.Thread(target=self._read_loop, daemon=True).start()

    def _recv_exact(self, n):
        """Read exactly n bytes off the stream.

        TCP is a byte stream: a single recv() may return fewer bytes than asked
        for whenever a message straddles segment boundaries. Looping here is what
        keeps a partially-delivered response from desyncing (or killing) the
        reader. Returns None if the peer closed or we are shutting down.
        """
        buf = bytearray()
        while len(buf) < n:
            try:
                chunk = self.sock.recv(n - len(buf))
            except socket.timeout:
                if self.stop.is_set():
                    return None
                continue
            except OSError:
                return None
            if not chunk:
                return None
            buf += chunk
        return bytes(buf)

    def _read_loop(self):
        while not self.stop.is_set():
            hdr = self._recv_exact(1)
            if hdr is None:
                break
            n = RESP_SIZES.get(hdr[0])
            if n is None:
                log(f"{self.name}: unknown response type {hdr!r}, stream desync?")
                continue
            msg = self._recv_exact(n - 1)
            if msg is None:
                break
            if hdr[0] == ord("E"):
                # Body layout after the type byte: timestamp[8] token[14] shares[4] ...
                token = msg[8:22].decode(errors="replace").rstrip("\0 ")
                shares = struct.unpack_from(">I", msg, 22)[0]
                self.executed[token] = self.executed.get(token, 0) + shares
                self.total += shares
                log(f"{self.name}: fill +{shares} (token={token}) total={self.total}")
            elif hdr[0] == ord("C"):
                log(f"{self.name}: order canceled")

    def close(self):
        self.stop.set()
        self.sock.close()


# ── ITCH feed (incremental) ──────────────────────────────────────
class ItchFeed:
    """Live incremental feed from the exchange's feed gateway: send a NEXSUB
    registration datagram to the gateway, which then unicasts every frame to
    OUR bound port (per-subscriber fan-out — multicast membership emulated).
    Re-register every 1s; the gateway evicts silent subscribers after 5s."""

    def __init__(self, feed_ip, feed_port, bind_port, tickers):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", bind_port))
        self.sock.settimeout(0.1)
        self.feed_addr = (feed_ip, feed_port)
        self.last_reg = 0.0
        self.tickers = set(tickers)
        self.orders = {}
        self.buf = b""

    def _register(self):
        self.sock.sendto(b"NEXSUB", self.feed_addr)
        self.last_reg = time.monotonic()

    def poll(self, max_frames=64):
        if time.monotonic() - self.last_reg >= 1.0:
            self._register()
        try:
            data, _ = self.sock.recvfrom(65536)
        except socket.timeout:
            return []
        self.buf += data
        frames = []
        while len(self.buf) >= 9:
            t = self.buf[8]
            n = ITCH_SIZES.get(t)
            if n is None or len(self.buf) < 8 + n:
                break
            seq = struct.unpack_from(">Q", self.buf, 0)[0]
            frames.append((seq, self.buf[8:8 + n]))
            self.buf = self.buf[8 + n:]
        for _, payload in frames:
            self.apply(payload)
        return frames

    def apply(self, p):
        t = p[0]
        sl = struct.unpack_from(">H", p, 1)[0]
        if t == ord("A") or t == ord("F"):
            ref = struct.unpack_from(">Q", p, 11)[0]
            side = "B" if p[19] == ord("B") else "S"
            qty = struct.unpack_from(">I", p, 20)[0]
            price = struct.unpack_from(">I", p, 32)[0]
            self.orders[(sl, ref)] = (side, price, qty)
        elif t == ord("E") or t == ord("X"):
            ref = struct.unpack_from(">Q", p, 11)[0]
            qty = struct.unpack_from(">I", p, 19)[0]
            k = (sl, ref)
            if k in self.orders:
                side, price, rest = self.orders[k]
                rest -= qty
                if rest <= 0:
                    del self.orders[k]
                else:
                    self.orders[k] = (side, price, rest)
        elif t == ord("D"):
            ref = struct.unpack_from(">Q", p, 11)[0]
            self.orders.pop((sl, ref), None)
        elif t == ord("U"):
            old = struct.unpack_from(">Q", p, 11)[0]
            new = struct.unpack_from(">Q", p, 19)[0]
            qty = struct.unpack_from(">I", p, 27)[0]
            price = struct.unpack_from(">I", p, 31)[0]
            k = (sl, old)
            if k in self.orders:
                side, _, _ = self.orders[k]
                del self.orders[k]
                self.orders[(sl, new)] = (side, price, qty)

    def bbo(self, ticker):
        bids = [(p, q) for (t, _), (s, p, q) in self.orders.items()
                if t == ticker and s == "B"]
        asks = [(p, q) for (t, _), (s, p, q) in self.orders.items()
                if t == ticker and s == "S"]
        bid_p = max((p for p, _ in bids), default=0)
        ask_p = min((p for p, _ in asks), default=0)
        bid_q = sum(q for p, q in bids if p == bid_p) if bid_p else 0
        ask_q = sum(q for p, q in asks if p == ask_p) if ask_p else 0
        return bid_p, bid_q, ask_p, ask_q


# ── seeder (resting MM) ──────────────────────────────────────────
def seed_main(args):
    pin_core(args.core)
    cli = OuchClient(args.gateway_port, "seeder")
    cli.start_reader()
    cli.enter("MM_BID_001", "B", args.bid_qty, args.ticker, BID_PRICE, tif=99998)
    log(f"seeder: bid {args.bid_qty}@{BID_PRICE} posted")
    sold = 0
    for r in range(args.rounds):
        token = f"MM_ASK_{r:03d}"
        bid_token = f"MM_BID_{r:03d}"
        if r > 0:
            cli.enter(bid_token, "B", args.bid_qty, args.ticker, BID_PRICE, tif=99998)
            log(f"seeder: bid {args.bid_qty}@{BID_PRICE} re-posted ({bid_token})")
        cli.enter(token, "S", args.ask_qty, args.ticker, ASK_PRICE, tif=99998)
        log(f"seeder: round {r + 1}/{args.rounds} ask {args.ask_qty}@{ASK_PRICE} posted")
        deadline = time.monotonic() + args.round_timeout
        while cli.executed.get(token, 0) < args.ask_qty:
            if args.stop.is_set():
                break
            if time.monotonic() > deadline:
                log(f"seeder: round {r + 1} TIMEOUT after {args.round_timeout}s "
                    f"(filled {cli.executed.get(token, 0)}/{args.ask_qty})")
                break
            time.sleep(0.005)
        round_sold = cli.executed.get(token, 0)
        sold += round_sold
        log(f"seeder: round {r + 1} done — sold {round_sold}/{args.ask_qty} "
            f"(cumulative {sold})")
        if args.stop.is_set():
            break
    time.sleep(0.2)
    print(f"RESULT seeder sold={sold}")
    cli.close()


# ── rival (competing taker) ──────────────────────────────────────
def rival_main(args):
    pin_core(args.core)
    cli = OuchClient(args.gateway_port, args.name)
    cli.start_reader()
    seq = 0

    def fire():
        nonlocal seq
        seq += 1
        cli.enter(f"PY{seq:07d}", "B", args.qty, args.ticker, ASK_PRICE, tif=0)
        log(f"{args.name}: fired IOC #{seq} qty={args.qty}@{ASK_PRICE}")

    if args.strategy == "speed":
        interval = args.interval / 1000.0
        next_fire = time.monotonic()
        log(f"{args.name}: speed bot, interval={args.interval}ms")
        while not args.stop.is_set():
            now = time.monotonic()
            if now >= next_fire:
                fire()
                next_fire = now + interval
            time.sleep(0.001)
    else:  # signal — mirror LiquidityTaker: fire when |imbalance| >= threshold
        feed = ItchFeed(args.feed_ip, args.feed_port,
                        args.bind_port if args.bind_port else args.feed_port + 1,
                        [args.ticker])
        log(f"{args.name}: signal bot registered on feed gateway {args.feed_ip}:"
            f"{args.feed_port} (bind :{args.bind_port if args.bind_port else args.feed_port + 1}), "
            f"threshold={args.threshold}")
        while not args.stop.is_set():
            if feed.poll():
                bid_p, bid_q, ask_p, ask_q = feed.bbo(args.ticker)
                total = bid_q + ask_q
                if bid_p and ask_p and total > 0:
                    imbalance = (bid_q - ask_q) * 1_000_000 // total
                    log(f"{args.name}: bid={bid_q}@{bid_p} ask={ask_q}@{ask_p} "
                        f"imb={imbalance}")
                    if imbalance >= args.threshold:
                        fire()
            time.sleep(0.0005)
    time.sleep(0.2)
    print(f"RESULT {args.name} filled={cli.total} sent={cli.orders_sent}")
    cli.close()


# ── feed relay ───────────────────────────────────────────────────
def relay_main(args):
    """Owns the exchange's UDP incremental port and fans every datagram out to
    subscriber ports relay_base..relay_base+subscribers-1, so the C++ consumer
    and every Python signal rival get their own copy (loopback unicast)."""
    pin_core(args.core)
    in_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    in_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    in_sock.bind(("", args.feed_port))
    in_sock.settimeout(0.1)
    outs = [socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            for _ in range(args.subscribers)]
    log(f"relay: listening {args.feed_port}, fanning out to "
        f"{args.relay_base}..{args.relay_base + args.subscribers - 1}")
    while not args.stop.is_set():
        try:
            data, _ = in_sock.recvfrom(65536)
        except socket.timeout:
            continue
        for i, out in enumerate(outs):
            out.sendto(data, ("127.0.0.1", args.relay_base + i))
    for out in outs:
        out.close()
    in_sock.close()


# ── demo orchestration ───────────────────────────────────────────
def wait_for_port(port, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=1)
            s.close()
            return True
        except OSError:
            time.sleep(0.1)
    return False


def demo_main(args):
    global VERBOSE
    VERBOSE = args.verbose
    root = os.path.dirname(os.path.abspath(__file__))
    ne_bin = os.environ.get("NANOEXCHANGE_BIN",
                            os.path.join(root, "..", "..", "NanoExchange", "build", "NanoExchange"))
    at_bin = os.environ.get("ALPHA_TRADER_BIN", os.path.join(root, "..", "build", "alpha_trader"))
    devnull = open(os.devnull, "w")

    verb = ["--verbose"] if args.verbose else []
    ne = None
    if not args.no_exchange:
        print("▶ starting NanoExchange...")
        ne = subprocess.Popen(
            [ne_bin, "lo", str(args.gateway_port), args.feed_ip, str(args.feed_port),
             str(args.snapshot_port), "lo"] + [str(t) for t in args.tickers],
            stdout=devnull, stderr=devnull)
    try:
        if not wait_for_port(args.gateway_port):
            print("✗ TIMEOUT waiting for NanoExchange")
            return 1
        print(f"✓ NanoExchange up on :{args.gateway_port} "
              f"(feed gateway {args.feed_ip}:{args.feed_port}, snap :{args.snapshot_port})")

        print(f"▶ starting alpha_trader (ioc qty {args.qty}, imbalance {args.threshold})...")
        at_log = open("/tmp/compete_alpha.log", "w")
        at_cmd = [at_bin, "--ioc-qty", str(args.qty), "--imbalance", str(args.threshold),
                  "--position-limit", "1000000", "--gateway-port", str(args.gateway_port),
                  "--incremental-ip", args.feed_ip,
                  "--incremental-port", str(args.feed_port),
                  "--receive-port", str(args.feed_base),
                  "--snapshot-ip", "127.0.0.1", "--snapshot-port", str(args.snapshot_port)]
        if args.pin:
            base = args.core_offset + args.rivals + 1
            at_cmd += ["--core-consumer", str(base),
                       "--core-engine", str(base + 1),
                       "--core-gateway", str(base + 2)]
        at = subprocess.Popen(at_cmd, stdout=at_log, stderr=subprocess.STDOUT)

        rivals = []
        for i in range(args.rivals):
            name = f"{args.strategy}_{i}"
            cmd = [sys.executable, os.path.abspath(__file__), "rival",
                   "--gateway-port", str(args.gateway_port),
                   "--name", name, "--qty", str(args.qty),
                   "--ticker", str(args.ticker), "--strategy", args.strategy,
                   "--threshold", str(args.threshold),
                   "--feed-ip", args.feed_ip,
                   "--feed-port", str(args.feed_port),
                   "--bind-port", str(args.feed_base + 1 + i)] + verb
            if args.pin:
                cmd += ["--core", str(args.core_offset + 1 + i)]
            if args.strategy == "speed":
                cmd += ["--interval", str(args.interval)]
            log = open(f"/tmp/compete_{name}.log", "w")
            rivals.append((name, subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)))
        print(f"✓ {args.rivals} rivals started ({args.strategy} strategy), "
              f"alpha_trader started (C++ engine, sync-gated)")
        time.sleep(2.0)
        print("✓ participants up — subscribing to fan-out gateway")

        print(f"▶ seeding ticker {args.ticker} (bid {args.bid_qty}@{BID_PRICE}, "
              f"ask {args.ask_qty}@{ASK_PRICE}, {args.rounds} rounds) — race starts now...")
        seed_cmd = [sys.executable, os.path.abspath(__file__), "seed",
                    "--gateway-port", str(args.gateway_port),
                    "--ticker", str(args.ticker), "--rounds", str(args.rounds),
                    "--bid-qty", str(args.bid_qty), "--ask-qty", str(args.ask_qty)] + verb
        if args.pin:
            seed_cmd += ["--core", str(args.core_offset)]
        seed = subprocess.Popen(seed_cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True)
        seed_lines = []
        threading.Thread(target=lambda: [seed_lines.append(l.rstrip()) or
                                         print(f"  [seed] {l.rstrip()}", flush=True)
                                         for l in seed.stdout], daemon=True).start()
        print("✓ book seeded, round 1 live")

        if args.pin:
            print(f"✓ pinned: seeder->{args.core_offset}  rivals->"
                  f"{args.core_offset + 1}..{args.core_offset + args.rivals}  "
                  f"alpha_trader(c,e,g)->{args.core_offset + args.rivals + 1}.."
                  f"{args.core_offset + args.rivals + 3}")
        print("🏁 racing...")
        t0 = time.monotonic()
        heartbeat = 0
        while seed.poll() is None:
            time.sleep(0.2)
            heartbeat += 1
            if heartbeat % 25 == 0:
                print(f"  ...elapsed {time.monotonic() - t0:.0f}s "
                      f"(waiting for rounds to complete)", flush=True)
        seed.wait()
        print(f"✓ racing complete ({time.monotonic() - t0:.1f}s)")

        for _, p in rivals:
            p.send_signal(signal.SIGINT)
        at.send_signal(signal.SIGINT)
        for _, p in rivals:
            p.wait()
        at.wait()
        at_log.close()
        if ne is not None:
            ne.send_signal(signal.SIGINT)
            ne.wait()
    except BaseException:
        if ne is not None:
            ne.send_signal(signal.SIGINT)
        raise

    def result_of(path, label):
        if path and os.path.exists(path):
            for line in open(path):
                m = re.search(r"RESULT \S+ filled=(\d+)(?: sent=(\d+))?", line)
                if m:
                    return int(m.group(1)), int(m.group(2) or -1)
            print(f"  {label}: no RESULT line in {path}")
        else:
            print(f"  {label}: missing log {path}")
        return -1, -1

    seed_sold = -1
    for line in seed_lines:
        m = re.search(r"RESULT \S+ sold=(\d+)", line)
        if m:
            seed_sold = int(m.group(1))

    print("\n═══ SCOREBOARD (filled shares) ═══")
    total = 0
    for name, p in rivals:
        filled, sent = result_of(f"/tmp/compete_{name}.log", name)
        if filled >= 0:
            print(f"  {name:<24} filled={filled} sent={sent}")
            total += filled

    at_summary = open("/tmp/compete_alpha.log").read()
    m = re.search(r"orders sent: (\d+).*?fills: (\d+)  filled qty: (\d+)", at_summary, re.S)
    if m:
        at_sent, at_fills, at_qty = int(m.group(1)), int(m.group(2)), int(m.group(3))
        print(f"  alpha_trader (C++)        sent={at_sent} fills={at_fills} qty={at_qty}")
        total += at_qty
    print("  ─────────────────────────────────────────")
    print(f"  taker total bought        {total}")
    print(f"  seeder total sold         {seed_sold}")
    print(f"  balanced: {'YES' if total == seed_sold else 'NO — MISMATCH!'}")

    return 0


# ── entry point ──────────────────────────────────────────────────
def build_parser():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)

    def add_common(sub):
        sub.add_argument("--gateway-port", type=int, default=11000)
        sub.add_argument("--feed-ip", type=str, default="127.0.0.1",
                         help="exchange feed gateway address (NEXSUB registration target)")
        sub.add_argument("--feed-port", type=int, default=21001,
                         help="exchange feed gateway port")
        sub.add_argument("--snapshot-port", type=int, default=21003)
        sub.add_argument("--ticker", type=int, default=1)
        sub.add_argument("--qty", type=int, default=5)
        sub.add_argument("--threshold", type=int, default=THRESHOLD)
        sub.add_argument("--core", type=int, default=-1,
                         help="pin process to one CPU core (default: scheduler decides)")
        sub.add_argument("--verbose", action="store_true",
                         help="print live debug lines (fills, fires, rounds)")

    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("seed")
    add_common(s)
    s.add_argument("--rounds", type=int, default=5)
    s.add_argument("--round-timeout", type=float, default=15.0,
                   help="abort a round if not consumed in this many seconds")
    s.add_argument("--bid-qty", type=int, default=200)
    s.add_argument("--ask-qty", type=int, default=100)
    s.set_defaults(func=seed_main)

    r = sub.add_parser("rival")
    add_common(r)
    r.add_argument("--name", type=str, default="rival")
    r.add_argument("--strategy", choices=["speed", "signal"], default="signal")
    r.add_argument("--interval", type=int, default=10, help="speed mode: ms between IOC bursts")
    r.add_argument("--bind-port", type=int, default=None,
                   help="signal mode: local UDP port to receive the feed (default: feed-port + 1)")
    r.set_defaults(func=rival_main)

    rel = sub.add_parser("relay")
    add_common(rel)
    rel.add_argument("--subscribers", type=int, default=1)
    rel.add_argument("--relay-base", type=int, default=21100)
    rel.set_defaults(func=relay_main)

    d = sub.add_parser("demo")
    add_common(d)
    d.add_argument("--rounds", type=int, default=5)
    d.add_argument("--bid-qty", type=int, default=200)
    d.add_argument("--ask-qty", type=int, default=100)
    d.add_argument("--rivals", type=int, default=3)
    d.add_argument("--strategy", choices=["speed", "signal"], default="signal")
    d.add_argument("--interval", type=int, default=10)
    d.add_argument("--tickers", nargs="*", type=int, default=[1, 2, 3])
    d.add_argument("--feed-base", type=int, default=21100,
                   help="first UDP receive port for the incremental feed: alpha_trader=base, "
                        "rival i=base+1+i")
    d.add_argument("--pin", action="store_true",
                   help="pin every participant to its own core: seeder=offset, rivals=offset+1.., "
                        "alpha_trader threads=offset+rivals+1..+3")
    d.add_argument("--core-offset", type=int, default=0)
    d.add_argument("--no-exchange", action="store_true",
                   help="skip starting NanoExchange (use an already-running one)")
    d.set_defaults(func=demo_main)
    return p


def main():
    args = build_parser().parse_args()
    global VERBOSE
    VERBOSE = getattr(args, "verbose", False)
    args.stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: args.stop.set())
    raise SystemExit(args.func(args))


if __name__ == "__main__":
    main()
