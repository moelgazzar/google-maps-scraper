"""Browser-only NordVPN SOCKS5 routing for the Playwright prospecting worker.

Chromium cannot authenticate directly to SOCKS5 proxies.  This module exposes a
loopback-only HTTP CONNECT proxy and authenticates its outbound connections to
NordVPN's SOCKS5 service.  Only browsers explicitly launched with the returned
loopback proxy use the route; the Mac's network settings are not changed.
"""
import asyncio
import getpass
import ipaddress
import json
import os
import socket
import struct
import subprocess
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit


KEYCHAIN_USERNAME_SERVICE = "Sonor Prospecting NordVPN service username"
KEYCHAIN_PASSWORD_SERVICE = "Sonor Prospecting NordVPN service password"
ORACLE_SECRET_FILE = Path.home() / ".config" / "sonor" / "google-maps-scraping" / "nord.json"
DEFAULT_HOSTS = (
    "atlanta.us.socks.nordhold.net",
    "chicago.us.socks.nordhold.net",
    "dallas.us.socks.nordhold.net",
    "los-angeles.us.socks.nordhold.net",
    "new-york.us.socks.nordhold.net",
    "phoenix.us.socks.nordhold.net",
    "san-francisco.us.socks.nordhold.net",
    "us.socks.nordhold.net",
    "amsterdam.nl.socks.nordhold.net",
    "stockholm.se.socks.nordhold.net",
    "nl.socks.nordhold.net",
    "se.socks.nordhold.net",
)


class NordProxyError(RuntimeError):
    """A browser-only NordVPN route could not be established."""


def _keychain_value(service):
    try:
        result = subprocess.run(
            ["security", "find-generic-password", "-a", getpass.getuser(),
             "-s", service, "-w"],
            check=False, capture_output=True, text=True,
        )
    except FileNotFoundError:
        return ""
    return result.stdout.strip() if result.returncode == 0 else ""


def service_credentials():
    """Load Nord manual-setup credentials without putting them in project files."""
    env_username = os.environ.get("NORDVPN_SERVICE_USERNAME", "")
    env_password = os.environ.get("NORDVPN_SERVICE_PASSWORD", "")
    if bool(env_username) != bool(env_password):
        raise NordProxyError(
            "Set both NORDVPN_SERVICE_USERNAME and NORDVPN_SERVICE_PASSWORD, or neither."
        )
    username = env_username or _keychain_value(KEYCHAIN_USERNAME_SERVICE)
    password = env_password or _keychain_value(KEYCHAIN_PASSWORD_SERVICE)
    if not username and not password and ORACLE_SECRET_FILE.exists():
        try:
            if ORACLE_SECRET_FILE.stat().st_mode & 0o077:
                raise NordProxyError("Oracle NordVPN credential file must have mode 0600.")
            stored = json.loads(ORACLE_SECRET_FILE.read_text())
            username = str(stored["username"]).strip()
            password = str(stored["password"]).strip()
        except NordProxyError:
            raise
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as error:
            raise NordProxyError("Oracle NordVPN credential file is invalid.") from error
    if not username or not password:
        raise NordProxyError(
            "NordVPN service credentials are missing. Double-click "
            "'Configure NordVPN Browser Proxy.command' and enter the manual-setup "
            "service username and password from your Nord Account."
        )
    return username, password


def proxy_hosts(value=None):
    hosts = tuple(part.strip().lower() for part in (value or "").split(",") if part.strip())
    hosts = hosts or DEFAULT_HOSTS
    for host in hosts:
        try:
            socket.getaddrinfo(host, 1080, type=socket.SOCK_STREAM)
        except socket.gaierror as error:
            raise NordProxyError(f"NordVPN proxy host does not resolve: {host}") from error
    return hosts


def route_state(path):
    try:
        data = json.loads(Path(path).read_text())
        ipaddress.ip_address(data["public_ip"])
        return {"host": str(data["host"]), "public_ip": data["public_ip"]}
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        return {"host": "", "public_ip": ""}


def ordered_hosts(hosts, last_host=""):
    hosts = tuple(hosts)
    if last_host not in hosts:
        return hosts
    start = hosts.index(last_host) + 1
    return hosts[start:] + hosts[:start]


def save_route_state(path, host, public_ip):
    ipaddress.ip_address(public_ip)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps({"host": host, "public_ip": public_ip}, indent=2) + "\n")
    temporary.replace(path)


async def _read_headers(reader, maximum=65536):
    data = bytearray()
    while b"\r\n\r\n" not in data:
        chunk = await reader.read(4096)
        if not chunk:
            raise NordProxyError("Browser closed the proxy request before sending headers.")
        data.extend(chunk)
        if len(data) > maximum:
            raise NordProxyError("Browser proxy request headers were too large.")
    head, body = bytes(data).split(b"\r\n\r\n", 1)
    return head, body


async def _socks_connect(upstream_host, upstream_port, username, password, target_host, target_port):
    reader, writer = await asyncio.open_connection(upstream_host, upstream_port)
    try:
        writer.write(b"\x05\x01\x02")
        await writer.drain()
        if await reader.readexactly(2) != b"\x05\x02":
            raise NordProxyError(f"NordVPN proxy {upstream_host} rejected password authentication.")
        user = username.encode()
        secret = password.encode()
        if not user or not secret or len(user) > 255 or len(secret) > 255:
            raise NordProxyError("NordVPN service credentials must be between 1 and 255 bytes.")
        writer.write(b"\x01" + bytes((len(user),)) + user + bytes((len(secret),)) + secret)
        await writer.drain()
        if await reader.readexactly(2) != b"\x01\x00":
            raise NordProxyError("NordVPN rejected the manual-setup service credentials.")
        encoded_host = target_host.encode("idna")
        if len(encoded_host) > 255:
            raise NordProxyError("Proxy destination hostname is too long.")
        writer.write(b"\x05\x01\x00\x03" + bytes((len(encoded_host),)) + encoded_host
                     + struct.pack("!H", target_port))
        await writer.drain()
        version, reply, _reserved, address_type = await reader.readexactly(4)
        if version != 5 or reply != 0:
            raise NordProxyError(
                f"NordVPN proxy {upstream_host} could not reach {target_host}:{target_port} "
                f"(SOCKS reply {reply})."
            )
        if address_type == 1:
            await reader.readexactly(4)
        elif address_type == 3:
            await reader.readexactly((await reader.readexactly(1))[0])
        elif address_type == 4:
            await reader.readexactly(16)
        else:
            raise NordProxyError("NordVPN proxy returned an unknown address type.")
        await reader.readexactly(2)
        return reader, writer
    except BaseException:
        writer.close()
        with suppress(Exception):
            await writer.wait_closed()
        raise


async def _relay(reader, writer):
    try:
        while chunk := await reader.read(65536):
            writer.write(chunk)
            await writer.drain()
    except (ConnectionError, asyncio.CancelledError):
        pass
    finally:
        with suppress(Exception):
            writer.write_eof()


@dataclass
class NordProxyBridge:
    upstream_host: str
    username: str
    password: str
    upstream_port: int = 1080
    server: asyncio.AbstractServer | None = None
    tasks: set = field(default_factory=set, init=False)
    clients: set = field(default_factory=set, init=False)

    @property
    def browser_server(self):
        if not self.server or not self.server.sockets:
            raise NordProxyError("The local NordVPN bridge has not started.")
        return f"http://127.0.0.1:{self.server.sockets[0].getsockname()[1]}"

    async def start(self):
        self.server = await asyncio.start_server(self._accept, "127.0.0.1", 0)
        return self

    async def close(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.server = None

        clients = tuple(self.clients)
        for writer in clients:
            writer.close()
        if clients:
            await asyncio.gather(*(writer.wait_closed() for writer in clients), return_exceptions=True)
        self.clients.difference_update(clients)

        tasks = tuple(self.tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self.tasks.difference_update(tasks)

    def _accept(self, reader, writer):
        self.clients.add(writer)
        task = asyncio.create_task(self._handle(reader, writer))
        self.tasks.add(task)

        def finished(done):
            self.tasks.discard(done)
            with suppress(BaseException):
                done.result()

        task.add_done_callback(finished)

    async def _handle(self, client_reader, client_writer):
        upstream_writer = None
        response_started = False
        try:
            head, initial_body = await _read_headers(client_reader)
            lines = head.split(b"\r\n")
            method, target, version = lines[0].decode("latin-1").split(" ", 2)
            if method.upper() == "CONNECT":
                host, separator, port_text = target.rpartition(":")
                if not separator or not host:
                    raise NordProxyError(f"Invalid CONNECT destination: {target}")
                port = int(port_text)
                upstream_reader, upstream_writer = await _socks_connect(
                    self.upstream_host, self.upstream_port, self.username, self.password, host, port
                )
                client_writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                response_started = True
                if initial_body:
                    upstream_writer.write(initial_body)
                    await upstream_writer.drain()
            else:
                parsed = urlsplit(target)
                if parsed.scheme != "http" or not parsed.hostname:
                    raise NordProxyError(f"Unsupported browser proxy target: {target}")
                port = parsed.port or 80
                upstream_reader, upstream_writer = await _socks_connect(
                    self.upstream_host, self.upstream_port, self.username, self.password,
                    parsed.hostname, port,
                )
                path = parsed.path or "/"
                if parsed.query:
                    path += "?" + parsed.query
                filtered = [line for line in lines[1:]
                            if not line.lower().startswith(b"proxy-connection:")]
                request = f"{method} {path} {version}\r\n".encode("latin-1")
                upstream_writer.write(request + b"\r\n".join(filtered) + b"\r\n\r\n" + initial_body)
                await upstream_writer.drain()
                response_started = True
            await asyncio.gather(
                _relay(client_reader, upstream_writer),
                _relay(upstream_reader, client_writer),
            )
        except (ValueError, OSError, asyncio.IncompleteReadError, NordProxyError) as error:
            if not response_started:
                detail = str(error).replace("\r", " ").replace("\n", " ")[:300]
                client_writer.write(
                    f"HTTP/1.1 502 Bad Gateway\r\nContent-Type: text/plain\r\n"
                    f"Connection: close\r\nContent-Length: {len(detail.encode())}\r\n\r\n{detail}".encode()
                )
                with suppress(Exception):
                    await client_writer.drain()
        finally:
            if upstream_writer:
                upstream_writer.close()
                with suppress(Exception):
                    await upstream_writer.wait_closed()
            client_writer.close()
            with suppress(Exception):
                await client_writer.wait_closed()
            self.clients.discard(client_writer)


async def verify_browser_proxy(context, host, previous_ip=""):
    page = await context.new_page()
    try:
        response = await page.goto(
            "https://api.ipify.org?format=json", wait_until="domcontentloaded", timeout=20000
        )
        if not response or not response.ok:
            raise NordProxyError(f"NordVPN route {host} failed its public-IP check.")
        public_ip = (await response.json()).get("ip", "")
        ipaddress.ip_address(public_ip)
        if previous_ip and public_ip == previous_ip:
            raise NordProxyError(f"NordVPN route {host} reused the previous public IP {public_ip}.")
        return public_ip
    except NordProxyError:
        raise
    except Exception as error:
        raise NordProxyError(f"NordVPN route {host} failed its public-IP check: {error}") from error
    finally:
        await page.close()


async def launch_nord_browser(playwright, host, username, password, previous_ip=""):
    """Launch and verify Chrome without changing system network settings."""
    bridge = await NordProxyBridge(host, username, password).start()
    browser = context = None
    try:
        browser = await playwright.chromium.launch(
            channel="chrome", headless=False, proxy={"server": bridge.browser_server}
        )
        context = await browser.new_context(locale="en-US", viewport={"width": 1280, "height": 800})
        public_ip = await verify_browser_proxy(context, host, previous_ip)
        await context.close()
        return browser, bridge, public_ip
    except BaseException:
        if context:
            await context.close()
        if browser:
            await browser.close()
        await bridge.close()
        raise
