import asyncio
import ipaddress
import socket
from concurrent.futures import ThreadPoolExecutor
from typing import (
    Optional,
    Self,
    Tuple,
    TypedDict,
)

import socks

from .base import Connector


class Proxy(TypedDict):
    scheme: str
    hostname: str
    port: int
    username: Optional[str]
    password: Optional[str]


class SocksProxyConnector(Connector):
    @classmethod
    async def new(cls, destination: Tuple[str, int], proxy: Proxy, timeout: int) -> Self:
        scheme = proxy.get("scheme")
        if scheme is None:
            raise ValueError("No scheme specified")

        proxy_type = socks.PROXY_TYPES.get(scheme.upper())

        if proxy_type is None:
            raise ValueError(f"Unknown proxy type {scheme}")

        hostname = proxy.get("hostname")
        port = proxy.get("port")
        username = proxy.get("username")
        password = proxy.get("password")

        try:
            ip_address = ipaddress.ip_address(hostname)
        except ValueError:
            is_proxy_ipv6 = False
        else:
            is_proxy_ipv6 = isinstance(ip_address, ipaddress.IPv6Address)

        proxy_family = socket.AF_INET6 if is_proxy_ipv6 else socket.AF_INET
        sock = socks.socksocket(proxy_family)

        sock.set_proxy(
            proxy_type=proxy_type,
            addr=hostname,
            port=port,
            username=username,
            password=password
        )
        sock.settimeout(timeout)

        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor(max_workers=1) as executor:
            await loop.run_in_executor(executor, sock.connect, destination)

        sock.setblocking(False)

        reader, writer = await asyncio.open_connection(
            sock=sock
        )
        return cls(reader=reader, writer=writer)
