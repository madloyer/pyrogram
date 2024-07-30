import asyncio
from typing import (
    Dict,
    Optional,
    Self,
    Tuple,
    TypedDict,
)

from python_socks import ProxyType
from python_socks.async_.asyncio import Proxy as AsyncProxy

from .base import Connector


class Proxy(TypedDict):
    scheme: str
    hostname: str
    port: int
    username: Optional[str]
    password: Optional[str]


proxy_type_by_scheme: Dict[str, ProxyType] = {
    "SOCKS4": ProxyType.SOCKS4,
    "SOCKS5": ProxyType.SOCKS5,
    "HTTP": ProxyType.HTTP,
}


class PythonSocksProxyConnector(Connector):
    @classmethod
    async def new(cls, destination: Tuple[str, int], proxy: Proxy, timeout: int) -> Self:
        scheme = proxy.get("scheme")
        if scheme is None:
            raise ValueError("No scheme specified")

        proxy_type = proxy_type_by_scheme.get(scheme.upper())

        if proxy_type is None:
            raise ValueError(f"Unknown proxy type {scheme}")

        hostname = proxy.get("hostname")
        port = proxy.get("port")
        username = proxy.get("username")
        password = proxy.get("password")

        proxy = AsyncProxy.create(
            proxy_type=proxy_type,
            host=hostname,
            port=port,
            username=username,
            password=password,
        )

        sock = await proxy.connect(
            dest_host=destination[0],
            dest_port=destination[1],
            timeout=timeout,
        )

        reader, writer = await asyncio.open_connection(
            sock=sock
        )
        return cls(reader=reader, writer=writer)
