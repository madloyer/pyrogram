import asyncio
import ipaddress
import socket
from typing import (
    Self,
    Tuple,
)

from .base import Connector


class DirectConnector(Connector):
    @classmethod
    async def new(cls, destination: Tuple[str, int]) -> Self:
        host, port = destination

        try:
            ip_address = ipaddress.ip_address(host)
        except ValueError:
            is_ipv6 = False
        else:
            is_ipv6 = isinstance(ip_address, ipaddress.IPv6Address)

        family = socket.AF_INET6 if is_ipv6 else socket.AF_INET

        reader, writer = await asyncio.open_connection(
            host=host,
            port=port,
            family=family
        )
        return cls(
            reader=reader,
            writer=writer
        )
