#  Pyrogram - Telegram MTProto API Client Library for Python
#  Copyright (C) 2017-present Dan <https://github.com/delivrance>
#
#  This file is part of Pyrogram.
#
#  Pyrogram is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published
#  by the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Pyrogram is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with Pyrogram.  If not, see <http://www.gnu.org/licenses/>.

import asyncio
import bisect
import logging
import os
from hashlib import sha1
from io import BytesIO
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
    Union,
)

import pyrogram
from pyrogram import raw
from pyrogram.connection import Connection
from pyrogram.crypto import mtproto
from pyrogram.errors import (
    AuthKeyDuplicated,
    FloodPremiumWait,
    FloodWait,
    InternalServerError,
    RPCError,
    SecurityCheckMismatch,
    ServiceUnavailable,
    Unauthorized,
)
from pyrogram.raw.all import layer
from pyrogram.raw.base import (
    MsgsAck,
    MsgsStateReq,
    Updates,
)
from pyrogram.raw.core import (
    FutureSalts,
    GzipPacked,
    Int,
    Message,
    MsgContainer,
    TLObject,
)
from pyrogram.raw.types import (
    BadMsgNotification,
    BadServerSalt,
    DestroyAuthKeyFail,
    DestroyAuthKeyNone,
    DestroyAuthKeyOk,
    DestroySessionNone,
    DestroySessionOk,
    MsgDetailedInfo,
    MsgNewDetailedInfo,
    MsgResendReq,
    MsgsAllInfo,
    NewSessionCreated,
    Pong,
    RpcResult,
)
from .internals import (
    MsgFactory,
    MsgId,
)

log = logging.getLogger(__name__)


class Result:
    def __init__(self):
        self.value = None
        self.event = asyncio.Event()


class Session:
    START_TIMEOUT = 2
    WAIT_TIMEOUT = 15
    SLEEP_THRESHOLD = 10
    MAX_RETRIES = 10
    ACKS_THRESHOLD = 10
    PING_INTERVAL = 5
    STORED_MSG_IDS_MAX_SIZE = 1000 * 2

    TRANSPORT_ERRORS = {
        404: "auth key not found",
        429: "transport flood",
        444: "invalid DC"
    }

    def __init__(
        self,
        client: "pyrogram.Client",
        dc_id: int,
        auth_key: bytes,
        test_mode: bool,
        is_media: bool = False,
        is_cdn: bool = False
    ):
        self.client = client
        self.dc_id = dc_id
        self.auth_key = auth_key
        self.test_mode = test_mode
        self.is_media = is_media
        self.is_cdn = is_cdn

        self.connection: Optional[Connection] = None

        self.auth_key_id = sha1(auth_key).digest()[-8:]

        self.session_id = os.urandom(8)
        self.msg_factory = MsgFactory()

        self.salt = 0

        self.pending_acks: Set[int] = set()

        self.results: Dict[int, Result] = {}

        self.stored_msg_ids: List[int] = []

        self.ping_task = None
        self.ping_task_event = asyncio.Event()

        self.recv_task = None

        self.is_started = asyncio.Event()

        self.loop = asyncio.get_event_loop()

    async def start(self):
        while True:
            try:
                self.connection = await self.client.connection_factory.new(
                    dc_id=self.dc_id,
                    test_mode=self.test_mode,
                    ipv6=self.client.ipv6,
                    proxy=self.client.proxy,
                    media=self.is_media,
                    protocol_factory=self.client.protocol_factory
                )

                self.recv_task = self.loop.create_task(self.recv_worker())

                await self.send(raw.functions.Ping(ping_id=0), timeout=self.START_TIMEOUT)

                if not self.is_cdn:
                    await self.send(
                        raw.functions.InvokeWithLayer(
                            layer=layer,
                            query=raw.functions.InitConnection(
                                api_id=await self.client.storage.api_id(),
                                app_version=self.client.app_version,
                                device_model=self.client.device_model,
                                system_version=self.client.system_version,
                                system_lang_code=self.client.system_lang_code,
                                lang_pack=self.client.lang_pack,
                                lang_code=self.client.lang_code,
                                query=raw.functions.help.GetConfig(),
                                params=self.client.init_connection_params,
                            )
                        ),
                        timeout=self.START_TIMEOUT
                    )

                self.ping_task = self.loop.create_task(self.ping_worker())

                log.info("Session initialized: Layer %s", layer)
                log.info("Device: %s - %s", self.client.device_model, self.client.app_version)
                log.info("System: %s (%s)", self.client.system_version, self.client.lang_code)
            except AuthKeyDuplicated as e:
                await self.stop()
                raise e
            except (OSError, RPCError):
                await self.stop()
            except Exception as e:
                await self.stop()
                raise e
            else:
                break

        self.is_started.set()

        log.info("Session started")

    async def stop(self):
        self.is_started.clear()

        self.stored_msg_ids.clear()

        self.ping_task_event.set()

        if self.ping_task:
            await self.ping_task

        self.ping_task_event.clear()

        if self.connection:
            await self.connection.close()

        if self.recv_task:
            await self.recv_task

        if not self.is_media and callable(self.client.disconnect_handler):
            try:
                await self.client.disconnect_handler(self.client)
            except Exception as e:
                log.exception(e)

        log.info("Session stopped")

    async def restart(self):
        await self.stop()
        await self.start()

    async def ping_worker(self):
        log.info("PingTask started")

        while True:
            try:
                await asyncio.wait_for(self.ping_task_event.wait(), self.PING_INTERVAL)
            except asyncio.TimeoutError:
                pass
            else:
                break

            try:
                await self.send(
                    raw.functions.PingDelayDisconnect(
                        ping_id=0, disconnect_delay=self.WAIT_TIMEOUT + 10
                    ), False
                )
            except OSError:
                self.loop.create_task(self.restart())  # TODO: graceful restart
                break
            except RPCError:
                pass

        log.info("PingTask stopped")

    async def recv_worker(self):
        log.info("NetworkTask started")

        while True:
            packet = await self.connection.recv()

            if packet is None or len(packet) == 4:
                if packet:
                    error_code = -Int.read(BytesIO(packet))

                    if error_code == 404:
                        raise Unauthorized(
                            "Auth key not found in the system. You must delete your session file "
                            "and log in again with your phone number or bot token."
                        )

                    log.warning(
                        "Server sent transport error: %s (%s)",
                        error_code, Session.TRANSPORT_ERRORS.get(error_code, "unknown error")
                    )

                if self.is_started.is_set():
                    self.loop.create_task(self.restart())  # TODO: graceful restart

                break

            message = await self.loop.run_in_executor(
                pyrogram.crypto_executor,
                mtproto.unpack,
                BytesIO(packet),
                self.session_id,
                self.auth_key,
                self.auth_key_id
            )

            try:
                await self.handle_message(message)
            except SecurityCheckMismatch as exc:
                log.info("Discarding packet: %s", exc)
                await self.connection.close()
                return
            except Exception as exc:
                log.warning("Unhandled exception: %s", exc)
            
            await self.send_pending_acks()

        log.info("NetworkTask stopped")

    async def handle_message(self, message: Message) -> None:
        if message.seq_no % 2:
            self.pending_acks.add(message.msg_id)

        self.message_security_check(message)

        if isinstance(message.body, RpcResult):
            return await self.handle_rpc_result(message)

        if isinstance(message.body, MsgContainer):
            return await self.handle_msg_container(message)

        if isinstance(message.body, GzipPacked):
            return await self.handle_gzip_packed(message)

        if isinstance(message.body, Pong):
            return await self.handle_pong(message)

        if isinstance(message.body, BadServerSalt):
            return await self.handle_bad_server_salt(message)

        if isinstance(message.body, BadMsgNotification):
            return await self.handle_bad_msg_notification(message)

        if isinstance(message.body, MsgDetailedInfo):
            return await self.handle_msg_detailed_info(message)

        if isinstance(message.body, MsgNewDetailedInfo):
            return await self.handle_msg_new_detailed_info(message)

        if isinstance(message.body, NewSessionCreated):
            return await self.handle_new_session_created(message)

        if isinstance(message.body, MsgsAck):
            return await self.handle_msgs_ack(message)

        if isinstance(message.body, FutureSalts):
            return await self.handle_future_salts(message)

        if isinstance(message.body, (MsgsStateReq, MsgResendReq)):
            return await self.handle_state_forgotten(message)

        if isinstance(message.body, MsgsAllInfo):
            return await self.handle_msg_all(message)

        if isinstance(message.body, (DestroySessionOk, DestroySessionNone)):
            return await self.handle_destroy_session(message)

        if isinstance(message.body, (DestroyAuthKeyOk, DestroyAuthKeyNone, DestroyAuthKeyFail)):
            return await self.handle_destroy_auth_key(message)

        return await self.handle_update(message)

    def message_security_check(self, message: Message) -> None:
        if len(self.stored_msg_ids) > Session.STORED_MSG_IDS_MAX_SIZE:
            del self.stored_msg_ids[:Session.STORED_MSG_IDS_MAX_SIZE // 2]

        if self.stored_msg_ids:
            if message.msg_id < self.stored_msg_ids[0]:
                raise SecurityCheckMismatch("The msg_id is lower than all the stored values")

        stored_msg_index = bisect.bisect_left(self.stored_msg_ids, message.msg_id)
        if stored_msg_index < len(self.stored_msg_ids) and self.stored_msg_ids[stored_msg_index] == message.msg_id:
            raise SecurityCheckMismatch("The msg_id is equal to any of the stored values")

        time_diff = (message.msg_id - MsgId()) / 2 ** 32

        if time_diff > 30:
            raise SecurityCheckMismatch(
                "The msg_id belongs to over 30 seconds in the future. "
                "Most likely the client time has to be synchronized."
            )

        if time_diff < -300:
            raise SecurityCheckMismatch(
                "The msg_id belongs to over 300 seconds in the past. "
                "Most likely the client time has to be synchronized."
            )

        bisect.insort(self.stored_msg_ids, message.msg_id)

    def set_request_result(self, request_id: int, value: Any) -> None:
        result = self.results.get(request_id)
        if result is None:
            return None

        result.value = value
        result.event.set()
        return None

    async def handle_msg_container(
        self,
        message: Message[MsgContainer],
    ) -> None:
        log.debug("MsgContainer received: %s", message)
        for msg in message.body.messages:
            try:
                await self.handle_message(msg)
            except SecurityCheckMismatch as exc:
                log.info("Discarding packet: %s", exc)
            except Exception as exc:
                log.warning("Unhandled exception: %s", exc)

    async def handle_rpc_result(self, message: Message[RpcResult]) -> None:
        log.debug("RpcResult received: %s", message)

        self.set_request_result(
            request_id=message.body.req_msg_id,
            value=message.body.result
        )

    async def handle_gzip_packed(self, message: Message[GzipPacked]) -> None:
        log.debug("GzipPacked received: %s", message)
        # TODO: re-search and handle_message
        return None

    async def handle_pong(self, message: Message[Pong]) -> None:
        log.debug("Pong received: %s", message)

        # TODO: set self ping event

        self.set_request_result(
            request_id=message.body.msg_id,
            value=message.body
        )

    async def handle_bad_server_salt(self, message: Message[BadServerSalt]) -> None:
        log.debug("BadServerSalt received: %s", message)
        # TODO: re-send messages

        self.set_request_result(
            request_id=message.body.bad_msg_id,
            value=message.body
        )

    async def handle_bad_msg_notification(self, message: Message[BadMsgNotification]) -> None:
        log.debug("BadMsgNotification received: %s", message)
        # TODO: re-send messages

        self.set_request_result(
            request_id=message.body.bad_msg_id,
            value=message.body
        )

    async def handle_msg_detailed_info(self, message: Message[MsgDetailedInfo]) -> None:
        log.debug("MsgDetailedInfo received: %s", message)
        self.pending_acks.add(message.body.answer_msg_id)

    async def handle_msg_new_detailed_info(self, message: Message[MsgNewDetailedInfo]) -> None:
        log.debug("MsgNewDetailedInfo received: %s", message)
        self.pending_acks.add(message.body.answer_msg_id)

    async def handle_new_session_created(self, message: Message[NewSessionCreated]) -> None:
        log.debug("NewSessionCreated received: %s", message)
        # TODO: re-send requests
        self.salt = message.body.server_salt
        return None

    async def handle_msgs_ack(self, message: Message[MsgsAck]) -> None:
        log.debug("MsgsAck received: %s", message)
        # TODO: ack messages
        # TODO: logOut request set result
        return None

    async def handle_future_salts(self, message: Message[FutureSalts]) -> None:
        log.debug("FutureSalts received: %s", message)
        # TODO: save future salts

        self.set_request_result(
            request_id=message.body.req_msg_id,
            value=message.body
        )

    async def handle_state_forgotten(self, message: Message[Union[MsgsStateReq, MsgResendReq]]) -> None:
        log.debug("MsgsStateReq/MsgResendReq received: %s", message)
        # TODO: re-send messages ?
        return None

    async def handle_msg_all(self, message: Message[MsgsAllInfo]) -> None:
        log.debug("MsgsAllInfo received: %s", message)
        # TODO: do nothing
        return None

    async def handle_destroy_session(self, message: Message[Union[DestroySessionOk, DestroySessionNone]]) -> None:
        log.debug("DestroySessionOk/DestroySessionNone received: %s", message)
        # TODO: Find pending destroy_session request and set result
        return None

    async def handle_destroy_auth_key(
        self, message: Message[Union[DestroyAuthKeyOk, DestroyAuthKeyNone, DestroyAuthKeyFail]]
    ) -> None:
        log.debug("DestroyAuthKeyOk/DestroyAuthKeyNone/DestroyAuthKeyFail received: %s", message)
        # TODO: Find pending destroy_auth_key request and set result
        return None

    async def handle_update(self, message: Message[Updates]) -> None:
        log.debug("Updates received: %s", message)
        await self.client.handle_updates(message.body)

    async def send_pending_acks(self) -> None:
        if len(self.pending_acks) < self.ACKS_THRESHOLD:
            return None

        log.debug("Sending %s acks", len(self.pending_acks))

        # TODO: lock self.pending_acks
        try:
            await self.send(data=raw.types.MsgsAck(msg_ids=list(self.pending_acks)), wait_response=False)
        except OSError:
            pass
        else:
            self.pending_acks.clear()

    async def send(self, data: TLObject, wait_response: bool = True, timeout: float = WAIT_TIMEOUT):
        message = self.msg_factory(data)
        msg_id = message.msg_id

        if wait_response:
            self.results[msg_id] = Result()

        log.debug("Sent: %s", message)

        payload = await self.loop.run_in_executor(
            pyrogram.crypto_executor,
            mtproto.pack,
            message,
            self.salt,
            self.session_id,
            self.auth_key,
            self.auth_key_id
        )

        try:
            await self.connection.send(payload)
        except OSError as e:
            self.results.pop(msg_id, None)
            raise e

        if not wait_response:
            return None

        try:
            await asyncio.wait_for(self.results[msg_id].event.wait(), timeout)
        except asyncio.TimeoutError:
            pass

        result = self.results.pop(msg_id).value

        if result is None:
            raise TimeoutError("Request timed out")

        if isinstance(result, raw.types.RpcError):
            if isinstance(data, (raw.functions.InvokeWithoutUpdates, raw.functions.InvokeWithTakeout)):
                data = data.query

            RPCError.raise_it(result, type(data))

        if isinstance(result, raw.types.BadMsgNotification):
            log.warning("%s: %s", BadMsgNotification.__name__, BadMsgNotification(result.error_code))

        if isinstance(result, raw.types.BadServerSalt):
            self.salt = result.new_server_salt
            return await self.send(data, wait_response, timeout)

        return result

    async def invoke(
        self,
        query: TLObject,
        retries: int = MAX_RETRIES,
        timeout: float = WAIT_TIMEOUT,
        sleep_threshold: float = SLEEP_THRESHOLD
    ):
        try:
            await asyncio.wait_for(self.is_started.wait(), self.WAIT_TIMEOUT)
        except asyncio.TimeoutError:
            pass

        if isinstance(query, (raw.functions.InvokeWithoutUpdates, raw.functions.InvokeWithTakeout)):
            inner_query = query.query
        else:
            inner_query = query

        query_name = ".".join(inner_query.QUALNAME.split(".")[1:])

        while True:
            try:
                return await self.send(query, timeout=timeout)
            except (FloodWait, FloodPremiumWait) as e:
                amount = e.value

                if amount > sleep_threshold >= 0:
                    raise

                log.warning(
                    '[%s] Waiting for %s seconds before continuing (required by "%s")',
                    self.client.name, amount, query_name
                )

                await asyncio.sleep(amount)
            except (OSError, InternalServerError, ServiceUnavailable) as e:
                if retries == 0:
                    raise e from None

                (log.warning if retries < 2 else log.info)(
                    '[%s] Retrying "%s" due to: %s',
                    Session.MAX_RETRIES - retries + 1,
                    query_name, str(e) or repr(e)
                )

                await asyncio.sleep(0.5)

                return await self.invoke(query, retries - 1, timeout)
