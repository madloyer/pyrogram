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
    Awaitable,
    Callable,
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
from .internals import (
    MsgFactory,
    MsgId,
)

log = logging.getLogger(__name__)


class RequestState:
    def __init__(
        self,
        request: raw.core.TLObject,
    ):
        self.request = request
        self.future = asyncio.Future()

    def set_result(self, value: Any) -> None:
        if self.future.done():
            return None
        self.future.set_result(value)

    def set_exception(self, value: Exception) -> None:
        if self.future.done():
            return None
        self.future.set_exception(value)

    @property
    def request_raw(self) -> raw.core.TLObject:
        request = self.request
        while isinstance(
            request, (
                raw.functions.InvokeAfterMsg,
                raw.functions.InvokeAfterMsgs,
                raw.functions.InitConnection,
                raw.functions.InvokeWithLayer,
                raw.functions.InvokeWithoutUpdates,
                raw.functions.InvokeWithMessagesRange,
                raw.functions.InvokeWithTakeout,
                raw.functions.InvokeWithBusinessConnection,
            )
        ):
            request = request.query

        return request


def get_auth_key_id(auth_key: bytes) -> bytes:
    auth_key_sha1 = sha1(auth_key).digest()
    return auth_key_sha1[-8:]


class Session:
    START_TIMEOUT = 2
    WAIT_TIMEOUT = 15
    SLEEP_THRESHOLD = 10
    MAX_RETRIES = 10
    ACKS_THRESHOLD = 10
    PING_INTERVAL = 5
    STORED_MSG_IDS_MAX_SIZE = 1000 * 2

    TRANSPORT_ERRORS = {
        403: "forbidden",
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

        self.auth_key_id = get_auth_key_id(self.auth_key)

        self.session_id = os.urandom(8)
        self.msg_factory = MsgFactory()

        self.salt = 0

        self.pending_acks: Set[int] = set()
        self.pending_requests: asyncio.Queue[RequestState] = asyncio.Queue()

        self.results: Dict[int, RequestState] = {}

        self.stored_msg_ids: List[int] = []

        self.ping_task: Optional[asyncio.Task[None]] = None
        self.ping_task_event = asyncio.Event()

        self.recv_task: Optional[asyncio.Task[None]] = None
        self.send_task: Optional[asyncio.Task[None]] = None

        self.is_started = asyncio.Event()

        self.loop = asyncio.get_event_loop()

        self.handler_by_constructor_id: Dict[int, Callable[[raw.core.Message], Awaitable[None]]] = {
            raw.types.RpcResult.ID: self.handle_rpc_result,
            raw.core.MsgContainer.ID: self.handle_msg_container,
            raw.core.GzipPacked.ID: self.handle_gzip_packed,
            raw.types.Pong.ID: self.handle_pong,
            raw.types.BadServerSalt.ID: self.handle_bad_server_salt,
            raw.types.BadMsgNotification.ID: self.handle_bad_msg_notification,
            raw.types.MsgDetailedInfo.ID: self.handle_msg_detailed_info,
            raw.types.MsgNewDetailedInfo.ID: self.handle_msg_new_detailed_info,
            raw.types.NewSessionCreated.ID: self.handle_new_session_created,
            raw.types.MsgsAck.ID: self.handle_msgs_ack,
            raw.core.FutureSalts.ID: self.handle_future_salts,
            raw.types.MsgsStateReq.ID: self.handle_state_forgotten,
            raw.types.MsgResendReq.ID: self.handle_state_forgotten,
            raw.types.MsgsAllInfo.ID: self.handle_msg_all,
            raw.types.DestroySessionOk.ID: self.handle_destroy_session,
            raw.types.DestroySessionNone.ID: self.handle_destroy_session,
            raw.types.DestroyAuthKeyOk.ID: self.handle_destroy_auth_key,
            raw.types.DestroyAuthKeyNone.ID: self.handle_destroy_auth_key,
            raw.types.DestroyAuthKeyFail.ID: self.handle_destroy_auth_key,

            raw.types.Updates.ID: self.handle_update,
            raw.types.UpdatesCombined.ID: self.handle_update,
            raw.types.UpdateShortMessage.ID: self.handle_update,
            raw.types.UpdateShortChatMessage.ID: self.handle_update,
            raw.types.UpdateShort.ID: self.handle_update,
            raw.types.UpdatesTooLong.ID: self.handle_update
        }

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
                self.send_task = self.loop.create_task(self.send_worker())

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

        if self.send_task:
            await self.send_task

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

    async def send_worker(self):
        while True:
            request_state = await self.pending_requests.get()

            try:
                message = self.msg_factory(request_state.request)
                payload = await self.loop.run_in_executor(
                    pyrogram.crypto_executor,
                    mtproto.pack,
                    message,
                    self.salt,
                    self.session_id,
                    self.auth_key,
                    self.auth_key_id
                )

                self.results[message.msg_id] = request_state

                try:
                    await self.connection.send(payload)
                except OSError as exc:
                    log.warning("Connection error: %s", exc)  # TODO: graceful restart
                    raise
                else:
                    log.debug("Sent: %s", request_state)
            finally:
                self.pending_requests.task_done()

    async def recv_worker(self):
        log.info("NetworkTask started")

        while True:
            packet = await self.connection.recv()

            if packet is None or len(packet) == 4:
                if packet:
                    error_code = -int.from_bytes(packet, byteorder="little", signed=True)

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

    async def handle_message(self, message: raw.core.Message) -> None:
        if message.seq_no % 2:
            self.pending_acks.add(message.msg_id)

        self.message_security_check(message)

        handler = self.handler_by_constructor_id.get(message.body.ID)
        if handler is None:
            return await self.handle_unknown(message)
        return await handler(message)

    def message_security_check(self, message: raw.core.Message) -> None:
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

    def set_request_state_result(self, msg_id: int, value: Any) -> None:
        request_state = self.results.pop(msg_id, None)
        if request_state is None:
            return None

        request_state.set_result(value)
        return None

    def set_request_state_exception(self, msg_id: int, value: Exception) -> None:
        request_state = self.results.pop(msg_id, None)
        if request_state is None:
            return None

        request_state.set_exception(value)
        return None

    async def handle_msg_container(
        self,
        message: raw.core.Message[raw.core.MsgContainer],
    ) -> None:
        log.debug("raw.core.MsgContainer received: %s", message)
        for msg in message.body.messages:
            try:
                await self.handle_message(msg)
            except SecurityCheckMismatch as exc:
                log.info("Discarding packet: %s", exc)
            except Exception as exc:
                log.warning("Unhandled exception: %s", exc)

    async def handle_rpc_result(self, message: raw.core.Message[raw.types.RpcResult]) -> None:
        log.debug("raw.types.RpcResult received: %s", message)

        request_state = self.results.pop(message.body.req_msg_id, None)
        if request_state is None:
            return None

        if isinstance(message.body.result, raw.types.RpcError):
            try:
                RPCError.raise_it(message.body.result, type(request_state.request_raw))
            except Exception as exc:
                request_state.set_exception(exc)
            else:
                request_state.set_result(None)
        else:
            request_state.set_result(message.body.result)

    async def handle_gzip_packed(self, message: raw.core.Message[raw.core.GzipPacked]) -> None:
        log.debug("raw.core.GzipPacked received: %s", message)
        # TODO: re-search and handle_message
        return None

    async def handle_pong(self, message: raw.core.Message[raw.types.Pong]) -> None:
        log.debug("raw.types.Pong received: %s", message)

        # TODO: set self ping event

        self.set_request_state_result(
            msg_id=message.body.msg_id,
            value=message.body
        )

    async def handle_bad_server_salt(self, message: raw.core.Message[raw.types.BadServerSalt]) -> None:
        log.debug("raw.types.BadServerSalt received: %s", message)

        self.salt = message.body.new_server_salt

        request_state = self.results.pop(message.body.bad_msg_id, None)
        if request_state is None:
            return None

        self.pending_requests.put_nowait(request_state)

    async def handle_bad_msg_notification(self, message: raw.core.Message[raw.types.BadMsgNotification]) -> None:
        log.debug("raw.types.BadMsgNotification received: %s", message)
        # TODO: re-send messages

        request_state = self.results.pop(message.body.bad_msg_id, None)
        if request_state is None:
            return None

        rpc_name = ".".join(request_state.request_raw.QUALNAME.split(".")[1:])

        request_state.set_exception(
            value=RPCError(
                value="BAD_MSG_NOTIFICATION",
                rpc_name=rpc_name,
            )
        )

    async def handle_msg_detailed_info(self, message: raw.core.Message[raw.types.MsgDetailedInfo]) -> None:
        log.debug("raw.types.MsgDetailedInfo received: %s", message)
        self.pending_acks.add(message.body.answer_msg_id)

    async def handle_msg_new_detailed_info(self, message: raw.core.Message[raw.types.MsgNewDetailedInfo]) -> None:
        log.debug("raw.types.MsgNewDetailedInfo received: %s", message)
        self.pending_acks.add(message.body.answer_msg_id)

    async def handle_new_session_created(self, message: raw.core.Message[raw.types.NewSessionCreated]) -> None:
        log.debug("raw.types.NewSessionCreated received: %s", message)
        # TODO: re-send requests
        self.salt = message.body.server_salt
        return None

    async def handle_msgs_ack(self, message: raw.core.Message[raw.types.MsgsAck]) -> None:
        log.debug("raw.types.MsgsAck received: %s", message)

        for msg_id in message.body.msg_ids:
            request_state = self.results.get(msg_id)
            if request_state is None:
                continue

            if isinstance(request_state.request_raw, raw.functions.auth.LogOut):
                del self.results[msg_id]
                request_state.set_result(True)

        return None

    async def handle_future_salts(self, message: raw.core.Message[raw.core.FutureSalts]) -> None:
        log.debug("raw.core.FutureSalts received: %s", message)
        # TODO: save future salts

        self.set_request_state_result(
            msg_id=message.body.req_msg_id,
            value=message.body
        )

    async def handle_state_forgotten(
        self, message: raw.core.Message[Union[raw.types.MsgsStateReq, raw.types.MsgResendReq]]
    ) -> None:
        log.debug("raw.types.MsgsStateReq/raw.types.MsgResendReq received: %s", message)
        return None

    async def handle_msg_all(self, message: raw.core.Message[raw.types.MsgsAllInfo]) -> None:
        log.debug("raw.types.MsgsAllInfo received: %s", message)
        return None

    async def handle_destroy_session(
        self, message: raw.core.Message[Union[raw.types.DestroySessionOk, raw.types.DestroySessionNone]]
    ) -> None:
        log.debug("raw.types.DestroySessionOk/raw.types.DestroySessionNone received: %s", message)

        msg_ids: List[int] = []

        for msg_id, request_state in self.results.items():
            request_raw = request_state.request_raw

            if not isinstance(request_raw, raw.functions.DestroySession):
                continue

            if request_raw.session_id == message.body.session_id:
                msg_ids.append(msg_id)

        for msg_id in msg_ids:
            self.set_request_state_result(msg_id=msg_id, value=message.body)

        return None

    async def handle_destroy_auth_key(
        self,
        message: raw.core.Message[
            Union[raw.types.DestroyAuthKeyOk, raw.types.DestroyAuthKeyNone, raw.types.DestroyAuthKeyFail]]
    ) -> None:
        log.debug(
            "raw.types.DestroyAuthKeyOk/raw.types.DestroyAuthKeyNone/raw.types.DestroyAuthKeyFail received: %s", message
        )

        # TODO: close connection

        msg_ids: List[int] = []

        for msg_id, request_state in self.results.items():
            if isinstance(request_state.request_raw, raw.functions.DestroyAuthKey):
                msg_ids.append(msg_id)

        for msg_id in msg_ids:
            self.set_request_state_result(msg_id=msg_id, value=message.body)

        return None

    async def handle_update(self, message: raw.core.Message[raw.base.Updates]) -> None:
        log.debug("raw.base.Updates received: %s", message)
        await self.client.handle_updates(message.body)

    async def handle_unknown(self, message: raw.core.Message[raw.core.TLObject]) -> None:
        log.debug("Unknown received: %s", message)
        return None

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

    async def send(self, data: raw.core.TLObject, wait_response: bool = True, timeout: float = WAIT_TIMEOUT):
        request_state = RequestState(request=data)
        self.pending_requests.put_nowait(request_state)

        if not wait_response:
            return None

        try:
            result = await asyncio.wait_for(request_state.future, timeout)
        except asyncio.TimeoutError as exc:
            raise TimeoutError("Request timed out") from exc

        return result

    async def invoke(
        self,
        query: raw.core.TLObject,
        retries: int = MAX_RETRIES,
        timeout: float = WAIT_TIMEOUT,
        sleep_threshold: float = SLEEP_THRESHOLD
    ):
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
