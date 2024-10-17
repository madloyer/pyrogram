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

from datetime import datetime
from typing import List, Dict, Type,Callable

import pyrogram
from pyrogram import raw, utils
from pyrogram import types
from pyrogram.errors import StickersetInvalid
from pyrogram.file_id import FileId, FileType, FileUniqueId, FileUniqueType
from ..object import Object


class Sticker(Object):
    """A sticker.

    Parameters:
        file_id (``str``):
            Identifier for this file, which can be used to download or reuse the file.

        file_unique_id (``str``):
            Unique identifier for this file, which is supposed to be the same over time and for different accounts.
            Can't be used to download or reuse the file.

        width (``int``):
            Sticker width.

        height (``int``):
            Sticker height.

        is_animated (``bool``):
            True, if the sticker is animated.

        is_video (``bool``):
            True, if the sticker is a video sticker.

        is_premium (``bool``):
            True, if the sticker is a premium only.

        file_name (``str``, *optional*):
            Sticker file name.

        mime_type (``str``, *optional*):
            MIME type of the file as defined by sender.

        file_size (``int``, *optional*):
            File size.

        date (:py:obj:`~datetime.datetime`, *optional*):
            Date the sticker was sent.

        emoji (``str``, *optional*):
            Emoji associated with the sticker.

        set_name (``str``, *optional*):
            Name of the sticker set to which the sticker belongs.

        thumbs (List of :obj:`~pyrogram.types.Thumbnail`, *optional*):
            Sticker thumbnails in the .webp or .jpg format.
    """

    # TODO: Add mask position

    def __init__(
        self,
        *,
        client: "pyrogram.Client" = None,
        file_id: str,
        file_unique_id: str,
        width: int,
        height: int,
        is_animated: bool,
        is_video: bool,
        is_premium: bool,
        file_name: str = None,
        mime_type: str = None,
        file_size: int = None,
        date: datetime = None,
        emoji: str = None,
        set_name: str = None,
        thumbs: List["types.Thumbnail"] = None
    ):
        super().__init__(client)

        self.file_id = file_id
        self.file_unique_id = file_unique_id
        self.file_name = file_name
        self.mime_type = mime_type
        self.file_size = file_size
        self.date = date
        self.width = width
        self.height = height
        self.is_animated = is_animated
        self.is_video = is_video
        self.is_premium = is_premium
        self.emoji = emoji
        self.set_name = set_name
        self.thumbs = thumbs
        # self.mask_position = mask_position

    cache = {}

    @staticmethod
    async def _get_sticker_set_name(invoke, input_sticker_set_id):
        try:
            set_id = input_sticker_set_id[0]
            set_access_hash = input_sticker_set_id[1]

            name = Sticker.cache.get((set_id, set_access_hash), None)

            if name is not None:
                return name

            name = (await invoke(
                raw.functions.messages.GetStickerSet(
                    stickerset=raw.types.InputStickerSetID(
                        id=set_id,
                        access_hash=set_access_hash
                    ),
                    hash=0
                )
            )).set.short_name

            Sticker.cache[(set_id, set_access_hash)] = name

            if len(Sticker.cache) > 250:
                for i in range(50):
                    Sticker.cache.pop(next(iter(Sticker.cache)))

            return name
        except StickersetInvalid:
            return None

    @staticmethod
    async def _parse(
        client,
        sticker: "raw.types.Document",
        document_attributes: Dict[Type["raw.base.DocumentAttribute"], "raw.base.DocumentAttribute"],
    ) -> "Sticker":
        sticker_attributes = (
            document_attributes[raw.types.DocumentAttributeSticker]
            if raw.types.DocumentAttributeSticker in document_attributes
            else document_attributes[raw.types.DocumentAttributeCustomEmoji]
        )

        image_size_attributes = document_attributes.get(raw.types.DocumentAttributeImageSize, None)
        file_name = getattr(document_attributes.get(raw.types.DocumentAttributeFilename, None), "file_name", None)
        video_attributes = document_attributes.get(raw.types.DocumentAttributeVideo, None)

        sticker_set = sticker_attributes.stickerset

        if isinstance(sticker_set, raw.types.InputStickerSetID):
            input_sticker_set_id = (sticker_set.id, sticker_set.access_hash)
            set_name = await Sticker._get_sticker_set_name(client.invoke, input_sticker_set_id)
        else:
            set_name = None

        return Sticker(
            file_id=FileId(
                file_type=FileType.STICKER,
                dc_id=sticker.dc_id,
                media_id=sticker.id,
                access_hash=sticker.access_hash,
                file_reference=sticker.file_reference
            ).encode(),
            file_unique_id=FileUniqueId(
                file_unique_type=FileUniqueType.DOCUMENT,
                media_id=sticker.id
            ).encode(),
            width=(
                image_size_attributes.w
                if image_size_attributes
                else video_attributes.w
                if video_attributes
                else 512
            ),
            height=(
                image_size_attributes.h
                if image_size_attributes
                else video_attributes.h
                if video_attributes
                else 512
            ),
            is_animated=sticker.mime_type == "application/x-tgsticker",
            is_video=sticker.mime_type == "video/webm",
            is_premium=bool(sticker.video_thumbs),
            # TODO: mask_position
            set_name=set_name,
            emoji=sticker_attributes.alt or None,
            file_size=sticker.size,
            mime_type=sticker.mime_type,
            file_name=file_name,
            date=utils.timestamp_to_datetime(sticker.date),
            thumbs=types.Thumbnail._parse(client, sticker),
            client=client
        )


    async def download(
        self,
        file_name: str = "",
        in_memory: bool = False,
        block: bool = True,
        progress: Callable = None,
        progress_args: tuple = ()
    ) -> str:
        """Bound method *download* of :obj:`~pyrogram.types.Sticker`.

        Use as a shortcut for:

        .. code-block:: python

            await client.download_media(message)

        Example:
            .. code-block:: python

                await message.download()

        Parameters:
            file_name (``str``, *optional*):
                A custom *file_name* to be used instead of the one provided by Telegram.
                By default, all files are downloaded in the *downloads* folder in your working directory.
                You can also specify a path for downloading files in a custom location: paths that end with "/"
                are considered directories. All non-existent folders will be created automatically.

            in_memory (``bool``, *optional*):
                Pass True to download the media in-memory.
                A binary file-like object with its attribute ".name" set will be returned.
                Defaults to False.

            block (``bool``, *optional*):
                Blocks the code execution until the file has been downloaded.
                Defaults to True.

            progress (``Callable``, *optional*):
                Pass a callback function to view the file transmission progress.
                The function must take *(current, total)* as positional arguments (look at Other Parameters below for a
                detailed description) and will be called back each time a new file chunk has been successfully
                transmitted.

            progress_args (``tuple``, *optional*):
                Extra custom arguments for the progress callback function.
                You can pass anything you need to be available in the progress callback scope; for example, a Message
                object or a Client instance in order to edit the message with the updated progress status.

        Other Parameters:
            current (``int``):
                The amount of bytes transmitted so far.

            total (``int``):
                The total size of the file.

            *args (``tuple``, *optional*):
                Extra custom arguments as defined in the ``progress_args`` parameter.
                You can either keep ``*args`` or add every single extra argument in your function signature.

        Returns:
            On success, the absolute path of the downloaded file as string is returned, None otherwise.

        Raises:
            RPCError: In case of a Telegram RPC error.
            ``ValueError``: If the message doesn't contain any downloadable media
        """
        return await self._client.download_media(
            message=self.file_id,
            file_name=file_name,
            in_memory=in_memory,
            block=block,
            progress=progress,
            progress_args=progress_args,
        )
