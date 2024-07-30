from typing import (
    Any,
    Union,
)

import pyrogram
from pyrogram.handlers import (
    CallbackQueryHandler,
    ChatJoinRequestHandler,
    ChatMemberUpdatedHandler,
    ChosenInlineResultHandler,
    DeletedMessagesHandler,
    EditedMessageHandler,
    InlineQueryHandler,
    MessageHandler,
    PollHandler,
    PreCheckoutQueryHandler,
    StoryHandler,
    UserStatusHandler,
)
from pyrogram.handlers.handler import Handler
from pyrogram.raw.core import TLObject
from pyrogram.raw.types import (
    Channel,
    ChannelForbidden,
    Chat,
    ChatEmpty,
    ChatForbidden,
    UpdateBotCallbackQuery,
    UpdateBotChatInviteRequester,
    UpdateBotDeleteBusinessMessage,
    UpdateBotEditBusinessMessage,
    UpdateBotInlineQuery,
    UpdateBotInlineSend,
    UpdateBotNewBusinessMessage,
    UpdateBotPrecheckoutQuery,
    UpdateChannelParticipant,
    UpdateChatParticipant,
    UpdateDeleteChannelMessages,
    UpdateDeleteMessages,
    UpdateEditChannelMessage,
    UpdateEditMessage,
    UpdateInlineBotCallbackQuery,
    UpdateMessagePoll,
    UpdateNewChannelMessage,
    UpdateNewMessage,
    UpdateNewScheduledMessage,
    UpdateStory,
    UpdateUserStatus,
    User,
    UserEmpty,
)
from pyrogram.utils import parse_deleted_messages

UsersType = dict[int, User | UserEmpty]
ChatsType = dict[int, Channel | ChannelForbidden | Chat | ChatEmpty | ChatForbidden]

NewMessageType = Union[
    UpdateNewMessage,
    UpdateNewChannelMessage,
    UpdateNewScheduledMessage,
    UpdateBotNewBusinessMessage,
]
EditMessageType = Union[
    UpdateEditMessage, UpdateEditChannelMessage, UpdateBotEditBusinessMessage
]
DeleteMessageType = Union[
    UpdateDeleteMessages,
    UpdateDeleteChannelMessages,
    UpdateBotDeleteBusinessMessage,
]
CallbackQueryUpdateType = Union[UpdateBotCallbackQuery, UpdateInlineBotCallbackQuery]
ChatMemberUpdateType = Union[UpdateChatParticipant, UpdateChannelParticipant]
UserStatusUpdateType = Union[UpdateUserStatus]
InlineQueryUpdateType = Union[UpdateBotInlineQuery]
PollUpdateType = Union[UpdateMessagePoll]
ChosenInlineResultUpdateType = Union[UpdateBotInlineSend]
ChatJoinRequestUpdateType = Union[UpdateBotChatInviteRequester]
NewStoryUpdateType = Union[UpdateStory]
BotPreCheckoutQueryUpdateType = Union[UpdateBotPrecheckoutQuery]


async def message_parser(
    client: "pyrogram.Client",
    update: NewMessageType,
    users: UsersType,
    chats: ChatsType,
):
    is_scheduled = isinstance(update, UpdateNewScheduledMessage)

    # noinspection PyProtectedMember
    update = await pyrogram.types.Message._parse(
        client=client,
        message=update.message,
        users=users,
        chats=chats,
        is_scheduled=is_scheduled,
        business_connection_id=getattr(update, "connection_id", None),
        reply_to_message=getattr(update, "reply_to_message", None),
    )

    return (
        update,
        "message",
        MessageHandler,
    )


async def edited_message_parser(
    client: "pyrogram.Client",
    update: EditMessageType,
    users: UsersType,
    chats: ChatsType,
):
    is_scheduled = isinstance(update, UpdateNewScheduledMessage)

    # noinspection PyProtectedMember
    update = await pyrogram.types.Message._parse(
        client=client,
        message=update.message,
        users=users,
        chats=chats,
        is_scheduled=is_scheduled,
        business_connection_id=getattr(update, "connection_id", None),
        reply_to_message=getattr(update, "reply_to_message", None),
    )

    return update, "edited_message", EditedMessageHandler


def deleted_messages_parser(
    client: "pyrogram.Client",
    update: DeleteMessageType,
    users: UsersType,
    chats: ChatsType,
):
    update = parse_deleted_messages(
        client=client, update=update, users=users, chats=chats
    )

    return update, "deleted_messages", DeletedMessagesHandler


async def callback_query_parser(
    client: "pyrogram.Client", update: CallbackQueryUpdateType, users: UsersType
):
    # noinspection PyProtectedMember
    update = await pyrogram.types.CallbackQuery._parse(
        client=client, callback_query=update, users=users
    )

    return update, "callback_query", CallbackQueryHandler


def user_status_parser(client: "pyrogram.Client", update: UserStatusUpdateType):
    # noinspection PyProtectedMember
    update = pyrogram.types.User._parse_user_status(client=client, user_status=update)

    return update, "user_status", UserStatusHandler


def inline_query_parser(
    client: "pyrogram.Client", update: InlineQueryUpdateType, users: UsersType
):
    # noinspection PyProtectedMember
    update = pyrogram.types.InlineQuery._parse(client=client, inline_query=update, users=users)

    return update, "inline_query", InlineQueryHandler


def poll_parser(client: "pyrogram.Client", update: PollUpdateType):
    # noinspection PyProtectedMember
    update = pyrogram.types.Poll._parse_update(client=client, update=update)

    return update, "poll", PollHandler


def chosen_inline_result_parser(
    client: "pyrogram.Client",
    update: ChosenInlineResultUpdateType,
    users: UsersType,
):
    # noinspection PyProtectedMember
    update = pyrogram.types.ChosenInlineResult._parse(
        client=client,
        chosen_inline_result=update,
        users=users,
    )

    return update, "chosen_inline_result", ChosenInlineResultHandler


def chat_member_updated_parser(
    client: "pyrogram.Client",
    update: ChatMemberUpdateType,
    users: UsersType,
    chats: ChatsType,
):
    # noinspection PyProtectedMember
    update = pyrogram.types.ChatMemberUpdated._parse(
        client=client, update=update, users=users, chats=chats
    )

    return update, "chat_member_updated", ChatMemberUpdatedHandler


def chat_join_request_parser(
    client: "pyrogram.Client",
    update: ChatJoinRequestUpdateType,
    users: UsersType,
    chats: ChatsType,
):
    # noinspection PyProtectedMember
    update = pyrogram.types.ChatJoinRequest._parse(
        client=client, update=update, users=users, chats=chats
    )

    return update, "chat_join_request", ChatJoinRequestHandler


async def story_parser(
    client: "pyrogram.Client",
    update: NewStoryUpdateType,
    users: UsersType,
    chats: ChatsType,
):
    # noinspection PyProtectedMember
    update = await pyrogram.types.Story._parse(
        client=client,
        story=update.story,
        users=users,
        chats=chats,
        peer=update.peer,
    )
    return update, "story", StoryHandler


async def pre_checkout_query_parser(
    client: "pyrogram.Client",
    update: BotPreCheckoutQueryUpdateType,
    users: UsersType,
):
    # noinspection PyProtectedMember
    update = await pyrogram.types.PreCheckoutQuery._parse(
        client=client,
        pre_checkout_query=update,
        users=users,
    )
    return update, "pre_checkout_query", PreCheckoutQueryHandler


async def parse_update(
    client: "pyrogram.Client",
    update: TLObject,
    users: UsersType,
    chats: ChatsType,
) -> tuple[Any | None, str | None, type[Handler] | None]:
    if isinstance(update, NewMessageType):
        return await message_parser(client, update, users, chats)
    elif isinstance(update, EditMessageType):
        return await edited_message_parser(client, update, users, chats)
    elif isinstance(update, DeleteMessageType):
        return deleted_messages_parser(client, update, users, chats)
    elif isinstance(update, CallbackQueryUpdateType):
        return await callback_query_parser(client, update, users)
    elif isinstance(update, ChatMemberUpdateType):
        return chat_member_updated_parser(client, update, users, chats)
    elif isinstance(update, UserStatusUpdateType):
        return user_status_parser(client, update)
    elif isinstance(update, InlineQueryUpdateType):
        return inline_query_parser(client, update, users)
    elif isinstance(update, PollUpdateType):
        return poll_parser(client, update)
    elif isinstance(update, ChosenInlineResultUpdateType):
        return chosen_inline_result_parser(client, update, users)
    elif isinstance(update, ChatJoinRequestUpdateType):
        return chat_join_request_parser(client, update, users, chats)
    elif isinstance(update, NewStoryUpdateType):
        return await story_parser(client, update, users, chats)
    elif isinstance(update, BotPreCheckoutQueryUpdateType):
        return await pre_checkout_query_parser(client, update, users)
    else:
        return None, None, None
