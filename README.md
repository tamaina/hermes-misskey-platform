# Misskey platform plugin for Hermes Agent

A [Hermes Agent](https://github.com/NousResearch/hermes-agent) gateway platform plugin that lets you talk to your agent from **Misskey** (and Misskey-compatible forks).

It connects to a Misskey instance using the REST API (outgoing) and the streaming WebSocket `main` channel (incoming), and:

- receives **direct notes** (visibility `specified`) and replies in the same thread with the same visibility / visible users;
- receives **personal chat** messages (Misskey chat, 1:1 only — room chats are ignored) and replies in chat;
- optionally handles public, home, or followers **@mentions** (off by default);
- enforces a **required allowlist** at the adapter level (fails closed when empty);
- ignores its own notes/messages (loop guard), de-duplicates events;
- reconnects the stream with backoff (1, 2, 5, 10, 30, 60 s);
- splits long replies to the instance note limit (`/api/meta` `maxNoteTextLength`, else 3000) or 2000 chars for chat;
- supports out-of-process sends (cron / `send_message`) via `MISSKEY_HOME_CHANNEL`.

## Requirements

- Hermes Agent with **platform plugin** support (`kind: platform` plugins, `ctx.register_platform`).
- `aiohttp` — already present in the Hermes virtualenv; no extra install needed.
- A Misskey account for the bot and an access token.

## Install

```sh
# Option A: copy
cp -r misskey-plugin-dir ~/.hermes/plugins/misskey

# Option B: clone
git clone https://github.com/tamaina/hermes-misskey-platform.git ~/.hermes/plugins/misskey

hermes plugins enable misskey
```

Then set the environment variables below (e.g. in the Hermes `.env` file) and restart the Hermes gateway.

## Configuration (environment variables)

| Variable | Required | Description |
|---|---|---|
| `MISSKEY_TOKEN` | **yes** | Access token of the bot account. Never logged. |
| `MISSKEY_INSTANCE_URL` | **yes** | Instance base URL, e.g. `https://example.social`. No default; the adapter refuses to start without it. |
| `MISSKEY_ALLOWED_USERS` | **yes** | Comma-separated `user@host` accounts allowed to talk to the bot, e.g. `alice@example.social`. **If empty, nobody is accepted** and a warning is logged on connect. |
| `MISSKEY_ALLOW_ALL_USERS` | no | `true` to disable the allowlist (development only; not recommended). |
| `MISSKEY_LISTEN_MENTIONS` | no | `true` to also handle @mentions in public, home, or followers notes. Default `false`. |
| `MISSKEY_HOME_CHANNEL` | no | Target for cron / notification delivery: `chat:<userId>` (chat) or `user:<userId>` (direct note). |
| `MISSKEY_HOME_CHANNEL_NAME` | no | Display name for the home channel. |
| `MISSKEY_MAX_NOTE_LENGTH` | no | Override note character limit (default: instance `maxNoteTextLength`, else 3000). |

Example:

```sh
MISSKEY_TOKEN=xxxxxxxx
MISSKEY_INSTANCE_URL=https://example.social
MISSKEY_ALLOWED_USERS=alice@example.social
```

### Token permissions

Create the token (Settings → API, or an app) with these permissions:

- `read:account`
- `write:notes`
- `read:notifications`
- `read:chat`
- `write:chat`

## Security notes

- **Use a dedicated bot account**, not your personal account. The agent can post as this account.
- **The allowlist is required.** There is no built-in default user; an empty `MISSKEY_ALLOWED_USERS` means the bot accepts no one (fail closed). Local users may be listed as `alice@example.social` (or bare `alice` for the bot's own instance).
- **Public mention listening is off by default.** Only direct (`specified`) notes and personal chat are handled unless you set `MISSKEY_LISTEN_MENTIONS=true`. Even then, the allowlist still applies.
- Replies to direct notes keep `specified` visibility and are only visible to the original participants.
- The token is sent only to your instance (in API bodies and the streaming URL query) and is never written to logs; lock files store only a hash fingerprint.

## Limitations

- Tested against one Misskey-compatible instance running **9000.0.0-beta.10**. Other versions/forks should work but are not verified.
- Chat support depends on the instance: Misskey chat must be available (`chatAvailability` policy). If it is disabled, only direct notes work.
- No typing indicator; attachments in chat are surfaced as `[attachment]` text only; room chats are not supported.

## Running tests

The tests are mock-based (stdlib `unittest`, no network) but need Hermes's `gateway` package and its venv:

```sh
cd ~/.hermes/plugins/misskey
<path-to-hermes-venv>/bin/python test_adapter.py
# If the Hermes source tree is not at ~/.hermes/hermes-agent:
HERMES_AGENT_ROOT=/path/to/hermes-agent <path-to-hermes-venv>/bin/python test_adapter.py
```

## License

MIT — see [LICENSE](LICENSE).

---

## 日本語

Hermes Agent を Misskey（および互換フォーク）から操作するためのプラットフォームプラグインです。ダイレクト投稿（公開範囲「指名」）と個人チャットを受信し、同じ会話に返信します。

- `~/.hermes/plugins/misskey` に配置し、`hermes plugins enable misskey` で有効化してください。
- `MISSKEY_TOKEN`・`MISSKEY_INSTANCE_URL`・`MISSKEY_ALLOWED_USERS` は**必須**です。許可リストが空の場合は誰も受け付けません（フェイルクローズ）。
- トークン権限: `read:account`, `write:notes`, `read:notifications`, `read:chat`, `write:chat`。
- 専用の Bot アカウントを使ってください。公開メンションへの反応はデフォルトで無効です（`MISSKEY_LISTEN_MENTIONS=true` で有効化）。
- 動作確認は Misskey 互換インスタンス 1 つ（9000.0.0-beta.10）のみです。チャット機能はインスタンスの設定に依存します。
