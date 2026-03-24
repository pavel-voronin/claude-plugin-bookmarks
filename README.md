# Bookmarks Channel

Claude Code channel server for [Chrome Bookmarks Gateway](https://github.com/pavel-voronin/chrome-bookmarks-gateway).

It does two things:

- streams bookmark change events into Claude Code through the experimental channel contract
- exposes bookmark operations as MCP tools backed by the gateway RPC API

The implementation follows the official [Channels reference](https://code.claude.com/docs/en/channels-reference).

## Requirements

- [Bun](https://bun.sh)
- A running [Chrome Bookmarks Gateway](https://github.com/pavel-voronin/chrome-bookmarks-gateway) instance

## Setup

**1. Go to your project and clone this repo into `plugins/`**

```sh
cd /your/project
git clone https://github.com/pavel-voronin/claude-plugin-bookmarks.git plugins/bookmarks
```

**2. Add the server to your project's `.mcp.json`**

```json
{
  "mcpServers": {
    "bookmarks": {
      "command": "bun",
      "args": [
        "run",
        "--cwd",
        "/your/project/plugins/bookmarks",
        "--silent",
        "start"
      ]
    }
  }
}
```

Replace `/your/project/plugins/bookmarks` with the actual path from step 1.

If your gateway doesn't run on the default `ws://127.0.0.1:3000/ws`, add an `env` block:

```json
"env": {
  "BOOKMARKS_GATEWAY_WS_URL": "ws://host:port/ws"
}
```

**3. Start Claude with the channel enabled**

```sh
claude --dangerously-load-development-channels server:bookmarks
```

## Tools exposed to Claude

- `get_tree`
- `get_subtree`
- `get_children`
- `get_recent`
- `get_bookmarks`
- `search_bookmarks`
- `create_bookmark`
- `update_bookmark`
- `move_bookmark`
- `remove_bookmark`
- `remove_bookmark_tree`

## Inbound event shape

Bookmark events are forwarded with field names matching the official [`chrome.bookmarks` API](https://developer.chrome.com/docs/extensions/reference/api/bookmarks) event signatures.

Concrete example for `onCreated` when the created node is a regular bookmark:

```
<channel source="bookmarks">
{
  "event": "onCreated",
  "data": {
    "id": "123",
    "bookmark": {
      "id": "123",
      "parentId": "1",
      "index": 7,
      "title": "Chrome Bookmarks API",
      "url": "https://developer.chrome.com/docs/extensions/reference/api/bookmarks",
      "dateAdded": 1772280000000,
      "dateLastUsed": 1772283600000,
      "syncing": true
    }
  }
}
</channel>
```

## Notes

- This server intentionally uses the Channels contract for inbound notifications and normal MCP tools for bookmark operations.
- It does not implement a chat-style `reply` tool because the gateway is an event source, not a human messaging surface.
