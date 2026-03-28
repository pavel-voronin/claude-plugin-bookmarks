#!/usr/bin/env bun

import { chmodSync, existsSync, mkdirSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { WebSocket } from "ws";

type TransportMode = "ws";

type JsonRpcId = string | number;

type JsonRpcRequest = {
  jsonrpc: "2.0";
  id?: JsonRpcId;
  method: string;
  params?: unknown;
};

type JsonRpcSuccess = {
  jsonrpc: "2.0";
  id: JsonRpcId;
  result: unknown;
};

type JsonRpcError = {
  jsonrpc: "2.0";
  id: JsonRpcId | null;
  error: {
    code: number;
    message: string;
    data?: unknown;
  };
};

type JsonRpcNotification = {
  jsonrpc: "2.0";
  method: string;
  params?: {
    args?: unknown[];
  };
};

type RpcResponse = JsonRpcSuccess | JsonRpcError;

type EnvConfig = {
  transport: TransportMode;
  wsUrl: string;
  synczUrl: string;
  authToken?: string;
};

const STATE_DIR =
  process.env.BOOKMARKS_CHANNEL_STATE_DIR ??
  join(homedir(), ".claude", "channels", "bookmarks");
const ENV_FILE = join(STATE_DIR, ".env");

try {
  chmodSync(ENV_FILE, 0o600);
  for (const line of readFileSync(ENV_FILE, "utf8").split("\n")) {
    const match = line.match(/^(\w+)=(.*)$/);
    if (match && process.env[match[1]] === undefined) {
      process.env[match[1]] = match[2];
    }
  }
} catch {}

function requiredUrl(name: string, fallback: string): string {
  const value = process.env[name] ?? fallback;
  return new URL(value).toString();
}

function toHttpUrl(wsUrl: string, pathname: string): string {
  const url = new URL(wsUrl);
  url.protocol = url.protocol === "wss:" ? "https:" : "http:";
  url.pathname = pathname;
  url.search = "";
  return url.toString();
}

function readConfig(): EnvConfig {
  const transport = process.env.BOOKMARKS_GATEWAY_TRANSPORT ?? "ws";
  if (transport !== "ws") {
    throw new Error(`BOOKMARKS_GATEWAY_TRANSPORT must be ws; got ${transport}`);
  }
  const wsUrl = requiredUrl("BOOKMARKS_GATEWAY_WS_URL", "ws://127.0.0.1:3000/ws");
  return {
    transport: "ws",
    wsUrl,
    synczUrl:
      process.env.BOOKMARKS_GATEWAY_SYNCZ_URL ??
      toHttpUrl(wsUrl, "/syncz"),
    authToken: process.env.BOOKMARKS_GATEWAY_AUTH_TOKEN || undefined,
  };
}

function isJsonRpcError(value: unknown): value is JsonRpcError {
  return typeof value === "object" && value !== null && "error" in value;
}

// Field names here intentionally match the official chrome.bookmarks event
// signatures so Claude sees stable, documented names instead of positional args.
function normalizeEventData(event: string, args: unknown[] = []): unknown {
  switch (event) {
    case "system.syncStatusChanged":
      return {
        status: args[0],
      };
    case "onCreated":
      return {
        id: args[0],
        bookmark: args[1],
      };
    case "onRemoved":
      return {
        id: args[0],
        removeInfo: args[1],
      };
    case "onChanged":
      return {
        id: args[0],
        changeInfo: args[1],
      };
    case "onMoved":
      return {
        id: args[0],
        moveInfo: args[1],
      };
    case "onChildrenReordered":
      return {
        id: args[0],
        reorderInfo: args[1],
      };
    default:
      return { args };
  }
}

class GatewayBridge {
  private readonly config: EnvConfig;
  private readonly server: Server;
  private ws: WebSocket | null = null;
  private nextRpcId = 1;
  private pending = new Map<JsonRpcId, { resolve: (value: unknown) => void; reject: (reason: Error) => void }>();
  private reconnectTimer: NodeJS.Timeout | null = null;
  private stopped = false;

  constructor(config: EnvConfig, server: Server) {
    this.config = config;
    this.server = server;
  }

  async start(): Promise<void> {
    this.connectWebSocket();
  }

  async stop(): Promise<void> {
    this.stopped = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.ws?.close();
    this.ws = null;
    for (const [id, entry] of this.pending) {
      entry.reject(new Error(`pending request ${String(id)} cancelled during shutdown`));
    }
    this.pending.clear();
  }

  async call(method: string, params: unknown[]): Promise<unknown> {
    return this.callViaWs(method, params);
  }

  async getSyncStatus(): Promise<unknown> {
    const response = await fetch(this.config.synczUrl, {
      method: "GET",
      headers: this.config.authToken
        ? { Authorization: `Bearer ${this.config.authToken}` }
        : undefined,
    });

    let body: unknown = null;
    try {
      body = await response.json();
    } catch {}

    return {
      ok: response.ok,
      status: response.status,
      synczUrl: this.config.synczUrl,
      body,
    };
  }

  private scheduleReconnect(reason: string): void {
    if (this.stopped || this.reconnectTimer) return;
    process.stderr.write(`bookmarks channel: reconnecting after ${reason}\n`);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connectWebSocket();
    }, 2000);
    this.reconnectTimer.unref();
  }

  private connectWebSocket(): void {
    const url = new URL(this.config.wsUrl);
    if (this.config.authToken) {
      url.searchParams.set("access_token", this.config.authToken);
    }

    const ws = new WebSocket(url, {
      headers: this.config.authToken
        ? { Authorization: `Bearer ${this.config.authToken}` }
        : undefined,
    });
    this.ws = ws;

    ws.on("open", () => {
      process.stderr.write("bookmarks channel: websocket connected\n");
    });

    ws.on("message", data => {
      void this.handleGatewayPayload(String(data));
    });

    ws.on("close", () => {
      if (this.ws === ws) this.ws = null;
      this.rejectPending("websocket closed");
      this.scheduleReconnect("websocket close");
    });

    ws.on("error", error => {
      process.stderr.write(`bookmarks channel: websocket error: ${error}\n`);
    });
  }

  private rejectPending(reason: string): void {
    for (const [, entry] of this.pending) {
      entry.reject(new Error(reason));
    }
    this.pending.clear();
  }

  private async callViaWs(method: string, params: unknown[]): Promise<unknown> {
    const ws = this.ws;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      throw new Error("websocket transport is not connected");
    }

    const id = this.nextRpcId++;
    const payload: JsonRpcRequest = {
      jsonrpc: "2.0",
      id,
      method,
      params,
    };

    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      ws.send(JSON.stringify(payload), error => {
        if (error) {
          this.pending.delete(id);
          reject(error);
        }
      });
    });
  }

  private async handleGatewayPayload(payloadText: string): Promise<void> {
    const payload = JSON.parse(payloadText) as JsonRpcNotification | RpcResponse;

    if ("id" in payload && payload.id !== undefined && payload.id !== null) {
      const pending = this.pending.get(payload.id);
      if (!pending) return;
      this.pending.delete(payload.id);
      if (isJsonRpcError(payload)) {
        pending.reject(new Error(payload.error.message));
      } else {
        pending.resolve(payload.result);
      }
      return;
    }

    if (!("method" in payload)) return;
    await this.forwardEvent(payload as JsonRpcNotification);
  }

  private async forwardEvent(payload: JsonRpcNotification): Promise<void> {
    if (payload.method === "onImportBegan" || payload.method === "onImportEnded") return;
    const args = payload.params?.args ?? [];
    await this.server.notification({
      method: "notifications/claude/channel",
      params: {
        content: JSON.stringify(
          {
            event: payload.method,
            data: normalizeEventData(payload.method, args),
          },
          null,
          2,
        ),
      },
    });
  }
}

const server = new Server(
  { name: "bookmarks", version: "0.2.0" },
  {
    capabilities: {
      tools: {},
      experimental: {
        "claude/channel": {},
      },
    },
    instructions: [
      "This plugin bridges Chrome Bookmarks Gateway into Claude Code.",
      "",
      "Bookmark change events arrive as <channel> blocks whose body is a JSON object with event and data fields.",
      "Sync status changes also arrive as channel events with event=system.syncStatusChanged and data.status.ok indicating whether synced bookmarks are currently reachable.",
      "Those channel blocks are notifications from the gateway, not direct user chat messages.",
      "",
      "Use the bookmark tools to inspect or mutate bookmarks:",
      "get_tree, get_subtree, get_children, get_recent, get_bookmarks, search_bookmarks, create_bookmark, update_bookmark, move_bookmark, remove_bookmark, remove_bookmark_tree, get_sync_status.",
      "",
      "If you react to an inbound bookmark event, perform the needed bookmark operations through tools. Writing plain transcript text does not send anything back to the gateway.",
      "",
      "The bookmark tools map directly to the Chrome Bookmarks API methods exposed by the gateway.",
    ].join("\n"),
  },
);

function toolResult(result: unknown) {
  return {
    content: [
      {
        type: "text" as const,
        text: JSON.stringify(result, null, 2),
      },
    ],
  };
}

function requireString(value: unknown, name: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${name} must be a non-empty string`);
  }
  return value;
}

function requireNumber(value: unknown, name: string): number {
  if (typeof value !== "number" || Number.isNaN(value)) {
    throw new Error(`${name} must be a number`);
  }
  return value;
}

const config = readConfig();
const bridge = new GatewayBridge(config, server);

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: "get_tree",
      description: "Get the full Chrome bookmarks tree.",
      inputSchema: { type: "object", properties: {} },
    },
    {
      name: "get_subtree",
      description: "Get a bookmark node and all descendants by id.",
      inputSchema: {
        type: "object",
        properties: { id: { type: "string" } },
        required: ["id"],
      },
    },
    {
      name: "get_children",
      description: "Get direct children for a bookmark folder id.",
      inputSchema: {
        type: "object",
        properties: { id: { type: "string" } },
        required: ["id"],
      },
    },
    {
      name: "get_recent",
      description: "Get the most recently added bookmarks.",
      inputSchema: {
        type: "object",
        properties: { count: { type: "number" } },
        required: ["count"],
      },
    },
    {
      name: "get_bookmarks",
      description: "Get one bookmark by id or many bookmarks by ids.",
      inputSchema: {
        type: "object",
        properties: {
          ids: {
            oneOf: [
              { type: "string" },
              { type: "array", items: { type: "string" } },
            ],
          },
        },
        required: ["ids"],
      },
    },
    {
      name: "search_bookmarks",
      description: "Search bookmarks by query string or Chrome search object.",
      inputSchema: {
        type: "object",
        properties: {
          query: {
            oneOf: [{ type: "string" }, { type: "object" }],
          },
        },
        required: ["query"],
      },
    },
    {
      name: "create_bookmark",
      description: "Create a bookmark or folder using the Chrome create payload.",
      inputSchema: {
        type: "object",
        properties: {
          bookmark: { type: "object" },
        },
        required: ["bookmark"],
      },
    },
    {
      name: "update_bookmark",
      description: "Update bookmark title or URL by id.",
      inputSchema: {
        type: "object",
        properties: {
          id: { type: "string" },
          changes: { type: "object" },
        },
        required: ["id", "changes"],
      },
    },
    {
      name: "move_bookmark",
      description: "Move a bookmark to another parent or index.",
      inputSchema: {
        type: "object",
        properties: {
          id: { type: "string" },
          destination: { type: "object" },
        },
        required: ["id", "destination"],
      },
    },
    {
      name: "remove_bookmark",
      description: "Remove a single bookmark node by id.",
      inputSchema: {
        type: "object",
        properties: { id: { type: "string" } },
        required: ["id"],
      },
    },
    {
      name: "remove_bookmark_tree",
      description: "Remove a bookmark node and all descendants by id.",
      inputSchema: {
        type: "object",
        properties: { id: { type: "string" } },
        required: ["id"],
      },
    },
    {
      name: "get_sync_status",
      description: "Check the gateway /syncz endpoint for the current bookmark sync status.",
      inputSchema: { type: "object", properties: {} },
    },
  ],
}));

server.setRequestHandler(CallToolRequestSchema, async req => {
  const args = (req.params.arguments ?? {}) as Record<string, unknown>;

  switch (req.params.name) {
    case "get_tree":
      return toolResult(await bridge.call("getTree", []));
    case "get_subtree":
      return toolResult(await bridge.call("getSubTree", [requireString(args.id, "id")]));
    case "get_children":
      return toolResult(await bridge.call("getChildren", [requireString(args.id, "id")]));
    case "get_recent":
      return toolResult(await bridge.call("getRecent", [requireNumber(args.count, "count")]));
    case "get_bookmarks":
      return toolResult(await bridge.call("get", [args.ids]));
    case "search_bookmarks":
      return toolResult(await bridge.call("search", [args.query]));
    case "create_bookmark":
      return toolResult(await bridge.call("create", [args.bookmark]));
    case "update_bookmark":
      return toolResult(
        await bridge.call("update", [requireString(args.id, "id"), args.changes]),
      );
    case "move_bookmark":
      return toolResult(
        await bridge.call("move", [requireString(args.id, "id"), args.destination]),
      );
    case "remove_bookmark":
      return toolResult(await bridge.call("remove", [requireString(args.id, "id")]));
    case "remove_bookmark_tree":
      return toolResult(await bridge.call("removeTree", [requireString(args.id, "id")]));
    case "get_sync_status":
      return toolResult(await bridge.getSyncStatus());
    default:
      throw new Error(`unknown tool: ${req.params.name}`);
  }
});

process.on("SIGINT", () => {
  void bridge.stop().finally(() => process.exit(0));
});
process.on("SIGTERM", () => {
  void bridge.stop().finally(() => process.exit(0));
});
process.on("unhandledRejection", error => {
  process.stderr.write(`bookmarks channel: unhandled rejection: ${String(error)}\n`);
});
process.on("uncaughtException", error => {
  process.stderr.write(`bookmarks channel: uncaught exception: ${String(error)}\n`);
});

mkdirSync(STATE_DIR, { recursive: true, mode: 0o700 });
if (!existsSync(ENV_FILE)) {
  process.stderr.write(`bookmarks channel: config file not found at ${ENV_FILE}\n`);
}

await server.connect(new StdioServerTransport());
await bridge.start();
