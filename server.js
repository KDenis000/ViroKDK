"use strict";

const http = require("http");
const path = require("path");
const { randomUUID } = require("crypto");
const express = require("express");
const { Server } = require("socket.io");

const PORT = process.env.PORT || 3000;
const NAME_MAX = 24;
const TEXT_MAX = 1000;
const RING_TIMEOUT_MS = 60_000;

const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 1e6 });

app.use(express.static(path.join(__dirname, "public")));

app.get("/healthz", (_req, res) => {
  res.json({ ok: true, online: online.size, calls: calls.size, uptime: process.uptime() });
});

const online = new Map();
const calls = new Map();

function clean(value, max) {
  return String(value ?? "").replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim().slice(0, max);
}

function uniqueName(wanted) {
  const base = clean(wanted, NAME_MAX) || "Guest";
  const taken = new Set([...online.values()].map((u) => u.name.toLowerCase()));
  if (!taken.has(base.toLowerCase())) return base;
  let n = 2;
  while (taken.has(`${base} ${n}`.toLowerCase())) n += 1;
  return `${base} ${n}`;
}

function publicUser(user) {
  return { id: user.id, name: user.name };
}

function roster() {
  return [...online.values()].map(publicUser);
}

function broadcastRoster() {
  io.emit("roster", roster());
}

function callsFor(socketId) {
  return [...calls.values()].filter((c) => c.callerId === socketId || c.calleeId === socketId);
}

function endCall(callId, reason, notifyIds) {
  const call = calls.get(callId);
  if (!call) return;
  calls.delete(callId);
  clearTimeout(call.timeout);
  const ids = notifyIds ?? [call.callerId, call.calleeId];
  for (const id of ids) {
    if (id) io.to(id).emit("call:over", { callId, reason });
  }
}

io.on("connection", (socket) => {
  socket.on("user:join", (payload, ack) => {
    if (online.has(socket.id)) return ack?.({ ok: false, error: "Already joined." });
    const user = { id: socket.id, name: uniqueName(payload?.name), joinedAt: Date.now() };
    online.set(socket.id, user);
    ack?.({ ok: true, user: publicUser(user), users: roster() });
    socket.broadcast.emit("user:joined", publicUser(user));
    broadcastRoster();
  });

  socket.on("chat:message", (payload, ack) => {
    const me = online.get(socket.id);
    if (!me) return ack?.({ ok: false, error: "Join chat first." });
    const text = clean(payload?.text, TEXT_MAX);
    if (!text) return ack?.({ ok: false, error: "Empty message." });

    const message = { id: randomUUID(), from: publicUser(me), text, at: Date.now(), to: payload?.to ? clean(payload.to, 64) : null };
    if (message.to) {
      if (message.to === me.id) return ack?.({ ok: false, error: "Cannot DM yourself." });
      if (!online.has(message.to)) return ack?.({ ok: false, error: "User offline." });
      io.to(message.to).emit("chat:message", message);
      socket.emit("chat:message", message);
    } else {
      io.emit("chat:message", message);
    }
    ack?.({ ok: true, message });
  });

  socket.on("chat:typing", (payload) => {
    const me = online.get(socket.id);
    if (!me) return;
    socket.broadcast.emit("chat:typing", { from: publicUser(me), typing: Boolean(payload?.typing) });
  });

  socket.on("call:invite", (payload, ack) => {
    const me = online.get(socket.id);
    if (!me) return ack?.({ ok: false, error: "Join chat first." });
    const to = clean(payload?.to, 64);
    const media = payload?.media === "video" ? "video" : "audio";
    if (!to || to === socket.id) return ack?.({ ok: false, error: "Invalid target." });

    const target = online.get(to);
    if (!target || callsFor(socket.id).length || callsFor(to).length) {
      return ack?.({ ok: false, error: "User busy or offline." });
    }

    const callId = randomUUID();
    const call = { callId, callerId: socket.id, calleeId: to, media, timeout: setTimeout(() => endCall(callId, "No answer"), RING_TIMEOUT_MS) };
    calls.set(callId, call);

    io.to(to).emit("call:incoming", { callId, media, from: publicUser(me) });
    ack?.({ ok: true, callId });
  });

  socket.on("call:accept", (payload) => {
    const call = calls.get(clean(payload?.callId, 64));
    if (!call || call.calleeId !== socket.id) return;
    clearTimeout(call.timeout);
    io.to(call.callerId).emit("call:accepted", { callId: call.callId, media: call.media, from: publicUser(online.get(socket.id)) });
  });

  socket.on("call:decline", (payload) => {
    const call = calls.get(clean(payload?.callId, 64));
    if (!call) return;
    endCall(call.callId, "Call declined");
  });

  socket.on("call:end", (payload) => {
    const call = calls.get(clean(payload?.callId, 64));
    if (!call) return;
    endCall(call.callId, "Call ended");
  });

  socket.on("webrtc:signal", (payload) => {
    const call = calls.get(clean(payload?.callId, 64));
    if (!call || (socket.id !== call.callerId && socket.id !== call.calleeId)) return;
    const peerId = socket.id === call.callerId ? call.calleeId : call.callerId;
    io.to(peerId).emit("webrtc:signal", { callId: call.callId, from: socket.id, data: payload?.data ?? null });
  });

  socket.on("disconnect", () => {
    const user = online.get(socket.id);
    online.delete(socket.id);
    for (const call of callsFor(socket.id)) {
      endCall(call.callId, "Participant left");
    }
    if (user) {
      socket.broadcast.emit("user:left", publicUser(user));
      broadcastRoster();
    }
  });
});

server.listen(PORT, () => {
  console.log(`ViroKDK + Kchat server running → http://localhost:${PORT}`);
});
