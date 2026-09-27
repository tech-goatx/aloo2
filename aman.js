const fs = require("fs");
const path = require("path");
const http = require("http");
const express = require("express");
const pino = require("pino");
const {
  default: makeWASocket,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
  useMultiFileAuthState,
  DisconnectReason,
  Browsers,
} = require("@whiskeysockets/baileys");

const settings = require("./config");

const logger = pino({ level: "silent" });
const sessions = new Map();
const AUTH_ROOT = path.join(process.cwd(), "auth_info", "web_sessions");

const PAIR_CODES = (Array.isArray(settings.pairCodes) ? settings.pairCodes : [])
  .map((item) => String(item || "").replace(/[^A-Za-z0-9]/g, "").toUpperCase())
  .filter((item) => item.length === 8);

let baileysVersion = [2, 3000, 1027934701];
fetchLatestBaileysVersion()
  .then((fetched) => {
    if (fetched && fetched.version) baileysVersion = fetched.version;
  })
  .catch(() => {});

function pickPairCode() {
  if (!PAIR_CODES.length) return "";
  return PAIR_CODES[Math.floor(Math.random() * PAIR_CODES.length)];
}

function formatPairCode(code) {
  const raw = String(code || "")
    .replace(/[^A-Za-z0-9]/g, "")
    .toUpperCase();
  if (raw.length === 8) return raw.slice(0, 4) + "-" + raw.slice(4);
  return raw || String(code || "");
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, Number(ms) || 0));
}

class BotSession {
  constructor(phoneNumber) {
    this.phoneNumber = String(phoneNumber || "").replace(/[^0-9]/g, "");
    this.userId = this.phoneNumber;
    this.authPath = path.join(AUTH_ROOT, this.phoneNumber);
    this.sock = null;
    this.isConnected = false;
    this.isInitializing = false;
    this.registered = false;
    this.sessionExpired = false;
    this.manualClose = false;
    this.pairingCode = "";
    this.pairingRequested = false;
    this.pairingPromise = null;
    this.reconnectAttempts = 0;
    this.reconnectTimer = null;
    this.healthTimer = null;
    this.keepAliveTimer = null;
    this.lastActive = Date.now();
  }

  sendLog(message) {
    console.log("[" + this.phoneNumber + "] " + String(message));
  }

  socketAlive() {
    const ws = this.sock && this.sock.ws;
    if (!ws) return false;
    return !!(
      ws.isOpen === true ||
      (typeof ws.isOpen === "function" && ws.isOpen()) ||
      ws.readyState === 1 ||
      ws.readyState === "open"
    );
  }

  async waitForWs(ms) {
    const limit = Math.max(200, Number(ms) || 20000);
    const start = Date.now();
    while (Date.now() - start < limit) {
      if (this.sock && typeof this.sock.waitForSocketOpen === "function") {
        try {
          await this.sock.waitForSocketOpen();
          return true;
        } catch (err) {}
      }
      if (this.socketAlive()) return true;
      await delay(120);
    }
    return false;
  }

  async requestPairCode() {
    if (this.pairingCode) return this.pairingCode;
    if (this.pairingPromise) return this.pairingPromise;
    this.pairingRequested = true;
    this.pairingPromise = this.doRequestPairCode().finally(() => {
      this.pairingPromise = null;
    });
    return this.pairingPromise;
  }

  async doRequestPairCode() {
    const phone = this.phoneNumber;
    if (!phone || phone.length < 8) {
      this.pairingRequested = false;
      return "";
    }
    const ready = await this.waitForWs(20000);
    if (!ready) {
      this.pairingRequested = false;
      this.sendLog("pairing failed: socket not open");
      return "";
    }
    await delay(1500);
    const custom = pickPairCode();
    try {
      const code = custom
        ? await this.sock.requestPairingCode(phone, custom)
        : await this.sock.requestPairingCode(phone);
      const formatted = formatPairCode(code || custom);
      if (!formatted) {
        this.pairingRequested = false;
        return "";
      }
      this.pairingCode = formatted;
      this.sendLog("pair code: " + formatted);
      return formatted;
    } catch (err) {
      this.sendLog("custom pair failed: " + err.message);
      try {
        const code = await this.sock.requestPairingCode(phone);
        const formatted = formatPairCode(code);
        if (formatted) {
          this.pairingCode = formatted;
          this.sendLog("pair code: " + formatted);
          return formatted;
        }
      } catch (fallbackErr) {
        this.sendLog("pairing failed: " + fallbackErr.message);
      }
      this.pairingRequested = false;
      this.pairingCode = "";
      return "";
    }
  }

  startHealth() {
    if (this.healthTimer) clearInterval(this.healthTimer);
    this.healthTimer = setInterval(() => {
      try {
        if (this.sock && this.isConnected) {
          this.lastActive = Date.now();
          this.sock.sendPresenceUpdate("available").catch(() => {});
        }
      } catch (err) {}
    }, 60 * 1000);
    if (this.healthTimer.unref) this.healthTimer.unref();
  }

  startKeepAlive() {
    if (this.keepAliveTimer) clearInterval(this.keepAliveTimer);
    this.keepAliveTimer = setInterval(() => {
      this.keepAlive().catch((err) => {
        this.sendLog("keep-alive failed: " + err.message);
      });
    }, 10 * 60 * 1000);
    if (this.keepAliveTimer.unref) this.keepAliveTimer.unref();
  }

  async keepAlive() {
    if (this.manualClose) return;
    if (this.isConnected && this.socketAlive()) {
      this.lastActive = Date.now();
      try {
        await this.sock.sendPresenceUpdate("available");
      } catch (err) {}
      return;
    }
    if (this.registered && !this.isInitializing) {
      this.sendLog("silent restart");
      await this.initialize();
    }
  }

  async initialize() {
    if (this.isInitializing) return;
    if (this.pairingRequested && this.socketAlive() && !this.registered) return;
    this.isInitializing = true;
    this.manualClose = false;
    this.sessionExpired = false;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.sock) {
      try {
        this.sock.end(undefined);
      } catch (err) {}
      this.sock = null;
    }

    try {
      if (!fs.existsSync(this.authPath)) {
        fs.mkdirSync(this.authPath, { recursive: true });
      }

      try {
        const fetched = await fetchLatestBaileysVersion();
        if (fetched && fetched.version) baileysVersion = fetched.version;
      } catch (err) {}

      const { state, saveCreds } = await useMultiFileAuthState(this.authPath);
      this.registered = !!(state.creds && state.creds.registered);
      if (!this.registered && state.creds && state.creds.pairingCode) {
        this.pairingCode = formatPairCode(state.creds.pairingCode);
        this.pairingRequested = true;
      }

      const sock = makeWASocket({
        version: baileysVersion,
        logger,
        auth: {
          creds: state.creds,
          keys: makeCacheableSignalKeyStore(state.keys, logger),
        },
        syncFullHistory: false,
        browser: Browsers.macOS("Safari"),
        printQRInTerminal: false,
        markOnlineOnConnect: true,
        keepAliveIntervalMs: 15000,
        connectTimeoutMs: 45000,
        defaultQueryTimeoutMs: 30000,
      });

      this.sock = sock;
      sock.ev.on("creds.update", saveCreds);

      sock.ev.on("connection.update", async (update) => {
        try {
          await this.onConnectionUpdate(update);
        } catch (err) {
          this.sendLog("connection.update error: " + err.message);
        }
      });

      sock.ev.on("messages.upsert", async (upsert) => {
        try {
          if (!upsert || !Array.isArray(upsert.messages)) return;
          this.lastActive = Date.now();
        } catch (err) {}
      });

      if (this.registered) {
        this.startHealth();
        this.startKeepAlive();
      }
    } catch (err) {
      this.sendLog("initialize failed: " + err.message);
      this.isInitializing = false;
      if (this.registered) this.scheduleReconnect();
      return;
    }
    this.isInitializing = false;
  }

  async onConnectionUpdate(update) {
    const { connection, lastDisconnect, qr } = update;
    if (qr && !this.pairingCode) this.sendLog("qr received, use pair code");

    if (
      !this.registered &&
      this.phoneNumber &&
      !this.pairingRequested &&
      !this.pairingCode &&
      (connection === "connecting" || qr || this.socketAlive())
    ) {
      this.requestPairCode().catch((err) => {
        this.sendLog("pair request failed: " + err.message);
      });
    }

    if (connection === "open") {
      this.isConnected = true;
      this.registered = true;
      this.sessionExpired = false;
      this.reconnectAttempts = 0;
      this.lastActive = Date.now();
      this.pairingCode = "";
      this.pairingRequested = false;
      this.startHealth();
      this.startKeepAlive();
      try {
        await this.sock.sendPresenceUpdate("available");
      } catch (err) {}
      this.sendLog("connected");
    }

    if (connection === "close") {
      this.isConnected = false;
      const status =
        lastDisconnect &&
        lastDisconnect.error &&
        lastDisconnect.error.output &&
        lastDisconnect.error.output.statusCode;
      const loggedOut = status === DisconnectReason.loggedOut || status === 401;

      if (this.manualClose) {
        this.sendLog("session closed");
        return;
      }
      if (loggedOut || status === 403) {
        this.sessionExpired = true;
        this.registered = false;
        this.sendLog("session expired");
        return;
      }

      const credsRegistered = !!(
        this.sock &&
        this.sock.authState &&
        this.sock.authState.creds &&
        this.sock.authState.creds.registered
      );
      if (credsRegistered) this.registered = true;

      if (!this.registered) {
        this.sendLog("pair socket closed" + (status ? " (" + status + ")" : ""));
        this.reconnectTimer = setTimeout(() => {
          this.initialize().catch((err) => {
            this.sendLog("pair retry failed: " + err.message);
          });
        }, 1200);
        return;
      }

      this.scheduleReconnect();
    }
  }

  scheduleReconnect() {
    if (this.manualClose || this.sessionExpired || this.reconnectTimer) return;
    this.reconnectAttempts += 1;
    const wait = Math.min(30000, 2000 * this.reconnectAttempts);
    this.sendLog("reconnect in " + wait + "ms");
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.initialize().catch((err) => {
        this.sendLog("reconnect failed: " + err.message);
      });
    }, wait);
  }

  async close(manual) {
    this.manualClose = !!manual;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    if (this.healthTimer) clearInterval(this.healthTimer);
    if (this.keepAliveTimer) clearInterval(this.keepAliveTimer);
    this.reconnectTimer = null;
    this.healthTimer = null;
    this.keepAliveTimer = null;
    if (this.sock) {
      try {
        this.sock.end(undefined);
      } catch (err) {}
    }
    this.sock = null;
    this.isConnected = false;
  }
}

async function startPair(number) {
  const phone = String(number || "").replace(/[^0-9]/g, "");
  if (!phone || phone.length < 8) {
    const err = new Error("invalid number");
    err.status = 400;
    throw err;
  }
  let session = sessions.get(phone);
  if (!session) {
    session = new BotSession(phone);
    sessions.set(phone, session);
  }
  if (session.isConnected) return session;
  if (session.pairingCode) return session;
  if (session.sessionExpired) {
    session.sessionExpired = false;
    session.pairingRequested = false;
    session.pairingCode = "";
    session.pairingPromise = null;
    session.registered = false;
  }
  if (!session.isInitializing && !session.pairingRequested) {
    session.initialize().catch((err) => {
      console.error("[pair] init failed:", err.message);
    });
  }
  const deadline = Date.now() + 25000;
  while (!session.pairingCode && Date.now() < deadline) {
    await delay(250);
  }
  return session;
}

function loadExistingSessions() {
  try {
    if (!fs.existsSync(AUTH_ROOT)) {
      fs.mkdirSync(AUTH_ROOT, { recursive: true });
      return;
    }
    const folders = fs.readdirSync(AUTH_ROOT, { withFileTypes: true });
    for (const entry of folders) {
      if (!entry.isDirectory()) continue;
      const phone = entry.name.replace(/[^0-9]/g, "");
      if (!phone) continue;
      const credsPath = path.join(AUTH_ROOT, entry.name, "creds.json");
      if (!fs.existsSync(credsPath)) continue;
      let registered = false;
      try {
        const parsed = JSON.parse(fs.readFileSync(credsPath, "utf8"));
        registered = !!(parsed && parsed.registered);
      } catch (err) {}
      if (!registered) {
        console.log("[aman] skip unpaired leftover session:", phone);
        continue;
      }
      if (sessions.has(phone)) continue;
      const session = new BotSession(phone);
      session.registered = true;
      sessions.set(phone, session);
      session.sendLog("loading saved session");
      session.initialize().catch((err) => {
        session.sendLog("load failed: " + err.message);
      });
    }
  } catch (err) {
    console.error("[aman] loadExistingSessions failed:", err.message);
  }
}

function startHttp() {
  const app = express();
  app.use(express.json());
  app.use(express.urlencoded({ extended: true }));
  const publicDir = path.join(process.cwd(), "public");
  app.use(express.static(publicDir));

  app.get("/", (req, res) => {
    const html = path.join(publicDir, "index.html");
    if (fs.existsSync(html)) return res.sendFile(html);
    res.status(200).send("AMAN MD pair");
  });

  app.get("/pair", (req, res) => {
    const html = path.join(publicDir, "index.html");
    if (fs.existsSync(html)) return res.sendFile(html);
    res.status(200).send("AMAN MD pair");
  });

  app.get("/info", (req, res) => {
    res.json({
      botName: settings.botName || "",
      ownerName: settings.ownerName || "",
      version: settings.version || "",
      prefix: settings.prefix || ".",
      connected: Array.from(sessions.values()).filter((s) => s.isConnected).length,
    });
  });

  app.get("/code", async (req, res) => {
    try {
      const number = String(req.query.number || req.query.phone || "").replace(/[^0-9]/g, "");
      const session = await startPair(number);
      const code = session.pairingCode || "";
      if (!code) {
        return res.status(202).json({
          success: false,
          ok: false,
          error: "pairing code still generating",
          number,
        });
      }
      return res.json({ success: true, ok: true, code, number });
    } catch (err) {
      console.error("[http] /code failed:", err.message);
      return res.status(err.status || 500).json({
        success: false,
        ok: false,
        error: err.message,
      });
    }
  });

  const port = Number(process.env.PORT || settings.port || 8000);
  const server = http.createServer(app);
  server.listen(port, "0.0.0.0", () => {
    console.log("[http] listening on", port);
  });
  return server;
}

function main() {
  startHttp();
  loadExistingSessions();
  process.on("unhandledRejection", (err) => {
    console.error("[process] unhandledRejection:", err && err.message ? err.message : err);
  });
  process.on("uncaughtException", (err) => {
    console.error("[process] uncaughtException:", err && err.message ? err.message : err);
  });
}

main();

module.exports = { BotSession, sessions };
