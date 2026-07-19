// ============================================================
// WhatsApp Web Clone – Baileys Edition (Production Grade)
// Library: @whiskeysockets/baileys (NOT whatsapp-web.js)
// ============================================================

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const BaileysLib = require('@whiskeysockets/baileys');
const makeWASocket = BaileysLib.default;
const {
    useMultiFileAuthState,
    DisconnectReason,
    fetchLatestBaileysVersion,
    jidNormalizedUser,
    downloadMediaMessage,
    getContentType,
    proto,
    generateWAMessageFromContent,
    prepareWAMessageMedia,
    isJidGroup,
    isJidBroadcast,
} = BaileysLib;

// ── HELPERS ───────────────────────────────────────────────
function toNum(t) {
    if (typeof t === 'number') return t;
    if (t && typeof t === 'object') {
        if (typeof t.toNumber === 'function') return t.toNumber();
        if ('low' in t) return t.low;
    }
    return 0;
}

function toTimestamp(ts) {
    if (!ts) return Math.floor(Date.now() / 1000);
    if (typeof ts === 'number') return ts;
    if (typeof ts === 'object' && ts.low !== undefined) return ts.low; // protobuf Long
    if (typeof ts.toNumber === 'function') return ts.toNumber();
    return parseInt(ts) || Math.floor(Date.now() / 1000);
}

// ── ROBUST STORE IMPLEMENTATION ───────────────────────────
class SimpleStore {
    constructor() {
        this.chats = {
            _data: new Map(),
            all: () => Array.from(this.chats._data.values()),
            get: (id) => this.chats._data.get(id),
            set: (id, c) => this.chats._data.set(id, { ...this.chats._data.get(id), ...c }),
            upsert: (chats) => chats.forEach(c => this.chats.set(c.id, c))
        };
        this.contacts = {};
        this.messages = {};
    }
    bind(ev) {
        ev.on('chats.upsert', (chats) => this.chats.upsert(chats));
        ev.on('chats.update', (updates) => {
            for (const u of updates) if (u.id) this.chats.set(u.id, u);
        });
        ev.on('messaging-history.set', ({ chats, contacts, messages }) => {
            if(chats) chats.forEach(c => this.chats.set(c.id, c));
            if(contacts) contacts.forEach(c => { if(c.id) this.contacts[c.id] = { ...(this.contacts[c.id]||{}), ...c }; });
            if(messages) messages.forEach(msg => {
                const jid = msg.key.remoteJid;
                if(jid) {
                    if(!this.messages[jid]) this.messages[jid] = new Map();
                    this.messages[jid].set(msg.key.id, msg);
                }
            });
        });
        ev.on('contacts.upsert', (contacts) => {
            contacts.forEach(c => { if(c.id) this.contacts[c.id] = { ...(this.contacts[c.id]||{}), ...c }; });
        });
        ev.on('messages.upsert', ({ messages, type }) => {
            for (const msg of messages) {
                const jid = msg.key.remoteJid;
                if (!jid) continue;
                if(!this.messages[jid]) this.messages[jid] = new Map();
                this.messages[jid].set(msg.key.id, msg);

                const chat = this.chats.get(jid) || { id: jid };
                const body = (typeof getMsgBody === 'function') ? getMsgBody(msg) : (msg.message?.conversation || 'Message');
                this.chats.set(jid, { 
                    ...chat, 
                    lastMessage: { body }, 
                    conversationTimestamp: msg.messageTimestamp,
                    unreadCount: (chat.unreadCount||0) + (msg.key.fromMe ? 0 : 1)
                });
            }
        });
    }
    readFromFile(f) {
        try { if(fs.existsSync(f)) { const d=JSON.parse(fs.readFileSync(f)); d.chats.forEach(c=>this.chats.set(c.id,c)); this.contacts=d.contacts||{}; } } catch(e){}
    }
    writeToFile(f) {
        try { fs.writeFileSync(f, JSON.stringify({ chats: this.chats.all(), contacts: this.contacts })); } catch(e){}
    }
}

let makeInMemoryStore = BaileysLib.makeInMemoryStore;
if (typeof makeInMemoryStore !== 'function') {
    console.log('Using custom SimpleStore implementation');
    makeInMemoryStore = () => new SimpleStore();
}

const { Boom } = require('@hapi/boom');
const P = require('pino');
const qrcode = require('qrcode');
const fs = require('fs');
const path = require('path');
const supa = require('./supabase/sync');
const access = require('./access');

const PORT = parseInt(process.env.PORT || '3001', 10);
const PUBLIC_URL = process.env.PUBLIC_URL || ''; // e.g. https://cx.fob.net.ng — used for the share-access QR
const DATA_DIR = process.env.DATA_DIR || '.';          // Render persistent disk mount (e.g. /data)
const SESSION_DIR = DATA_DIR + '/session';
const CACHE_DIR = DATA_DIR + '/cache';
const SCHEDULE_FILE = DATA_DIR + '/scheduled.json';
const CALL_BLOCK_FILE = DATA_DIR + '/call_block.json';

const app = express();
const server = http.createServer(app);
app.use('/client', express.static(path.join(__dirname, 'public')));
// Serve the client at the site root too (same origin, no Vercel needed):
app.get('/app', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

if (!fs.existsSync(CACHE_DIR)) fs.mkdirSync(CACHE_DIR, { recursive: true });
if (!fs.existsSync(SESSION_DIR)) fs.mkdirSync(SESSION_DIR, { recursive: true });

const io = new Server(server, {
    cors: { origin: '*' },
    pingTimeout: 60000,
    pingInterval: 25000,
    maxHttpBufferSize: 1e8
});

// ── CONTACTS & CACHE ─────────────────────────────────────
const CONTACTS_FILE = DATA_DIR + '/contacts.json';
let customNames = {};
try { customNames = JSON.parse(fs.readFileSync(CONTACTS_FILE, 'utf8')); } catch {}

const CONTACT_CACHE_FILE = DATA_DIR + '/contact_cache.json';
const contactCache = new Map();
try {
    if (fs.existsSync(CONTACT_CACHE_FILE)) {
        const data = JSON.parse(fs.readFileSync(CONTACT_CACHE_FILE, 'utf8'));
        Object.entries(data).forEach(([k, v]) => contactCache.set(k, v));
    }
} catch {}
function saveContactCache() {
    try { fs.writeFileSync(CONTACT_CACHE_FILE, JSON.stringify(Object.fromEntries(contactCache), null, 2)); } catch {}
}

// ── BAILEYS STORE ─────────────────────────────────────────
const STORE_FILE = DATA_DIR + '/baileys_store.json';
const store = makeInMemoryStore({ logger: P({ level: 'silent' }) });
try { if (fs.existsSync(STORE_FILE)) store.readFromFile(STORE_FILE); } catch {}
setInterval(() => { try { store.writeToFile(STORE_FILE); } catch {} }, 10000);

// ── PROFILE PIC QUEUE ─────────────────────────────────────
const picQueue = [];
let isProcessingQueue = false;

async function processPicQueue() {
    if (isProcessingQueue || picQueue.length === 0 || !sock) return;
    isProcessingQueue = true;
    const CONCURRENCY = 8;            // fetch many pics in parallel instead of 1 every 3-6s
    while (picQueue.length > 0) {
        const now = Date.now();
        // Pull a batch, skipping jids that are fresh or recently failed.
        const batch = [];
        while (batch.length < CONCURRENCY && picQueue.length > 0) {
            const jid = picQueue.shift();
            const cached = contactCache.get(jid);
            if (cached?.picUrl && now - (cached.lastFetched || 0) < 172800000) continue;
            if (cached?.lastFailed && now - cached.lastFailed < 3600000) continue;
            batch.push(jid);
        }
        if (!batch.length) break;
        await Promise.all(batch.map(async (jid) => {
            const t = Date.now();
            try {
                const url = await sock.profilePictureUrl(jid, 'image').catch(() => null);
                const prev = contactCache.get(jid) || {};
                contactCache.set(jid, {
                    ...prev,
                    picUrl: url || prev.picUrl || null,
                    lastFetched: url ? t : prev.lastFetched,
                    lastFailed: url ? null : t
                });
                saveContactCache();
                if (url) io.emit('chat_metadata', { chatId: jid, url });
            } catch {
                const prev = contactCache.get(jid) || {};
                contactCache.set(jid, { ...prev, lastFailed: Date.now() });
            }
        }));
        // tiny stagger so we don't hammer the socket
        await new Promise(r => setTimeout(r, 400));
    }
    isProcessingQueue = false;
}

// ── CALL BLOCK ────────────────────────────────────────────
let blockedCalls = new Set();
try { blockedCalls = new Set(JSON.parse(fs.readFileSync(CALL_BLOCK_FILE, 'utf8'))); } catch {}
function saveCallBlock() { fs.writeFileSync(CALL_BLOCK_FILE, JSON.stringify([...blockedCalls], null, 2)); }

// ── SCHEDULER ─────────────────────────────────────────────
let scheduledTasks = [];
try {
    scheduledTasks = JSON.parse(fs.readFileSync(SCHEDULE_FILE, 'utf8'));
    scheduledTasks.forEach(t => { if (!t.status) t.status = 'pending'; });
} catch {}
function saveSchedule() { fs.writeFileSync(SCHEDULE_FILE, JSON.stringify(scheduledTasks, null, 2)); }

// ── STATE ─────────────────────────────────────────────────
let sock = null;
let isClientReady = false;
let ghostMode = false;
let currentState = 'connecting..';
let lastQR = null;        // most recent QR data URL (re-emitted to late-joining clients)
let needsPairing = false; // true when session is logged out and waiting for a fresh scan
const socketState = new Map();

// ── BAILEYS MESSAGE SERIALIZER ────────────────────────────
function getJidDisplayName(jid) {
    if (!jid) return '';
    const cached = contactCache.get(jid);
    if (cached?.name) return cached.name;
    if (customNames[jid]) return customNames[jid];
    const storeContact = store.contacts[jid];
    if (storeContact?.name) return storeContact.name;
    if (storeContact?.notify) return storeContact.notify;
    return jid.split('@')[0].split(':')[0];
}

function getMsgBody(msg) {
    if (!msg?.message) return '';
    const m = msg.message;
    
    // Unwrap ViewOnce
    if (m.viewOnceMessage?.message) return getMsgBody({ ...msg, message: m.viewOnceMessage.message });
    if (m.viewOnceMessageV2?.message) return getMsgBody({ ...msg, message: m.viewOnceMessageV2.message });

    return m.conversation
        || m.extendedTextMessage?.text
        || m.imageMessage?.caption
        || m.videoMessage?.caption
        || m.documentMessage?.title
        || m.audioMessage && '[Voice Message]'
        || m.stickerMessage && '[Sticker]'
        || m.reactionMessage && `Reacted: ${m.reactionMessage.text}`
        || m.protocolMessage && ''
        || m.ephemeralMessage && getMsgBody({ message: m.ephemeralMessage.message })
        || '';
}

function getMsgType(msg) {
    if (!msg?.message) return 'chat';
    const m = msg.message;
    
    // Unwrap ViewOnce
    if (m.viewOnceMessage?.message) return getMsgType({ ...msg, message: m.viewOnceMessage.message });
    if (m.viewOnceMessageV2?.message) return getMsgType({ ...msg, message: m.viewOnceMessageV2.message });

    if (m.imageMessage) return 'image';
    if (m.videoMessage) return 'video';
    if (m.audioMessage || m.pttMessage) return m.pttMessage ? 'ptt' : 'audio';
    if (m.documentMessage) return 'document';
    if (m.stickerMessage) return 'sticker';
    if (m.reactionMessage) return 'reaction';
    return 'chat';
}

async function downloadAndEncodeMedia(msg) {
    try {
        let m = msg.message;
        let msgToDownload = msg;

        // Unwrap ViewOnce for download
        if (m.viewOnceMessage?.message) { m = m.viewOnceMessage.message; msgToDownload = { ...msg, message: m }; }
        else if (m.viewOnceMessageV2?.message) { m = m.viewOnceMessageV2.message; msgToDownload = { ...msg, message: m }; }

        const type = getContentType(m);
        if (!type || type === 'conversation' || type === 'extendedTextMessage') return { mediaUrl: null, mediaType: null };

        const stream = await downloadMediaMessage(msgToDownload, 'buffer', {}, { logger: P({ level: 'silent' }), reuploadRequest: sock.updateMediaMessage });
        if (!stream) return { mediaUrl: null, mediaType: null };

        let mime = 'application/octet-stream';
        if (m.imageMessage) mime = m.imageMessage.mimetype || 'image/jpeg';
        else if (m.videoMessage) mime = m.videoMessage.mimetype || 'video/mp4';
        else if (m.audioMessage) mime = m.audioMessage.mimetype || 'audio/ogg';
        else if (m.pttMessage) mime = m.pttMessage.mimetype || 'audio/ogg; codecs=opus';
        else if (m.documentMessage) mime = m.documentMessage.mimetype || 'application/octet-stream';

        const b64 = Buffer.isBuffer(stream) ? stream.toString('base64') : Buffer.from(stream).toString('base64');
        return { mediaUrl: `data:${mime};base64,${b64}`, mediaType: mime };
    } catch {
        return { mediaUrl: null, mediaType: null };
    }
}

async function serializeMessage(msg, chatJid) {
    if (!msg?.key) return null;
    const isOutgoing = msg.key.fromMe || false;
    const authorJid = isJidGroup(chatJid)
        ? (msg.key.participant || msg.participant || '')
        : (isOutgoing ? (sock?.user?.id || '') : chatJid);

    // Detect ViewOnce
    const isViewOnce = !!(msg.message?.viewOnceMessage || msg.message?.viewOnceMessageV2);

    const body = getMsgBody(msg);
    const type = getMsgType(msg);

    let mediaUrl = null, mediaType = null;
    const hasMedia = ['image', 'video', 'audio', 'ptt', 'document', 'sticker'].includes(type);
    if (hasMedia) {
        const result = await downloadAndEncodeMedia(msg).catch(() => ({ mediaUrl: null, mediaType: null }));
        mediaUrl = result.mediaUrl;
        mediaType = result.mediaType;
    }

    // Quoted message
    let hasQuotedMsg = false, quotedMsg = null;
    const ctx = msg.message?.extendedTextMessage?.contextInfo
        || msg.message?.imageMessage?.contextInfo
        || msg.message?.videoMessage?.contextInfo;
    if (ctx?.quotedMessage) {
        hasQuotedMsg = true;
        quotedMsg = { body: ctx.quotedMessage.conversation || ctx.quotedMessage.extendedTextMessage?.text || '[media]' };
    }

    const senderName = customNames[authorJid] || getJidDisplayName(authorJid);

    return {
        id: msg.key.id,
        timestamp: toTimestamp(msg.messageTimestamp),
        from: authorJid,
        senderName,
        body,
        type,
        mediaUrl,
        mediaType,
        isOutgoing,
        isStarred: false,
        hasQuotedMsg,
        quotedMsg,
        ack: msg.status || 0,
        chatId: chatJid,
        isViewOnce
    };
}

async function appendBatchToCache(chatId, rawMessages) {
    const cachePath = path.join(CACHE_DIR, `${chatId.replace(/[^a-z0-9]/gi, '_')}.json`);
    try {
        let msgs = [];
        if (fs.existsSync(cachePath)) {
            const data = await fs.promises.readFile(cachePath, 'utf8');
            msgs = JSON.parse(data);
        }
        const existingIds = new Set(msgs.map(m => m.id));
        const serializedBatch = await Promise.all(rawMessages.map(m => serializeMessage(m, chatId)));
        
        for (const s of serializedBatch) {
            if (s && !existingIds.has(s.id)) {
                msgs.push(s);
            }
        }
        msgs.sort((a, b) => a.timestamp - b.timestamp);
        await fs.promises.writeFile(cachePath, JSON.stringify(msgs));
    } catch (e) { console.error('Batch save error:', e.message); }
}

async function appendToCache(chatId, serializedMsg) {
    const cachePath = path.join(CACHE_DIR, `${chatId.replace(/[^a-z0-9]/gi, '_')}.json`);
    try {
        let msgs = [];
        if (fs.existsSync(cachePath)) {
            const data = await fs.promises.readFile(cachePath, 'utf8');
            msgs = JSON.parse(data);
        }
        if (!msgs.some(m => m.id === serializedMsg.id)) {
            msgs.push(serializedMsg);
            // Limit removed to allow fetching ALL history
            await fs.promises.writeFile(cachePath, JSON.stringify(msgs));
        }
    } catch {}
}

function getCachePath(chatId) {
    return path.join(CACHE_DIR, `${chatId.replace(/[^a-z0-9]/gi, '_')}.json`);
}

function sanitizeCachedMessages(msgs) {
    if (!Array.isArray(msgs)) return [];
    return msgs.map(m => {
        let ts = m.timestamp;
        if (typeof ts === 'object' && ts !== null) {
            ts = ts.low || (typeof ts.toNumber === 'function' ? ts.toNumber() : 0) || 0;
        }
        ts = Number(ts) || 0;
        return { ...m, timestamp: ts };
    }).filter(m => m.timestamp > 0);
}

async function loadChatHistory(jid, targetTotal = 100000) {
    if (!sock) return;
    const cachePath = getCachePath(jid);
    let existing = [];
    try {
        if (fs.existsSync(cachePath)) {
            existing = JSON.parse(await fs.promises.readFile(cachePath, 'utf8'));
        }
    } catch {}

    // Merge base map (keeps everything we already have — no 50 cap)
    const msgMap = new Map();
    existing.forEach(m => { if (m && m.id) msgMap.set(m.id, m); });

    const PAGE = 5000;
    let safety = 0;
    while (msgMap.size < targetTotal && safety < 120) {
        safety++;
        try {
            const msgs = await sock.fetchMessageHistory(
                PAGE,
                { key: { remoteJid: jid, fromMe: false, id: 'FETCH_HISTORY' }, messageTimestamp: 0 },
                new Date()
            );
            if (!msgs || msgs.length === 0) break;
            const before = msgMap.size;
            const serialized = await Promise.all(msgs.map(m => serializeMessage(m, jid).catch(() => null)));
            serialized.filter(Boolean).forEach(m => { if (m && m.id) msgMap.set(m.id, m); });
            if (msgMap.size === before) break; // server returned nothing new
            const merged = Array.from(msgMap.values()).sort((a, b) => a.timestamp - b.timestamp);
            await fs.promises.writeFile(cachePath, JSON.stringify(merged)).catch(() => {});
            if (msgs.length < PAGE) break; // fewer than requested => exhausted
            await new Promise(r => setTimeout(r, 200));
        } catch {
            break;
        }
    }
    const merged = Array.from(msgMap.values()).sort((a, b) => a.timestamp - b.timestamp);
    await fs.promises.writeFile(cachePath, JSON.stringify(merged)).catch(() => {});
    console.log(`[backfill] ${jid}: ${merged.length} msgs cached`);
    return merged;
}

// ── BAILEYS CHAT LIST BUILDER ──────────────────────────────
function buildChatList() {
    const chats = store.chats.all() || [];
    return chats
        .filter(c => c.id && !isJidBroadcast(c.id))
        .map(chat => {
            const jid = chat.id;
            const isGroup = isJidGroup(jid);
            const cached = contactCache.get(jid) || {};
            const storeContact = store.contacts[jid] || {};
            const name = customNames[jid] || cached.name || storeContact.name || storeContact.notify || chat.name || jid.split('@')[0];
            
            // 1. Try memory store (sorted)
            const msgsMap = store.messages[jid];
            let lastMsgObj = null;
            if (msgsMap && msgsMap.size > 0) {
                lastMsgObj = Array.from(msgsMap.values())
                    .filter(m => m.messageTimestamp)
                    .sort((a, b) => toNum(a.messageTimestamp) - toNum(b.messageTimestamp))
                    .pop();
            }

            let lastMsg = '', lastMsgType = 'chat', lastMsgTime = 0, lastMsgFromMe = false;

            if (lastMsgObj) {
                lastMsg = getMsgBody(lastMsgObj);
                lastMsgType = getMsgType(lastMsgObj);
                lastMsgTime = toNum(lastMsgObj.messageTimestamp);
                lastMsgFromMe = lastMsgObj.key?.fromMe || false;
            } else {
                // 2. Try disk cache
                try {
                    const cachePath = getCachePath(jid);
                    if (fs.existsSync(cachePath)) {
                        const diskMsgs = JSON.parse(fs.readFileSync(cachePath, 'utf8'));
                        if (diskMsgs.length > 0) {
                            const last = diskMsgs[diskMsgs.length - 1];
                            lastMsg = last.body || (last.mediaUrl ? '[media]' : '');
                            lastMsgType = last.type || 'chat';
                            let ts = last.timestamp;
                            if (typeof ts === 'object' && ts !== null) ts = ts.low || 0;
                            lastMsgTime = Number(ts) || 0;
                            lastMsgFromMe = last.isOutgoing || false;
                        }
                    }
                } catch {}
                
                // 3. Fallback to chat object metadata
                if (!lastMsgTime) {
                     lastMsg = chat.lastMessage?.conversation || chat.lastMessage?.extendedTextMessage?.text || '';
                     lastMsgTime = toNum(chat.conversationTimestamp || chat.lastMessageRecvTimestamp || 0);
                }
            }

            return {
                id: jid,
                name,
                isGroup,
                unreadCount: chat.unreadCount || 0,
                lastMsg,
                lastMsgType,
                lastMsgTime,
                lastMsgFromMe,
                isMuted: !!(chat.mute && chat.mute > Date.now() / 1000),
                isPinned: !!(chat.pinned),
                picUrl: cached.picUrl || null
            };
        })
        .sort((a, b) => {
            if (a.isPinned !== b.isPinned) return a.isPinned ? -1 : 1;
            return b.lastMsgTime - a.lastMsgTime;
        });
}

// ── BAILEYS CONNECTION ─────────────────────────────────────
async function connectToBaileys() {
    // Clean up any previous socket instance before reconnecting so we don't
    // leak open connections (e.g. after a logged-out reset loop).
    if (sock) {
        try { sock.ev.removeAllListeners(); } catch {}
        try { sock.ws && sock.ws.close && sock.ws.close(); } catch {}
        try { sock.end && sock.end(new Error('reconnect')); } catch {}
    }
    const { state, saveCreds } = await useMultiFileAuthState(SESSION_DIR);
    const { version } = await fetchLatestBaileysVersion();

    sock = makeWASocket({
        version,
        logger: P({ level: 'silent' }),
        auth: state,
        printQRInTerminal: false,
        browser: ['D4RKAXIS', 'Chrome', '120.0.0'],
        generateHighQualityLinkPreview: false,
        syncFullHistory: true,
        markOnlineOnConnect: false,
        shouldSyncHistoryMessage: () => true,
        getMessage: async (key) => {
            const msgs = store.messages[key.remoteJid];
            if (msgs) {
                const msg = msgs.get(key.id);
                return msg?.message || undefined;
            }
            return undefined;
        }
    });

    store.bind(sock.ev);

    // ── QR CODE ──────────────────────────────────────────
    sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update;

        if (qr) {
            try {
                const qrDataUrl = await qrcode.toDataURL(qr);
                lastQR = qrDataUrl;
                needsPairing = true;
                io.emit('qr', qrDataUrl);
                io.emit('wa_state', 'NEEDS_PAIRING');
                console.log('QR Code generated');
            } catch (e) { console.error('QR gen error:', e); }
        }

        if (connection === 'open') {
            lastQR = null;
            needsPairing = false;
        }

        if (connection === 'close') {
            isClientReady = false;
            const statusCode = lastDisconnect?.error instanceof Boom
                ? lastDisconnect.error.output?.statusCode
                : null;
            const loggedOut = statusCode === DisconnectReason.loggedOut;

            if (loggedOut) {
                // Session was unlinked / token expired server-side. The device can no
                // longer reconnect with the old creds — we MUST present a fresh QR.
                console.log('Session logged out (401). Resetting auth state and regenerating QR...');
                try {
                    // fs is already required at module top (see line 110).
                    fs.rmSync(SESSION_DIR, { recursive: true, force: true });
                    fs.mkdirSync(SESSION_DIR, { recursive: true });
                } catch (e) { console.error('Auth reset error:', e); }
                currentState = 'NEEDS_PAIRING';
                io.emit('wa_state', 'NEEDS_PAIRING');
                io.emit('wa_logged_out');
                // Reconnect with the wiped state → Baileys will emit a fresh QR.
                setTimeout(() => connectToBaileys(), 1500);
                return;
            }

            currentState = 'DISCONNECTED';
            io.emit('wa_state', 'DISCONNECTED');
            io.emit('wa_disconnected');

            const shouldReconnect = statusCode !== null
                ? statusCode !== DisconnectReason.loggedOut
                : true;

            console.log('Connection closed, reconnect:', shouldReconnect);
            if (shouldReconnect) {
                setTimeout(() => connectToBaileys(), 3000);
            }
        }

        if (connection === 'open') {
            isClientReady = true;
            currentState = 'CONNECTED';
            io.emit('wa_state', 'CONNECTED');
            io.emit('ready');
            console.log('WhatsApp Connected via Baileys ✓');
            console.log('User:', sock.user?.id);
            
            // BACKGROUND: Pre-load FULL message history for all chats (unlimited)
            setTimeout(async () => {
                const chats = store.chats.all() || [];
                console.log(`Pre-loading FULL history for ${chats.length} chats...`);
                for (const chat of chats.slice(0, 50)) { // Top 50 chats
                    const jid = chat.id;
                    if (!jid || isJidBroadcast(jid)) continue;
                    try {
                        await loadChatHistory(jid); // unlimited — fetches ALL history day one
                        await new Promise(r => setTimeout(r, 300)); // Rate limit
                    } catch {}
                }
                console.log('History pre-load complete');
            }, 5000); // Wait 5s after connect
        }
    });

    // ── CREDENTIALS SAVE ─────────────────────────────────
    sock.ev.on('creds.update', saveCreds);

    // ── HISTORY SYNC (Advanced: Fetch all old messages) ──
    sock.ev.on('messaging-history.set', async ({ messages }) => {
        console.log('Received history sync with', messages?.length || 0, 'messages');
        if (!messages) return;
        
        // Batch messages by chat to prevent disk IO lag
        const batches = {};
        for (const msg of messages) {
            if (!msg.key?.remoteJid || msg.key.remoteJid === 'status@broadcast') continue;
            const jid = msg.key.remoteJid;
            if (!batches[jid]) batches[jid] = [];
            batches[jid].push(msg);
        }
        for (const [jid, msgs] of Object.entries(batches)) {
            await appendBatchToCache(jid, msgs);
        }
        io.emit('history_synced', messages.length);
    });

    // ── MESSAGES UPSERT (incoming + outgoing) ────────────
    sock.ev.on('messages.upsert', async ({ messages, type }) => {
        // Allow ALL types (notify, append, history) to ensure we capture everything
        for (const msg of messages) {
            try {
                if (!msg.key?.remoteJid || msg.key.remoteJid === 'status@broadcast') continue;
                const chatJid = msg.key.remoteJid;
                const serialized = await serializeMessage(msg, chatJid);
                if (!serialized || !serialized.body && !serialized.mediaUrl) continue;
                serialized.chatId = chatJid;
                io.emit('new_message', serialized);
                await appendToCache(chatJid, serialized);
                // Supabase sync (no-op if not configured)
                supa.upsertMessage({
                    id: serialized.id, chatJid,
                    fromMe: serialized.isOutgoing, senderJid: serialized.from,
                    body: serialized.body, mediaType: serialized.mediaType,
                    mediaUrl: serialized.mediaUrl, timestamp: serialized.timestamp,
                    isViewOnce: serialized.isViewOnce, raw: msg
                });
                supa.upsertChat(chatJid, {
                    name: getJidDisplayName(chatJid),
                    isGroup: isJidGroup(chatJid),
                    lastBody: serialized.body || (serialized.mediaType ? `[${serialized.mediaType}]` : ''),
                    lastTime: serialized.timestamp,
                    unread: 0
                });
            } catch {}
        }
    });

    // ── MESSAGE STATUS UPDATES (Sent/Delivered/Read) ──────
    sock.ev.on('messages.update', (updates) => {
        for (const update of updates) {
            if (update.update.status) {
                io.emit('msg_ack_change', {
                    id: update.key.id,
                    ack: update.update.status,
                    chatId: update.key.remoteJid
                });
            }
        }
    });

    // ── MESSAGE STATUS (acks) ─────────────────────────────
    sock.ev.on('message-receipt.update', (updates) => {
        if (ghostMode) {
            console.log(`🕵️ Dropped ${updates.length} read receipts (stealth mode)`);
            return;
        }
        for (const update of updates) {
            io.emit('msg_ack_change', {
                id: update.key.id,
                ack: update.receipt.receiptTimestamp ? 3 : 2,
                chatId: update.key.remoteJid
            });
        }
    });

    // ── CONTACTS UPDATE ───────────────────────────────────
    sock.ev.on('contacts.update', (updates) => {
        for (const contact of updates) {
            if (contact.id) {
                const prev = contactCache.get(contact.id) || {};
                if (contact.name || contact.notify) {
                    contactCache.set(contact.id, {
                        ...prev,
                        name: contact.name || contact.notify || prev.name
                    });
                    supa.upsertContact(contact.id, { name: contact.name, notify: contact.notify });
                }
            }
        }
    });

    // ── CALLS ─────────────────────────────────────────────
    sock.ev.on('call', async (calls) => {
        for (const call of calls) {
            if (blockedCalls.has(call.from) && call.status === 'offer') {
                await sock.rejectCall(call.id, call.from).catch(() => {});
            }
        }
    });

    // ── SCHEDULER ─────────────────────────────────────────
    setInterval(async () => {
        if (!isClientReady || !sock) return;
        const now = Date.now();
        const due = scheduledTasks.filter(t => t.status === 'pending' && t.time <= now);
        if (!due.length) return;

        for (const task of due) {
            task.status = 'processing';
            try {
                let shouldSend = true;
                if (task.type === 'conditional') {
                    const msgs = store.messages[task.chatId];
                    if (msgs) {
                        const arr = Array.from(msgs.values()).slice(-30);
                        for (const m of arr) {
                            const isFromTarget = task.conditionalAuthorId
                                ? (m.key.participant || m.key.remoteJid) === task.conditionalAuthorId
                                : !m.key.fromMe;
                            if (isFromTarget && (m.messageTimestamp * 1000) > task.createdAt) {
                                shouldSend = false;
                                break;
                            }
                        }
                    }
                }
                if (shouldSend) {
                    await sock.sendMessage(task.chatId, { text: task.text });
                    task.status = 'sent';
                    task.sentAt = Date.now();
                } else {
                    task.status = 'skipped';
                }
            } catch (e) { task.status = 'failed'; task.error = e.message; }
        }
        saveSchedule();
        io.emit('schedule_updated', scheduledTasks);
    }, 3000);

    return sock;
}

// ── HTTP ROUTES ───────────────────────────────────────────

app.get('/', (req, res) => res.send(HTML));

app.get('/auth/callback', (req, res) => {
    const code = req.query.code;
    if (code) {
        io.emit('graph_auth_code', code);
        res.send('<html><body style="font-family:sans-serif;text-align:center;padding:60px"><h2 style="color:#0078d4">✓ Done! Close this tab.</h2><script>window.close()</script></body></html>');
    } else {
        res.send('Error: ' + (req.query.error_description || req.query.error || 'Unknown'));
    }
});

// ── CLEAR LOCAL + SUPABASE, THEN RESYNC ─────────────────
// Wipes local store/cache and (if configured) Supabase tables, then reconnects
// to generate a fresh QR so the phone re-pushes the FULL history.
async function doClearResync() {
    let supabase = false;
    if (supa.isEnabled()) {
        const ok = await supa.clearAll();
        await supa.flush();
        if (!ok) throw new Error('supabase clear failed');
        supabase = true;
    }
    // Clear local state (keep the WhatsApp session so we don't have to re-scan)
    [STORE_FILE, CONTACT_CACHE_FILE].forEach(f => { try { fs.unlinkSync(f); } catch {} });
    try { fs.rmSync(CACHE_DIR, { recursive: true, force: true }); } catch {}
    try { fs.mkdirSync(CACHE_DIR); } catch {}
    contactCache.clear();
    // Force a fresh full re-sync: drop the WhatsApp session and reconnect (new QR).
    try { fs.rmSync(SESSION_DIR, { recursive: true, force: true }); } catch {}
    try { fs.mkdirSync(SESSION_DIR); } catch {}
    isClientReady = false;
    needsPairing = true;
    currentState = 'NEEDS_PAIRING';
    if (sock) { try { sock.ev.removeAllListeners(); sock.end && sock.end(); } catch {} sock = null; }
    connectToBaileys();
    return { ok: true, supabase, message: 'Cleared. Scan the new QR to resync.' };
}

app.post('/api/clear-and-resync', async (req, res) => {
    try {
        const r = await doClearResync();
        res.json(r);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ── SHARE ACCESS QR (head's own client link) ───────────
// Returns a QR encoding the public web-client URL (same origin here, so it just
// points at /app). Anyone who scans opens the web client connected to THIS WA.
app.get('/api/share-qr', async (req, res) => {
    const key = req.headers['x-admin-key'] || req.query.key || '';
    if (!access.isHead(key)) return res.status(403).json({ error: 'head key required' });
    try {
        const base = (PUBLIC_URL || (req.protocol + '://' + req.get('host'))).replace(/\/$/, '');
        const shareUrl = base + '/app';
        const dataUrl = await qrcode.toDataURL(shareUrl, { width: 320, margin: 1 });
        res.json({ ok: true, url: shareUrl, qr: dataUrl });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ── INVITE QR (head generates a single-use device invite) ─
// Protected by ADMIN_KEY header (the head's key). Returns a QR encoding an
// invite code the scanning device redeems with `redeem_invite`.
app.get('/api/invite-qr', async (req, res) => {
    const key = req.headers['x-admin-key'] || req.query.key || '';
    if (!access.isHead(key)) return res.status(403).json({ error: 'head key required' });
    try {
        const code = access.createInvite('head');
        // The client encodes this as a link the device opens to redeem.
        const base = (PUBLIC_URL || (req.protocol + '://' + req.get('host'))).replace(/\/$/, '');
        const inviteUrl = base + '/app?invite=' + code;
        const dataUrl = await qrcode.toDataURL(inviteUrl, { width: 320, margin: 1 });
        res.json({ ok: true, code, inviteUrl, qr: dataUrl });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ── DEVICE REGISTRY (head management) ─────────────────────
app.get('/api/devices', (req, res) => {
    const key = req.headers['x-admin-key'] || req.query.key || '';
    if (!access.isHead(key)) return res.status(403).json({ error: 'head key required' });
    res.json({ ok: true, devices: access.listDevices(), headKey: access.HEAD_KEY });
});
app.post('/api/devices/:id/approve', (req, res) => {
    const key = req.headers['x-admin-key'] || req.query.key || '';
    if (!access.isHead(key)) return res.status(403).json({ error: 'head key required' });
    res.json({ ok: access.approveDevice(req.params.id) });
});
app.post('/api/devices/:id/remove', (req, res) => {
    const key = req.headers['x-admin-key'] || req.query.key || '';
    if (!access.isHead(key)) return res.status(403).json({ error: 'head key required' });
    res.json({ ok: access.removeDevice(req.params.id) });
});


// ── SOCKET.IO HANDLERS ────────────────────────────────────
io.on('connection', (socket) => {
    console.log('Client connected:', socket.id);
    socketState.set(socket.id, { chatId: null, oldestTimestamp: null, allLoaded: false });

    if (isClientReady) socket.emit('ready');
    socket.emit('wa_state', currentState);
    socket.emit('ghost_status', ghostMode);

    // Re-emit the latest QR to a client that opened the page AFTER the QR was
    // generated (otherwise the QR modal never shows for late visitors).
    if (lastQR) {
        socket.emit('qr', lastQR);
        if (needsPairing) socket.emit('wa_state', 'NEEDS_PAIRING');
    }
    socket.emit('schedule_updated', scheduledTasks);

    socket.on('disconnect', () => socketState.delete(socket.id));
    socket.on('set_active_chat', () => {});

    // ── ACCESS / HEAD-GATEWAY MODEL ──────────────────────
    // The SERVER is the head/gateway. The WhatsApp phone pairs to it (Baileys QR).
    // YOU (the owner) become the head: either by sending ADMIN_KEY, or — on a
    // fresh deploy with no ADMIN_KEY set and zero devices — the first client to
    // call `auth` is auto-promoted to head (bootstrap). After that, only the
    // head key works, and other devices join only via a head-generated invite.
    let deviceId = null, deviceRole = 'guest';

    socket.on('auth', ({ key, name } = {}) => {
        if (access.isHead(key)) {
            deviceId = 'head'; deviceRole = 'head';
            socket.emit('auth_ok', { role: 'head', headKey: access.HEAD_KEY, devices: access.listDevices() });
        } else if (access.isBootstrap()) {
            // First-claim: no admin key set and no devices yet → this device is the owner.
            deviceId = access.makeHead(name || 'Owner');
            deviceRole = 'head';
            socket.emit('auth_ok', { role: 'head', headKey: access.HEAD_KEY, devices: access.listDevices(), bootstrapped: true });
        } else {
            socket.emit('auth_fail', { reason: 'not_head' });
        }
    });

    // Head (or any approved device) mints a single-use invite QR.
    socket.on('create_invite', ({ key, name } = {}) => {
        if (!access.isHead(key)) return socket.emit('auth_fail', { reason: 'head_only' });
        const code = access.createInvite(name || 'head');
        const inviteUrl = (PUBLIC_URL || '') + '/app?invite=' + code;
        socket.emit('invite_created', { code, inviteUrl });
    });

    // A new device redeems an invite → registers (pending head approval unless first).
    socket.on('redeem_invite', ({ code, name } = {}) => {
        const r = access.redeemInvite(code, name);
        if (!r.ok) return socket.emit('auth_fail', { reason: r.reason });
        deviceId = r.id; deviceRole = r.role;
        socket.emit('auth_ok', { role: r.role, approved: r.approved, deviceId: r.id, headKey: null, devices: [] });
        if (r.approved) socket.emit('device_approved', { deviceId: r.id });
    });

    // Head approves / removes a pending device.
    socket.on('approve_device', ({ key, id } = {}) => {
        if (!access.isHead(key)) return socket.emit('auth_fail', { reason: 'head_only' });
        if (access.approveDevice(id)) { io.emit('device_approved', { deviceId: id }); socket.emit('devices', access.listDevices()); }
    });
    socket.on('remove_device', ({ key, id } = {}) => {
        if (!access.isHead(key)) return socket.emit('auth_fail', { reason: 'head_only' });
        if (access.removeDevice(id)) { io.emit('device_removed', { deviceId: id }); socket.emit('devices', access.listDevices()); }
    });
    socket.on('list_devices', ({ key } = {}) => {
        if (!access.isHead(key)) return;
        socket.emit('devices', access.listDevices());
    });

    // Helper: is this socket allowed to read/send?
    const mayUse = () => deviceRole === 'head' || (deviceId && access.canUse(deviceId));

    // ── CLEAR & RESYNC (button) ────────────────────────
    socket.on('clear_and_resync', async (data, ack) => {
        try {
            const r = await doClearResync();
            socket.emit('clear_and_resync_done', r);
            if (typeof ack === 'function') ack(r);
        } catch (e) {
            const r = { ok: false, error: e.message };
            socket.emit('clear_and_resync_done', r);
            if (typeof ack === 'function') ack(r);
        }
    });

    // ── SEND MEDIA (image / video / audio / ptt / document) ──
    // Client reads the File, converts to base64, and emits this.
    socket.on('send_media', async ({ chatId, data, mime, filename, asDocument, ptt }) => {
        if (!mayUse()) return socket.emit('auth_required');
        if (!sock || !chatId || !data) return;
        try {
            const buf = Buffer.from(data.replace(/^data:.*,/, ''), 'base64');
            const type = mime || 'application/octet-stream';
            const isImg = type.startsWith('image/');
            const isVid = type.startsWith('video/');
            const isAud = type.startsWith('audio/');
            const isDoc = asDocument || (!isImg && !isVid && !isAud);
            const opts = {};
            if (isImg) opts.image = buf;
            else if (isVid) opts.video = buf;
            else if (isAud) { opts.audio = buf; opts.ptt = !!ptt; }
            else { opts.document = buf; opts.fileName = filename || 'file'; }
            if (filename && (isImg || isVid)) opts.caption = '';
            await sock.sendMessage(chatId, opts);
        } catch (e) {
            console.error('send_media error:', e.message);
            socket.emit('toast_msg', 'Media send failed: ' + e.message);
        }
    });

    socket.on('fetch_pic', async ({ chatId }) => {
        if (!sock) return;
        try {
            const url = await sock.profilePictureUrl(chatId, 'image').catch(() => null);
            if (url) {
                const prev = contactCache.get(chatId) || {};
                contactCache.set(chatId, { ...prev, picUrl: url, lastFetched: Date.now() });
                saveContactCache();
                io.emit('chat_metadata', { chatId, url });
            }
        } catch {}
    });

    socket.on('fetch_pic_force', async ({ chatId }) => {
        if (!sock) return;
        try {
            const url = await sock.profilePictureUrl(chatId, 'image').catch(() => null);
            const prev = contactCache.get(chatId) || {};
            contactCache.set(chatId, { ...prev, picUrl: url || null, lastFetched: Date.now(), lastFailed: url ? null : Date.now() });
            saveContactCache();
            io.emit('chat_metadata', { chatId, url: url || null });
            socket.emit('toast_msg', url ? 'Picture refreshed ✓' : 'No picture found');
        } catch (e) { console.error('Force pic error:', e); }
    });

    // ── GET CHATS ─────────────────────────────────────────
    socket.on('get_chats', async () => {
        if (!mayUse()) return socket.emit('auth_required');
        try {
            const list = buildChatList();
            socket.emit('chats_list', list);

            // Queue pics for chats missing them (ALL chats, not capped)
            list.forEach(c => {
                if (!c.picUrl && !picQueue.includes(c.id)) picQueue.push(c.id);
            });
            if (picQueue.length > 0) processPicQueue();
        } catch (err) { console.error('get_chats error:', err); }
    });

    // ── OPEN CHAT ─────────────────────────────────────────
    socket.on('open_chat', async ({ chatId }) => {
        if (!mayUse()) return socket.emit('auth_required');
        const state = socketState.get(socket.id);
        if (!state) return;
        state.chatId = chatId;
        state.oldestTimestamp = null;
        state.allLoaded = false;

        // Serve cache immediately
        const cachePath = getCachePath(chatId);
        let cachedMsgs = [];
        try {
            if (fs.existsSync(cachePath)) {
                const data = await fs.promises.readFile(cachePath, 'utf8');
                cachedMsgs = JSON.parse(data);
                cachedMsgs = sanitizeCachedMessages(cachedMsgs);
                cachedMsgs = cachedMsgs.map(m => ({
                    ...m,
                    senderName: customNames[m.from] || m.senderName
                }));
            }
        } catch {}

        // Merge store messages with cache
        const storeMessages = store.messages[chatId];
        let freshMsgs = [];
        if (storeMessages) {
            const arr = Array.from(storeMessages.values());
            const serialized = await Promise.all(
                arr.map(m => serializeMessage(m, chatId).catch(() => null))
            );
            freshMsgs = serialized.filter(Boolean);
        }

        const msgMap = new Map();
        cachedMsgs.forEach(m => msgMap.set(m.id, m));
        freshMsgs.forEach(m => msgMap.set(m.id, m));
        const merged = Array.from(msgMap.values())
            .filter(m => m && m.timestamp)
            .sort((a, b) => a.timestamp - b.timestamp);

        if (merged.length > 0) state.oldestTimestamp = merged[0].timestamp;
        
        // Save merged back to cache
        await fs.promises.writeFile(cachePath, JSON.stringify(merged)).catch(() => {});

        // Group participants
        let participants = [];
        if (isJidGroup(chatId)) {
            try {
                const meta = await sock.groupMetadata(chatId);
                participants = (meta.participants || []).map(p => ({ id: p.id }));
            } catch {}
        }

        socket.emit('chat_opened', { 
            messages: merged, 
            hasMore: false,  // Cache is everything we have
            participants 
        });

        // Fetch contact metadata + pic
        const cached = contactCache.get(chatId);
        if (!cached?.picUrl) {
            if (!picQueue.includes(chatId)) { picQueue.unshift(chatId); processPicQueue(); }
        }
        const storeContact = store.contacts[chatId] || {};
        const name = customNames[chatId] || contactCache.get(chatId)?.name || storeContact.name || storeContact.notify;
        if (name) io.emit('chat_metadata', { chatId, name });
    });

    // ── LOAD OLDER ────────────────────────────────────────
    socket.on('load_older', async ({ chatId }) => {
        const state = socketState.get(socket.id);
        if (!state || state.allLoaded) {
            socket.emit('older_messages', { messages: [], hasMore: false });
            return;
        }

        try {
            // Load from disk cache (older portion)
            const cachePath = getCachePath(chatId);
            let allCached = [];
            try {
                if (fs.existsSync(cachePath)) {
                    allCached = JSON.parse(await fs.promises.readFile(cachePath, 'utf8'));
                }
            } catch {}

            const threshold = state.oldestTimestamp || Infinity;
            const older = allCached
                .filter(m => m.timestamp < threshold)
                .slice(-50);

            if (older.length > 0) {
                state.oldestTimestamp = older[0].timestamp;
                socket.emit('older_messages', { messages: older, hasMore: older.length === 50 });
            } else {
                state.allLoaded = true;
                socket.emit('older_messages', { messages: [], hasMore: false });
            }
        } catch (err) {
            console.error('load_older error:', err);
            socket.emit('older_messages', { messages: [], hasMore: false });
        }
    });

    // ── SEND MESSAGE ──────────────────────────────────────
    socket.on('send_message', async ({ chatId, text, replyTo }) => {
        if (!mayUse()) return socket.emit('auth_required');
        if (!sock) return;
        try {
            const options = { text };
            if (replyTo) {
                const storeMessages = store.messages[chatId];
                if (storeMessages) {
                    const quotedMsg = storeMessages.get(replyTo);
                    if (quotedMsg) options.quoted = quotedMsg;
                }
            }
            await sock.sendMessage(chatId, options);
        } catch (err) { console.error('send error:', err); }
    });

    // ── MARK READ ─────────────────────────────────────────
    socket.on('mark_read', async ({ chatId }) => {
        if (ghostMode || !sock) {
            console.log('🕵️ Ghost mode → skipped read receipt');
            return;
        }
        try {
            const storeMessages = store.messages[chatId];
            if (!storeMessages) return;
            const unread = Array.from(storeMessages.values())
                .filter(m => !m.key.fromMe)
                .slice(-10)
                .map(m => ({
                    remoteJid: chatId,
                    id: m.key.id,
                    fromMe: false,
                    participant: m.key.participant || undefined
                }));
            if (unread.length > 0) {
                await sock.readMessages(unread);
            }
        } catch (e) { console.error('mark_read error:', e); }
    });

    // ── GHOST MODE ────────────────────────────────────────
    socket.on('toggle_ghost', async () => {
        ghostMode = !ghostMode;
        io.emit('ghost_status', ghostMode);

        if (!sock) return;

        try {
            if (ghostMode) {
                await sock.sendPresenceUpdate('unavailable');
                console.log('🕵️ Stealth mode ON — fully invisible');
            } else {
                await sock.sendPresenceUpdate('available');
                console.log('👤 Normal mode — online');
            }
        } catch (e) {
            console.error('Presence update failed:', e.message);
        }
    });

    // ── TYPING ────────────────────────────────────────────
    socket.on('send_typing', async ({ chatId }) => {
        if (ghostMode || !sock || !chatId) return;
        try { await sock.sendPresenceUpdate('composing', chatId); } catch {}
    });
    socket.on('clear_typing', async ({ chatId }) => {
        if (!sock || !chatId) return;
        try { await sock.sendPresenceUpdate('paused', chatId); } catch {}
    });

    // ── SEARCH ────────────────────────────────────────────
    socket.on('search_messages', async ({ chatId, query }) => {
        try {
            const cachePath = getCachePath(chatId);
            let msgs = [];
            if (fs.existsSync(cachePath)) msgs = JSON.parse(await fs.promises.readFile(cachePath, 'utf8'));
            const q = query.toLowerCase();
            const results = msgs.filter(m => (m.body || '').toLowerCase().includes(q)).slice(-50);
            socket.emit('search_results', results);
        } catch { socket.emit('search_results', []); }
    });

    // ── STATS ─────────────────────────────────────────────
    socket.on('get_stats', async ({ chatId }) => {
        try {
            const cachePath = getCachePath(chatId);
            let msgs = [];
            if (fs.existsSync(cachePath)) msgs = JSON.parse(await fs.promises.readFile(cachePath, 'utf8'));
            const senderCounts = {}, hourCounts = new Array(24).fill(0);
            let mediaCount = 0, textCount = 0, totalChars = 0;
            for (const m of msgs) {
                const s = m.senderName || 'Unknown';
                senderCounts[s] = (senderCounts[s] || 0) + 1;
                hourCounts[new Date(m.timestamp * 1000).getHours()]++;
                if (m.mediaUrl) mediaCount++;
                else { textCount++; totalChars += (m.body || '').length; }
            }
            socket.emit('chat_stats', {
                total: msgs.length, mediaCount, textCount,
                avgLength: textCount ? Math.round(totalChars / textCount) : 0,
                senders: Object.entries(senderCounts).sort((a, b) => b[1] - a[1]).slice(0, 10),
                hourly: hourCounts,
            });
        } catch {}
    });

    // ── EXPORT ────────────────────────────────────────────
    socket.on('export_chat', async ({ chatId, format }) => {
        try {
            const cachePath = getCachePath(chatId);
            let msgs = [];
            if (fs.existsSync(cachePath)) msgs = JSON.parse(await fs.promises.readFile(cachePath, 'utf8'));
            const lines = msgs.map(m => {
                const d = new Date(m.timestamp * 1000).toLocaleString();
                const sender = m.senderName || m.from;
                const body = m.body || `[${m.type}]`;
                return format === 'csv'
                    ? `"${d}","${(sender || '').replace(/"/g, '""')}","${body.replace(/"/g, '""')}"`
                    : `[${d}] ${sender}: ${body}`;
            });
            const header = format === 'csv' ? 'Date,Sender,Message\n' : '';
            socket.emit('export_ready', { content: header + lines.join('\n'), format });
        } catch {}
    });

    // ── SYNC CHAT (full, unlimited) ──────────────────────
    socket.on('sync_chat', async ({ chatId }) => {
        socket.emit('toast_msg', 'Syncing full history...');
        await loadChatHistory(chatId); // unlimited — all messages day one
        const cachePath = getCachePath(chatId);
        try {
            const msgs = JSON.parse(await fs.promises.readFile(cachePath, 'utf8'));
            socket.emit('older_messages', { messages: msgs, hasMore: false });
            socket.emit('sync_complete', msgs.length);
        } catch {
            socket.emit('sync_complete', 0);
        }
    });

    // ── RENAME CONTACT ────────────────────────────────────
    socket.on('rename_contact', ({ id, name }) => {
        if (name && name.trim()) customNames[id] = name.trim();
        else delete customNames[id];
        fs.writeFileSync(CONTACTS_FILE, JSON.stringify(customNames, null, 2));
        io.emit('names_updated');
    });

    // ── CONTACT INFO ──────────────────────────────────────
    socket.on('get_contact_info', async ({ chatId }) => {
        if (!sock) return;
        try {
            let picUrl = null;
            try { picUrl = await sock.profilePictureUrl(chatId, 'image'); } catch {}
            if (picUrl) {
                const prev = contactCache.get(chatId) || {};
                contactCache.set(chatId, { ...prev, picUrl, lastFetched: Date.now() });
                saveContactCache();
            }

            const storeContact = store.contacts[chatId] || {};
            const cached = contactCache.get(chatId) || {};
            const name = customNames[chatId] || cached.name || storeContact.name || storeContact.notify || chatId.split('@')[0];

            let about = '';
            if (!isJidGroup(chatId)) {
                try {
                    const status = await sock.fetchStatus(chatId);
                    about = status?.status || '';
                } catch {}
            }

            let groupParticipants = 0;
            let description = '';
            if (isJidGroup(chatId)) {
                try {
                    const meta = await sock.groupMetadata(chatId);
                    groupParticipants = meta.participants?.length || 0;
                    description = meta.desc || '';
                } catch {}
            }

            // Get media from cache
            const cachePath = getCachePath(chatId);
            let media = [];
            try {
                if (fs.existsSync(cachePath)) {
                    const msgs = JSON.parse(await fs.promises.readFile(cachePath, 'utf8'));
                    media = msgs
                        .filter(m => m.mediaUrl && (m.mediaType?.startsWith('image/') || m.mediaType?.startsWith('video/')))
                        .slice(-6)
                        .reverse();
                }
            } catch {}

            socket.emit('contact_details', {
                id: chatId,
                name,
                number: chatId.split('@')[0],
                about: isJidGroup(chatId) ? description : about,
                picUrl: picUrl || null,
                isGroup: isJidGroup(chatId),
                media,
                groupParticipants,
            });
        } catch (e) { console.error('Contact info error:', e); }
    });

    // ── CALL BLOCK ────────────────────────────────────────
    socket.on('toggle_call_block', ({ chatId, active }) => {
        if (active) blockedCalls.add(chatId);
        else blockedCalls.delete(chatId);
        saveCallBlock();
    });

    // ── BLOCK CONTACT ─────────────────────────────────────
    socket.on('block_contact', async ({ chatId }) => {
        if (!sock) return;
        try { await sock.updateBlockStatus(chatId, 'block'); } catch {}
    });

    // ── DELETE CHAT ───────────────────────────────────────
    socket.on('delete_chat', async ({ chatId }) => {
        try {
            // Remove local cache
            const cachePath = getCachePath(chatId);
            if (fs.existsSync(cachePath)) fs.unlinkSync(cachePath);
            socket.emit('chat_deleted', chatId);
            socket.emit('get_chats');
        } catch {}
    });

    // ── PIN CHAT ──────────────────────────────────────────
    socket.on('pin_chat', async ({ chatId, pin }) => {
        // Baileys doesn't have direct pin API in all versions; update local
        const chats = store.chats.all();
        const chat = chats.find(c => c.id === chatId);
        if (chat) chat.pinned = pin ? Date.now() / 1000 : null;
        socket.emit('get_chats');
    });

    // ── SCHEDULER ─────────────────────────────────────────
    socket.on('add_schedule', (task) => {
        task.id = Date.now().toString(36) + Math.random().toString(36).substr(2);
        task.createdAt = Date.now();
        task.status = 'pending';
        scheduledTasks.push(task);
        saveSchedule();
        io.emit('schedule_updated', scheduledTasks);
    });

    socket.on('del_schedule', (id) => {
        scheduledTasks = scheduledTasks.filter(t => t.id !== id);
        saveSchedule();
        io.emit('schedule_updated', scheduledTasks);
    });

    // ── LOGOUT ────────────────────────────────────────────
    socket.on('logout_session', async () => {
        console.log('User requested session reset/logout...');
        try {
            if (sock) await sock.logout();
        } catch (e) {
            console.error('Logout failed (might be already closed):', e.message);
        }
        
        // Force clear session to trigger full history re-sync on next scan
        try { 
            fs.rmSync(SESSION_DIR, { recursive: true, force: true }); 
            fs.mkdirSync(SESSION_DIR); 
        } catch (e) { console.error('Failed to clear session dir:', e); }
        
        io.emit('wa_disconnected');
        setTimeout(() => connectToBaileys(), 2000);
    });

    socket.on('get_group_participants', async ({ chatId }) => {
        if (!sock) return;
        try {
            const meta = await sock.groupMetadata(chatId);
            socket.emit('group_participants', {
                chatId,
                participants: (meta.participants || []).map(p => ({ id: p.id }))
            });
        } catch {}
    });

    // ── EMAIL (unchanged from original) ───────────────────
    const GMAIL_TOKEN_FILE  = './gmail_tokens.json';
    const GRAPH_TOKEN_FILE  = './graph_tokens.json';

    // ─── UNIFIED EMAIL HANDLER ────────────────────────────────
    socket.on('email_op', async (op) => {
        const { action, data } = op;
        try {
            // ══ ACCOUNT MANAGEMENT ══════════════════════════════
            if (action === 'get_accounts') {
                const accounts = [];
                // Gmail
                if (fs.existsSync(GMAIL_TOKEN_FILE)) {
                    try {
                        const d = JSON.parse(fs.readFileSync(GMAIL_TOKEN_FILE));
                        if (Array.isArray(d)) {
                            d.forEach(t => accounts.push({ type: 'gmail', email: t.email, id: t.email }));
                        } else {
                            accounts.push({ type: 'gmail', email: d.email || 'Gmail', id: 'gmail' });
                        }
                    } catch {}
                }
                // Graph/Work accounts
                if (fs.existsSync(GRAPH_TOKEN_FILE)) {
                    try {
                        const accs = JSON.parse(fs.readFileSync(GRAPH_TOKEN_FILE));
                        accs.forEach(a => accounts.push({ type: 'graph', email: a.email, id: a.email }));
                    } catch {}
                }
                socket.emit('email_res', { type: 'accounts_list', accounts });
                return;
            }

            if (action === 'remove_account') {
                if (data.type === 'gmail') {
                    let accs = [];
                    try {
                        const d = JSON.parse(fs.readFileSync(GMAIL_TOKEN_FILE));
                        accs = Array.isArray(d) ? d : [d];
                    } catch {}
                    accs = accs.filter(a => a.email !== data.email);
                    fs.writeFileSync(GMAIL_TOKEN_FILE, JSON.stringify(accs));
                } else {
                    let accs = [];
                    try { accs = JSON.parse(fs.readFileSync(GRAPH_TOKEN_FILE)); } catch {}
                    accs = accs.filter(a => a.email !== data.email);
                    fs.writeFileSync(GRAPH_TOKEN_FILE, JSON.stringify(accs));
                }
                socket.emit('email_res', { type: 'account_removed' });
                return;
            }

            // ══ MICROSOFT GRAPH (Work Email) ════════════════════
            // Tenant ID from your setup
            const AZURE_AUTHORITY = 'https://login.microsoftonline.com/930b0753-2281-4653-83c9-9c853e9d5405';
            
            if (action === 'graph_get_auth_url') {
                const msal = require('@azure/msal-node');
                const cfg = { auth: { clientId: data.clientId, clientSecret: data.clientSecret, authority: AZURE_AUTHORITY } };
                const pca = new msal.ConfidentialClientApplication(cfg);
                const url = await pca.getAuthCodeUrl({
                    scopes: ['Mail.Read','Mail.Send','Mail.ReadWrite','offline_access','User.Read'],
                    redirectUri: 'http://localhost:3001/auth/callback'
                });
                // Save client creds temporarily
                fs.writeFileSync('./graph_pending.json', JSON.stringify({ clientId: data.clientId, clientSecret: data.clientSecret }));
                socket.emit('email_res', { type: 'graph_auth_url', url });
                return;
            }

            if (action === 'graph_exchange_code') {
                const msal = require('@azure/msal-node');
                const pending = JSON.parse(fs.readFileSync('./graph_pending.json'));
                const cfg = { auth: { clientId: pending.clientId, clientSecret: pending.clientSecret, authority: AZURE_AUTHORITY } };
                const pca = new msal.ConfidentialClientApplication(cfg);
                const result = await pca.acquireTokenByCode({
                    code: data.code,
                    scopes: ['Mail.Read','Mail.Send','Mail.ReadWrite','offline_access','User.Read'],
                    redirectUri: 'http://localhost:3001/auth/callback'
                });
                // Get user email
                // We can use the fetch API globally available in Node 18+ or install node-fetch. 
                // Assuming node environment supports fetch or polyfilled.
                const userRes = await fetch('https://graph.microsoft.com/v1.0/me', {
                    headers: { Authorization: 'Bearer ' + result.accessToken }
                });
                const user = await userRes.json();
                const email = user.mail || user.userPrincipalName;
                // Save tokens
                let accs = [];
                try { accs = JSON.parse(fs.readFileSync(GRAPH_TOKEN_FILE)); } catch {}
                const existing = accs.findIndex(a => a.email === email);
                const entry = { email, clientId: pending.clientId, clientSecret: pending.clientSecret, accessToken: result.accessToken, account: result.account };
                if (existing >= 0) accs[existing] = entry; else accs.push(entry);
                fs.writeFileSync(GRAPH_TOKEN_FILE, JSON.stringify(accs));
                socket.emit('email_res', { type: 'graph_ready', email });
                return;
            }

            if (action === 'graph_load') {
                let accs = [];
                try { accs = JSON.parse(fs.readFileSync(GRAPH_TOKEN_FILE)); } catch {}
                const acc = accs.find(a => a.email === data.email);
                if (!acc) { socket.emit('email_res', { type: 'no_config' }); return; }

                // Get fresh token
                const msal = require('@azure/msal-node');
                const cfg = { auth: { clientId: acc.clientId, clientSecret: acc.clientSecret, authority: AZURE_AUTHORITY } };
                const pca = new msal.ConfidentialClientApplication(cfg);
                const tokenRes = await pca.acquireTokenSilent({ scopes: ['Mail.Read','Mail.Send','Mail.ReadWrite'], account: acc.account });
                const token = tokenRes?.accessToken || acc.accessToken;

                const CACHE_FILE = `./graph_cache_${data.email}_${data.folder||'inbox'}.json`;
                // Serve cache instantly
                if (fs.existsSync(CACHE_FILE)) {
                    try {
                        const cached = JSON.parse(fs.readFileSync(CACHE_FILE));
                        socket.emit('email_res', { type: 'inbox', msgs: cached.msgs, fromCache: true, label: data.folder || 'INBOX', accountType: 'graph' });
                    } catch {}
                }

                // Map label to Graph folder
                const folderMap = { INBOX:'inbox', SENT:'sentitems', DRAFT:'drafts', TRASH:'deleteditems', SPAM:'junkemail', STARRED:'inbox' };
                const folder = folderMap[data.folder] || 'inbox';
                const isStarred = data.folder === 'STARRED';

                let url = `https://graph.microsoft.com/v1.0/me/mailFolders/${folder}/messages?$top=50&$select=id,subject,from,receivedDateTime,isRead,flag,bodyPreview&$orderby=receivedDateTime desc`;
                if (isStarred) url = `https://graph.microsoft.com/v1.0/me/messages?$top=50&$filter=flag/flagStatus eq 'flagged'&$select=id,subject,from,receivedDateTime,isRead,flag,bodyPreview`;
                if (data.search) url = `https://graph.microsoft.com/v1.0/me/messages?$search="${data.search}"&$top=30&$select=id,subject,from,receivedDateTime,isRead,flag,bodyPreview`;
                if (data.nextLink) url = data.nextLink;

                const res = await fetch(url, { headers: { Authorization: 'Bearer ' + token } });
                const json = await res.json();
                if (json.error) throw new Error(json.error.message);

                const emails = (json.value || []).map(m => ({
                    id: m.id,
                    subject: m.subject || '(no subject)',
                    from: m.from?.emailAddress ? (m.from.emailAddress.name + ' <' + m.from.emailAddress.address + '>') : 'Unknown',
                    date: m.receivedDateTime,
                    snippet: m.bodyPreview || '',
                    unread: !m.isRead,
                    starred: m.flag?.flagStatus === 'flagged',
                    nextLink: json['@odata.nextLink'] || null
                }));

                const nextLink = json['@odata.nextLink'] || null;
                if (!data.nextLink && !data.search) {
                    fs.writeFileSync(CACHE_FILE, JSON.stringify({ msgs: emails, updatedAt: Date.now() }));
                }
                socket.emit('email_res', { type: 'inbox', msgs: emails, nextPageToken: nextLink, fromCache: false, label: data.folder || 'INBOX', accountType: 'graph' });
                return;
            }

            if (action === 'graph_open') {
                let accs = [];
                try { accs = JSON.parse(fs.readFileSync(GRAPH_TOKEN_FILE)); } catch {}
                const acc = accs.find(a => a.email === data.accountEmail);
                if (!acc) throw new Error('Account not found');

                const BODY_CACHE = `./graph_body_${data.id}.json`;
                if (fs.existsSync(BODY_CACHE)) {
                    try {
                        const cached = JSON.parse(fs.readFileSync(BODY_CACHE));
                        socket.emit('email_res', { type: 'email_body', ...cached });
                        return;
                    } catch {}
                }

                const msal = require('@azure/msal-node');
                const cfg = { auth: { clientId: acc.clientId, clientSecret: acc.clientSecret, authority: AZURE_AUTHORITY } };
                const pca = new msal.ConfidentialClientApplication(cfg);
                const tokenRes = await pca.acquireTokenSilent({ scopes: ['Mail.Read','Mail.Send','Mail.ReadWrite'], account: acc.account });
                const token = tokenRes?.accessToken || acc.accessToken;

                // Mark as read
                fetch(`https://graph.microsoft.com/v1.0/me/messages/${data.id}`, {
                    method: 'PATCH',
                    headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' },
                    body: JSON.stringify({ isRead: true })
                }).catch(() => {});

                const res = await fetch(`https://graph.microsoft.com/v1.0/me/messages/${data.id}?$select=id,subject,from,receivedDateTime,body,toRecipients`, {
                    headers: { Authorization: 'Bearer ' + token }
                });
                const msg = await res.json();
                if (msg.error) throw new Error(msg.error.message);

                const fromAddr = msg.from?.emailAddress ? (msg.from.emailAddress.name + ' <' + msg.from.emailAddress.address + '>') : 'Unknown';
                const result = {
                    id: msg.id,
                    subject: msg.subject || '(no subject)',
                    from: fromAddr,
                    date: msg.receivedDateTime,
                    body: msg.body?.content || '(No content)',
                    isHtml: msg.body?.contentType === 'html'
                };
                fs.writeFileSync(BODY_CACHE, JSON.stringify(result));
                socket.emit('email_res', { type: 'email_body', ...result });
                return;
            }

            if (action === 'graph_send') {
                let accs = [];
                try { accs = JSON.parse(fs.readFileSync(GRAPH_TOKEN_FILE)); } catch {}
                const acc = accs.find(a => a.email === data.accountEmail);
                if (!acc) throw new Error('Account not found');

                const msal = require('@azure/msal-node');
                const cfg = { auth: { clientId: acc.clientId, clientSecret: acc.clientSecret, authority: AZURE_AUTHORITY } };
                const pca = new msal.ConfidentialClientApplication(cfg);
                const tokenRes = await pca.acquireTokenSilent({ scopes: ['Mail.Send'], account: acc.account });
                const token = tokenRes?.accessToken || acc.accessToken;

                await fetch('https://graph.microsoft.com/v1.0/me/sendMail', {
                    method: 'POST',
                    headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        message: {
                            subject: data.subject || '',
                            body: { contentType: 'Text', content: data.body },
                            toRecipients: [{ emailAddress: { address: data.to } }]
                        }
                    })
                });
                socket.emit('email_res', { type: 'sent' });
                return;
            }

            if (action === 'graph_star') {
                let accs = [];
                try { accs = JSON.parse(fs.readFileSync(GRAPH_TOKEN_FILE)); } catch {}
                const acc = accs.find(a => a.email === data.accountEmail);
                if (!acc) throw new Error('Account not found');
                const msal = require('@azure/msal-node');
                const cfg = { auth: { clientId: acc.clientId, clientSecret: acc.clientSecret, authority: AZURE_AUTHORITY } };
                const pca = new msal.ConfidentialClientApplication(cfg);
                const tokenRes = await pca.acquireTokenSilent({ scopes: ['Mail.ReadWrite'], account: acc.account });
                const token = tokenRes?.accessToken || acc.accessToken;
                await fetch(`https://graph.microsoft.com/v1.0/me/messages/${data.id}`, {
                    method: 'PATCH',
                    headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' },
                    body: JSON.stringify({ flag: { flagStatus: data.starred ? 'flagged' : 'notFlagged' } })
                });
                socket.emit('email_res', { type: 'star_updated', id: data.id, starred: data.starred });
                return;
            }

            if (action === 'graph_trash') {
                let accs = [];
                try { accs = JSON.parse(fs.readFileSync(GRAPH_TOKEN_FILE)); } catch {}
                const acc = accs.find(a => a.email === data.accountEmail);
                if (!acc) throw new Error('Account not found');
                const msal = require('@azure/msal-node');
                const cfg = { auth: { clientId: acc.clientId, clientSecret: acc.clientSecret, authority: AZURE_AUTHORITY } };
                const pca = new msal.ConfidentialClientApplication(cfg);
                const tokenRes = await pca.acquireTokenSilent({ scopes: ['Mail.ReadWrite'], account: acc.account });
                const token = tokenRes?.accessToken || acc.accessToken;
                await fetch(`https://graph.microsoft.com/v1.0/me/messages/${data.id}/move`, {
                    method: 'POST',
                    headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' },
                    body: JSON.stringify({ destinationId: 'deleteditems' })
                });
                socket.emit('email_res', { type: 'trashed', id: data.id });
                return;
            }

            // ══ GMAIL (unchanged) ════════════════════════════════
            const { google } = require('googleapis');

            if (action === 'gmail_auth_url') {
                const oauth2Client = new google.auth.OAuth2(data.clientId, data.clientSecret, 'urn:ietf:wg:oauth:2.0:oob');
                const url = oauth2Client.generateAuthUrl({ access_type:'offline', scope:['https://www.googleapis.com/auth/gmail.readonly','https://www.googleapis.com/auth/gmail.send','https://www.googleapis.com/auth/gmail.modify'] });
                socket.emit('email_res', { type: 'auth_url', url });
            }

            if (action === 'gmail_exchange_code') {
                const oauth2Client = new google.auth.OAuth2(data.clientId, data.clientSecret, 'urn:ietf:wg:oauth:2.0:oob');
                const { tokens } = await oauth2Client.getToken(data.code);
                // Get Gmail address
                oauth2Client.setCredentials(tokens);
                const gmail = google.gmail({ version:'v1', auth:oauth2Client });
                const profile = await gmail.users.getProfile({ userId:'me' });
                const email = profile.data.emailAddress;
                
                const cfg = { clientId:data.clientId, clientSecret:data.clientSecret, tokens, email };
                let accs = [];
                if (fs.existsSync(GMAIL_TOKEN_FILE)) {
                    try {
                        const d = JSON.parse(fs.readFileSync(GMAIL_TOKEN_FILE));
                        accs = Array.isArray(d) ? d : [d];
                    } catch {}
                }
                // Update existing or add new
                accs = accs.filter(a => a.email !== email);
                accs.push(cfg);
                
                fs.writeFileSync(GMAIL_TOKEN_FILE, JSON.stringify(accs));
                socket.emit('email_res', { type: 'gmail_ready', email });
            }

            if (action === 'gmail_load') {
                if (!fs.existsSync(GMAIL_TOKEN_FILE)) { socket.emit('email_res', { type: 'no_config' }); return; }
                let accs = []; try { const d = JSON.parse(fs.readFileSync(GMAIL_TOKEN_FILE)); accs = Array.isArray(d) ? d : [d]; } catch {}
                const cfg = accs.find(a => a.email === data.accountEmail) || accs[0];
                if (!cfg) { socket.emit('email_res', { type:'no_config' }); return; }

                const oauth2Client = new google.auth.OAuth2(cfg.clientId, cfg.clientSecret, 'urn:ietf:wg:oauth:2.0:oob');
                oauth2Client.setCredentials(cfg.tokens);
                oauth2Client.on('tokens', (t) => {
                    cfg.tokens={...cfg.tokens,...t};
                    // Read latest array to avoid race conditions
                    let currentAccs = []; try { const d = JSON.parse(fs.readFileSync(GMAIL_TOKEN_FILE)); currentAccs = Array.isArray(d) ? d : [d]; } catch {}
                    const idx = currentAccs.findIndex(a => a.email === cfg.email);
                    if (idx !== -1) currentAccs[idx] = cfg;
                    fs.writeFileSync(GMAIL_TOKEN_FILE, JSON.stringify(currentAccs)); 
                });
                
                const gmail = google.gmail({ version:'v1', auth:oauth2Client });
                const label = data?.label || 'INBOX';
                const CACHE_FILE = `./gmail_cache_${label}.json`;
                if (fs.existsSync(CACHE_FILE)) {
                    try { const c=JSON.parse(fs.readFileSync(CACHE_FILE)); socket.emit('email_res',{type:'inbox',msgs:c.msgs,fromCache:true,label,accountType:'gmail'}); } catch {}
                }
                const listParams = { userId:'me', maxResults:50, labelIds:[label] };
                if (data?.pageToken) listParams.pageToken = data.pageToken;
                if (data?.query) { listParams.q = data.query; delete listParams.labelIds; }
                const listRes = await gmail.users.messages.list(listParams);
                if (!listRes.data.messages?.length) { socket.emit('email_res',{type:'inbox',msgs:[],label,accountType:'gmail'}); return; }
                let emails = [];
                for (let i=0; i<listRes.data.messages.length; i+=10) {
                    const batch = listRes.data.messages.slice(i,i+10);
                    const results = await Promise.all(batch.map(async m => {
                        try {
                            const msg = await gmail.users.messages.get({userId:'me',id:m.id,format:'metadata',metadataHeaders:['Subject','From','Date']});
                            const h = msg.data.payload.headers, get = n => (h.find(x=>x.name===n)||{}).value||'';
                            return {id:m.id,subject:get('Subject')||'(no subject)',from:get('From')||'Unknown',date:get('Date'),snippet:msg.data.snippet||'',unread:(msg.data.labelIds||[]).includes('UNREAD'),starred:(msg.data.labelIds||[]).includes('STARRED')};
                        } catch { return null; }
                    }));
                    emails = emails.concat(results.filter(Boolean));
                }
                if (!data?.pageToken && !data?.query) fs.writeFileSync(CACHE_FILE, JSON.stringify({msgs:emails,updatedAt:Date.now()}));
                socket.emit('email_res', {type:'inbox',msgs:emails,nextPageToken:listRes.data.nextPageToken||null,fromCache:false,label,accountType:'gmail'});
            }

            if (action === 'gmail_open') {
                let accs = []; try { const d = JSON.parse(fs.readFileSync(GMAIL_TOKEN_FILE)); accs = Array.isArray(d) ? d : [d]; } catch {}
                const cfg = accs.find(a => a.email === data.accountEmail) || accs[0];
                if (!cfg) throw new Error('Account not found');
                const oauth2Client = new google.auth.OAuth2(cfg.clientId, cfg.clientSecret, 'urn:ietf:wg:oauth:2.0:oob');
                oauth2Client.setCredentials(cfg.tokens);
                const gmail = google.gmail({ version:'v1', auth:oauth2Client });
                const BODY_CACHE = `./gmail_body_${data.id}.json`;
                if (fs.existsSync(BODY_CACHE)) { try { socket.emit('email_res',{type:'email_body',...JSON.parse(fs.readFileSync(BODY_CACHE))}); return; } catch {} }
                const msg = await gmail.users.messages.get({userId:'me',id:data.id,format:'full'});
                const headers = msg.data.payload.headers, get = n => (headers.find(h=>h.name===n)||{}).value||'';
                try { await gmail.users.messages.modify({userId:'me',id:data.id,requestBody:{removeLabelIds:['UNREAD']}}); } catch {}
                let body='', isHtml=false;
                const findBody = parts => { if(!parts) return; for(const p of parts){ if(p.mimeType==='text/html'&&p.body?.data){body=Buffer.from(p.body.data,'base64').toString('utf-8');isHtml=true;return;} if(p.parts) findBody(p.parts); } if(!body) for(const p of parts){if(p.mimeType==='text/plain'&&p.body?.data){body=Buffer.from(p.body.data,'base64').toString('utf-8');return;}} };
                if (msg.data.payload.body?.data) body=Buffer.from(msg.data.payload.body.data,'base64').toString('utf-8'); else findBody(msg.data.payload.parts);
                const result = {id:data.id,subject:get('Subject')||'(no subject)',from:get('From')||'Unknown',date:get('Date'),body:body||'(No content)',isHtml};
                fs.writeFileSync(BODY_CACHE, JSON.stringify(result));
                socket.emit('email_res', {type:'email_body',...result});
            }

            if (action === 'gmail_send') {
                let accs = []; try { const d = JSON.parse(fs.readFileSync(GMAIL_TOKEN_FILE)); accs = Array.isArray(d) ? d : [d]; } catch {}
                const cfg = accs.find(a => a.email === data.accountEmail) || accs[0];
                
                const oauth2Client = new google.auth.OAuth2(cfg.clientId, cfg.clientSecret, 'urn:ietf:wg:oauth:2.0:oob');
                oauth2Client.setCredentials(cfg.tokens);
                const gmail = google.gmail({ version:'v1', auth:oauth2Client });
                const raw = Buffer.from(`To: ${data.to}\r\nSubject: ${data.subject||''}\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n${data.body}`).toString('base64url');
                await gmail.users.messages.send({userId:'me',requestBody:{raw}});
                socket.emit('email_res', { type:'sent' });
            }

            if (action === 'gmail_search') {
                if (!fs.existsSync(GMAIL_TOKEN_FILE)) { socket.emit('email_res', {type:'no_config'}); return; }
                let accs = []; try { const d = JSON.parse(fs.readFileSync(GMAIL_TOKEN_FILE)); accs = Array.isArray(d) ? d : [d]; } catch {}
                const cfg = accs.find(a => a.email === data.accountEmail) || accs[0];

                const oauth2Client = new google.auth.OAuth2(cfg.clientId, cfg.clientSecret, 'urn:ietf:wg:oauth:2.0:oob');
                oauth2Client.setCredentials(cfg.tokens);
                const gmail = google.gmail({ version:'v1', auth:oauth2Client });
                const listRes = await gmail.users.messages.list({userId:'me',maxResults:30,q:data.query});
                if (!listRes.data.messages?.length) { socket.emit('email_res',{type:'inbox',msgs:[],label:'SEARCH',accountType:'gmail'}); return; }
                const emails = await Promise.all(listRes.data.messages.map(async m => {
                    try { const msg=await gmail.users.messages.get({userId:'me',id:m.id,format:'metadata',metadataHeaders:['Subject','From','Date']}); const h=msg.data.payload.headers,get=n=>(h.find(x=>x.name===n)||{}).value||''; return {id:m.id,subject:get('Subject')||'(no subject)',from:get('From')||'Unknown',date:get('Date'),snippet:msg.data.snippet||'',unread:(msg.data.labelIds||[]).includes('UNREAD'),starred:(msg.data.labelIds||[]).includes('STARRED')}; } catch { return null; }
                }));
                socket.emit('email_res', {type:'inbox',msgs:emails.filter(Boolean),label:'SEARCH',accountType:'gmail'});
            }

            if (action === 'gmail_toggle_star') {
                let accs = []; try { const d = JSON.parse(fs.readFileSync(GMAIL_TOKEN_FILE)); accs = Array.isArray(d) ? d : [d]; } catch {}
                const cfg = accs.find(a => a.email === data.accountEmail) || accs[0];

                const oauth2Client = new google.auth.OAuth2(cfg.clientId, cfg.clientSecret, 'urn:ietf:wg:oauth:2.0:oob');
                oauth2Client.setCredentials(cfg.tokens);
                const gmail = google.gmail({ version:'v1', auth:oauth2Client });
                if (data.starred) await gmail.users.messages.modify({userId:'me',id:data.id,requestBody:{addLabelIds:['STARRED']}});
                else await gmail.users.messages.modify({userId:'me',id:data.id,requestBody:{removeLabelIds:['STARRED']}});
                socket.emit('email_res', { type:'star_updated', id:data.id, starred:data.starred });
            }

            if (action === 'gmail_trash') {
                let accs = []; try { const d = JSON.parse(fs.readFileSync(GMAIL_TOKEN_FILE)); accs = Array.isArray(d) ? d : [d]; } catch {}
                const cfg = accs.find(a => a.email === data.accountEmail) || accs[0];

                const oauth2Client = new google.auth.OAuth2(cfg.clientId, cfg.clientSecret, 'urn:ietf:wg:oauth:2.0:oob');
                oauth2Client.setCredentials(cfg.tokens);
                const gmail = google.gmail({ version:'v1', auth:oauth2Client });
                await gmail.users.messages.trash({userId:'me',id:data.id});
                socket.emit('email_res', { type:'trashed', id:data.id });
            }

        } catch(e) {
            console.error('Email error:', e.message);
            socket.emit('email_res', { type:'error', error: e.message });
        }
    });

});

connectToBaileys();
server.listen(PORT, () => console.log(`Running at http://localhost:${PORT}`));

// ─────────────────────────────────────────────────────────
// FRONTEND
// ─────────────────────────────────────────────────────────
const HTML = String.raw`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>WhatsApp</title>
<script src="/socket.io/socket.io.js"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --g:#00a884;--gd:#008069;--gl:#d9fdd3;
  --bg:#efeae2;--panel:#fff;--sid:#fff;
  --hdr:#202c33;--hicon:#aebac1;
  --sbg:#f0f2f5;--brdr:#e9edef;
  --bin:#fff;--bout:#d9fdd3;
  --tx:#111b21;--tx2:#667781;
  --unrd:#25d366;--hov:#f0f2f5;--act:#e9edef;
  --shd:rgba(11,20,26,.13);--rply:#f0f2f5;
}
[data-theme=dark]{
  --bg:#0d1418;--panel:#1c2a32;--sid:#111b21;
  --hdr:#202c33;--sbg:#2a3942;--brdr:#2a3942;
  --bin:#202c33;--bout:#005c4b;
  --tx:#e9edef;--tx2:#8696a0;
  --hov:#2a3942;--act:#2a3942;--rply:#2a3942;
  --shd:rgba(0,0,0,.4);
}
html,body{height:100%;overflow:hidden;font-family:'Inter',sans-serif}
body{background:#111b21;display:flex;align-items:center;justify-content:center}
#app{width:100vw;height:100vh;display:flex;background:var(--panel)}

/* SIDEBAR */
#sidebar{width:380px;min-width:280px;max-width:420px;border-right:1px solid var(--brdr);display:flex;flex-direction:column;background:var(--sid)}
.s-hdr{display:flex;align-items:center;justify-content:space-between;padding:10px 16px;background:var(--hdr);height:59px;flex-shrink:0}
.s-hdr-l{display:flex;align-items:center;gap:10px}
.my-av{width:40px;height:40px;border-radius:50%;background:var(--g);display:flex;align-items:center;justify-content:center;color:#fff;font-weight:600;font-size:16px;cursor:pointer;flex-shrink:0}
.app-name{color:#fff;font-size:17px;font-weight:600}
.hdr-btns{display:flex}
.hbtn{background:none;border:none;color:var(--hicon);cursor:pointer;width:40px;height:40px;border-radius:50%;display:flex;align-items:center;justify-content:center;transition:background .15s}
.hbtn:hover{background:rgba(255,255,255,.1)}
.hbtn svg{width:22px;height:22px;fill:var(--hicon)}
.s-search{padding:8px 12px 6px;background:var(--sid);flex-shrink:0}
.s-si{display:flex;align-items:center;background:var(--sbg);border-radius:8px;padding:7px 12px;gap:8px}
.s-si svg{width:17px;height:17px;fill:var(--tx2);flex-shrink:0}
.s-si input{border:none;background:none;outline:none;font-size:14.5px;color:var(--tx);width:100%;font-family:inherit}
.s-si input::placeholder{color:var(--tx2)}
.s-tabs{display:flex;padding:0 8px;gap:0;border-bottom:1px solid var(--brdr);flex-shrink:0;overflow-x:auto}
.s-tabs::-webkit-scrollbar{height:0}
.stab{padding:9px 16px;font-size:13px;color:var(--tx2);cursor:pointer;border-bottom:2px solid transparent;font-weight:500;white-space:nowrap;transition:all .15s;flex-shrink:0}
.stab.on{color:var(--g);border-bottom-color:var(--g)}
.stab:hover:not(.on){color:var(--tx)}
#chats-list{flex:1;overflow-y:auto;overflow-x:hidden}
#chats-list::-webkit-scrollbar{width:5px}
#chats-list::-webkit-scrollbar-thumb{background:rgba(0,0,0,.1);border-radius:3px}
.ci{display:flex;align-items:center;padding:10px 16px;gap:13px;cursor:pointer;border-bottom:1px solid var(--brdr);transition:background .1s}
.ci:hover{background:var(--hov)}
.ci.on{background:var(--act)}
.ci-av{width:49px;height:49px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:20px;color:#fff;font-weight:600;flex-shrink:0;text-transform:uppercase}
.ci-av img{width:100%;height:100%;border-radius:50%;object-fit:cover}
.ci-av.u{background:linear-gradient(135deg,#84a9ac,#5b8a8b)}
.ci-av.g{background:linear-gradient(135deg,#60a080,#3d8a6a)}
.ci-info{flex:1;min-width:0}
.ci-top{display:flex;justify-content:space-between;align-items:center;margin-bottom:3px}
.ci-name{font-size:16px;font-weight:500;color:var(--tx);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:190px}
.ci-time{font-size:11.5px;color:var(--tx2);white-space:nowrap}
.ci-time.u{color:var(--g)}
.ci-bot{display:flex;justify-content:space-between;align-items:center}
.ci-prev{font-size:13.5px;color:var(--tx2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex:1}
.ubadge{background:var(--g);color:#fff;font-size:11px;font-weight:700;min-width:20px;height:20px;border-radius:10px;display:inline-flex;align-items:center;justify-content:center;padding:0 5px;flex-shrink:0}

/* CHAT AREA */
#chat-area{flex:1;display:flex;flex-direction:column;background:var(--bg);position:relative;overflow:hidden}
.wa-bg{position:absolute;inset:0;opacity:.04;background-image:url("data:image/svg+xml,%3Csvg width='60' height='60' viewBox='0 0 60 60' xmlns='http://www.w3.org/2000/svg'%3E%3Cg fill='%23000'%3E%3Ccircle cx='30' cy='30' r='1.5'/%3E%3C/g%3E%3C/svg%3E");pointer-events:none;z-index:0}
#no-chat{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:16px;z-index:1;padding:40px;text-align:center}
.nc-ring{width:200px;height:200px;border-radius:50%;border:1px solid var(--brdr);display:flex;align-items:center;justify-content:center;background:var(--sbg)}
.nc-ring svg{width:80px;height:80px;fill:var(--tx2);opacity:.4}
.nc-h{font-size:32px;font-weight:300;color:var(--tx)}
.nc-p{font-size:14px;color:var(--tx2);line-height:1.7;max-width:380px}
.nc-enc{display:flex;align-items:center;gap:6px;font-size:13px;color:var(--tx2);margin-top:8px}
.nc-enc svg{width:14px;height:14px;fill:var(--g)}
#active-chat{display:none;flex-direction:column;height:100%;position:relative;z-index:1}

/* CHAT HEADER */
.c-hdr{display:flex;align-items:center;padding:9px 16px;gap:12px;background:var(--hdr);height:59px;flex-shrink:0}
.c-hdr-av{width:40px;height:40px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:17px;color:#fff;font-weight:600;text-transform:uppercase;cursor:pointer;flex-shrink:0}
.c-hdr-av img{width:100%;height:100%;border-radius:50%;object-fit:cover}
.c-hdr-av.u{background:linear-gradient(135deg,#84a9ac,#5b8a8b)}
.c-hdr-av.g{background:linear-gradient(135deg,#60a080,#3d8a6a)}
.c-hdr-info{flex:1;cursor:pointer;min-width:0}
.c-hdr-name{font-size:16px;font-weight:600;color:#fff;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.c-hdr-sub{font-size:12.5px;color:var(--hicon)}
.c-hdr-btns{display:flex}
.chbtn{background:none;border:none;color:var(--hicon);cursor:pointer;width:40px;height:40px;border-radius:50%;display:flex;align-items:center;justify-content:center;transition:background .15s;position:relative}
.chbtn:hover{background:rgba(255,255,255,.1)}
.chbtn svg{width:23px;height:23px;fill:var(--hicon)}
.chbtn .tip{position:absolute;bottom:-30px;left:50%;transform:translateX(-50%);background:rgba(0,0,0,.75);color:#fff;font-size:11px;padding:3px 8px;border-radius:4px;white-space:nowrap;pointer-events:none;opacity:0;transition:opacity .2s;z-index:200}
.chbtn:hover .tip{opacity:1}

/* SEARCH PANEL */
#sp{position:absolute;top:59px;right:0;width:310px;background:var(--panel);border-left:1px solid var(--brdr);height:calc(100% - 59px);display:flex;flex-direction:column;z-index:20;transform:translateX(100%);transition:transform .25s;box-shadow:-3px 0 12px rgba(0,0,0,.12)}
#sp.open{transform:none}
.sp-hdr{padding:20px 16px 14px;background:var(--hdr)}
.sp-hdr h3{color:#fff;font-size:15px;font-weight:400;margin-bottom:12px}
.sp-si{display:flex;align-items:center;background:rgba(255,255,255,.1);border-radius:8px;padding:7px 12px;gap:8px}
.sp-si svg{width:16px;height:16px;fill:#fff;opacity:.6}
.sp-si input{border:none;background:none;outline:none;font-size:14px;color:#fff;width:100%;font-family:inherit}
.sp-si input::placeholder{color:rgba(255,255,255,.5)}
#sr{flex:1;overflow-y:auto;padding:4px 0}
.sr-item{padding:12px 16px;cursor:pointer;border-bottom:1px solid var(--brdr);transition:background .1s;display:flex;flex-direction:column;gap:4px}
.sr-item:hover{background:var(--hov)}
.sr-sender{font-size:12px;color:var(--g);font-weight:600;margin-bottom:3px}
.sr-body{font-size:13.5px;color:var(--tx);line-height:1.4}
.sr-time{font-size:11px;color:var(--tx2);margin-top:3px}
.sr-hl{background:#ffeb3b;color:#000;border-radius:2px;padding:0 2px}
.sp-empty{padding:40px;text-align:center;color:var(--tx2);font-size:14px;line-height:1.6}
/* CONTACT INFO PANEL */
#ci-panel{position:absolute;top:59px;right:0;width:310px;background:var(--sbg);border-left:1px solid var(--brdr);height:calc(100% - 59px);display:flex;flex-direction:column;z-index:25;transform:translateX(100%);transition:transform .25s;box-shadow:-3px 0 12px rgba(0,0,0,.12);overflow-y:auto}
#ci-panel.open{transform:none}
.ci-hero{background:var(--panel);padding:24px 0;display:flex;flex-direction:column;align-items:center;box-shadow:0 1px 3px rgba(0,0,0,.08);margin-bottom:10px}
.ci-big-av{width:200px;height:200px;border-radius:50%;background:var(--sbg);margin-bottom:16px;overflow:hidden;display:flex;align-items:center;justify-content:center;font-size:60px;color:var(--tx2);cursor:pointer}
.ci-big-av img{width:100%;height:100%;object-fit:cover}
.ci-t1{font-size:20px;color:var(--tx);font-weight:500;margin-bottom:4px;display:flex;align-items:center;gap:8px}
.ci-t2{font-size:14px;color:var(--tx2)}
.ci-sect{background:var(--panel);padding:16px;margin-bottom:10px;box-shadow:0 1px 3px rgba(0,0,0,.08)}
.ci-sect h4{font-size:13px;color:var(--tx2);margin-bottom:10px;font-weight:500}
.ci-body{font-size:15px;color:var(--tx);line-height:1.5}
.ci-media-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:6px}
.ci-m-item{aspect-ratio:1;background:var(--sbg);cursor:pointer;position:relative;overflow:hidden}
.ci-m-item img,.ci-m-item video{width:100%;height:100%;object-fit:cover}
.ci-act{display:flex;align-items:center;gap:16px;color:#ef5350;font-size:15px;cursor:pointer;padding:16px;background:var(--panel);margin-bottom:10px;box-shadow:0 1px 3px rgba(0,0,0,.08)}
.ci-act svg{fill:#ef5350;width:20px;height:20px}
.ci-edit{cursor:pointer;fill:var(--g);width:16px;height:16px;opacity:.8}
/* SWITCH */
.switch{position:relative;display:inline-block;width:36px;height:20px;margin-right:10px}
.switch input{opacity:0;width:0;height:0}
.slider{position:absolute;cursor:pointer;top:0;left:0;right:0;bottom:0;background-color:#ccc;transition:.4s;border-radius:20px}
.slider:before{position:absolute;content:"";height:16px;width:16px;left:2px;bottom:2px;background-color:#fff;transition:.4s;border-radius:50%}
input:checked+.slider{background-color:var(--g)}
input:checked+.slider:before{transform:translateX(16px)}

/* STATS PANEL */
#stats-panel{position:absolute;top:59px;left:0;right:0;bottom:0;background:var(--panel);display:none;flex-direction:column;z-index:15;overflow-y:auto;padding:20px 24px}
#stats-panel.open{display:flex}
.sp-close{position:sticky;top:0;align-self:flex-end;background:var(--sbg);border:none;border-radius:50%;width:36px;height:36px;cursor:pointer;display:flex;align-items:center;justify-content:center;margin-bottom:8px;flex-shrink:0}
.sp-close svg{width:18px;height:18px;fill:var(--tx2)}
.stats-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:20px}
.sc{background:var(--sbg);border-radius:12px;padding:16px;text-align:center}
.sc-n{font-size:28px;font-weight:700;color:var(--g)}
.sc-l{font-size:12px;color:var(--tx2);margin-top:4px}
.ss h4{font-size:13px;font-weight:600;color:var(--tx2);margin-bottom:10px;text-transform:uppercase;letter-spacing:.5px}
.ss{margin-bottom:20px}
.sb{display:flex;align-items:center;gap:10px;margin-bottom:8px}
.sb-name{font-size:13px;color:var(--tx);min-width:110px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.sb-track{flex:1;height:7px;background:var(--brdr);border-radius:4px;overflow:hidden}
.sb-fill{height:100%;background:var(--g);border-radius:4px}
.sb-count{font-size:12px;color:var(--tx2);min-width:32px;text-align:right}
.hc{display:flex;align-items:flex-end;gap:2px;height:60px}
.hcb{flex:1;background:var(--g);border-radius:2px 2px 0 0;opacity:.65;min-height:2px}
.hcl{display:flex;justify-content:space-between;margin-top:4px}
.hcl span{font-size:10px;color:var(--tx2)}

/* FILTER BAR */
#filter-bar{display:none;padding:8px 12px;background:var(--panel);border-bottom:1px solid var(--brdr);gap:8px;flex-wrap:wrap;align-items:center;flex-shrink:0}
#filter-bar.show{display:flex}
.fi{padding:7px 12px;border:1px solid var(--brdr);border-radius:20px;font-size:13px;outline:none;background:var(--sbg);color:var(--tx);font-family:inherit}
.fi:focus{border-color:var(--g)}
.fi::placeholder{color:var(--tx2)}
.fbtn{padding:7px 16px;border-radius:20px;font-size:13px;font-weight:500;border:none;cursor:pointer;font-family:inherit}
.fbtn.go{background:var(--g);color:#fff}
.fbtn.cl{background:var(--sbg);color:var(--tx2);border:1px solid var(--brdr)}
.fct{font-size:12px;color:var(--tx2)}

/* LOADER */
#lm{padding:15px;display:none;flex-shrink:0}
.loader{border:3px solid var(--sbg);border-top:3px solid var(--g);border-radius:50%;width:24px;height:24px;animation:spin .8s linear infinite;margin:0 auto}
@keyframes spin{0%{transform:rotate(0deg)}100%{transform:rotate(360deg)}}

/* MESSAGES */
#messages{flex:1;overflow-y:auto;padding:16px 7%;display:flex;flex-direction:column;gap:1px}
#messages::-webkit-scrollbar{width:5px}
#messages::-webkit-scrollbar-thumb{background:rgba(0,0,0,.15);border-radius:3px}
[data-theme=dark] #messages::-webkit-scrollbar-thumb{background:rgba(255,255,255,.1)}
.date-div{display:flex;align-items:center;justify-content:center;margin:10px 0}
.date-div span{background:rgba(225,245,254,.92);color:#54656f;font-size:12.5px;padding:5px 12px;border-radius:8px;box-shadow:0 1px 1px var(--shd)}
[data-theme=dark] .date-div span{background:rgba(32,44,51,.95);color:var(--tx2)}
.mw{display:flex;flex-direction:column;margin:1px 0;animation:fi .15s ease}
@keyframes fi{from{opacity:0;transform:translateY(3px)}to{opacity:1;transform:none}}
.mw.out{align-items:flex-end}
.mw.in{align-items:flex-start}
.bubble{max-width:min(65%,680px);padding:6px 9px 8px;position:relative;box-shadow:0 1px 1px var(--shd);word-break:break-word;border-radius:7.5px}
.in .bubble{background:var(--bin);border-top-left-radius:0}
.out .bubble{background:var(--bout);border-top-right-radius:0}
.bubble::before{content:'';position:absolute;top:0;border:6px solid transparent}
.in .bubble::before{left:-11px;border-right-color:var(--bin);border-top-color:var(--bin)}
.out .bubble::before{right:-11px;border-left-color:var(--bout);border-top-color:var(--bout)}
.b-sender{font-size:12.5px;font-weight:600;color:var(--g);margin-bottom:3px}
.b-sender:hover{text-decoration:underline;cursor:pointer}
.b-reply{background:var(--rply);border-left:3px solid var(--g);padding:6px 10px;border-radius:4px;margin-bottom:6px}
.b-reply-s{font-size:11.5px;font-weight:600;color:var(--g)}
.b-reply-t{font-size:12.5px;color:var(--tx2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:300px}
.b-body{font-size:14.2px;color:var(--tx);line-height:19px;white-space:pre-wrap}
.b-foot{display:flex;align-items:center;justify-content:flex-end;gap:4px;margin-top:2px}
.b-time{font-size:11px;color:var(--tx2);white-space:nowrap}
.b-star{font-size:11px;color:#f0c040}
.tick svg{width:15px;height:11px}
.m-img{max-width:100%;border-radius:6px;cursor:pointer;display:block;margin-bottom:4px;max-height:280px;object-fit:cover}
.revoked{font-style:italic;color:var(--tx2);font-size:13.5px;display:flex;align-items:center;gap:6px}
.m-vid{max-width:100%;border-radius:6px;max-height:280px}
.m-doc{display:flex;align-items:center;gap:10px;background:var(--rply);padding:10px;border-radius:8px;color:var(--g);text-decoration:none;font-size:13px;font-weight:500;margin-bottom:4px}
.m-doc svg{width:26px;height:26px;fill:var(--g);flex-shrink:0}
.m-audio{width:200px;margin-bottom:4px}
.empty-msgs{display:flex;align-items:center;justify-content:center;flex:1;color:var(--tx2);font-size:14px;gap:8px}

/* SCROLL TO BOTTOM */
#s2b{position:absolute;bottom:76px;right:18px;width:42px;height:42px;border-radius:50%;background:var(--panel);border:none;cursor:pointer;box-shadow:0 2px 10px rgba(0,0,0,.22);display:flex;align-items:center;justify-content:center;z-index:10;opacity:0;pointer-events:none;transition:opacity .2s}
#s2b.show{opacity:1;pointer-events:all}
#s2b svg{width:22px;height:22px;fill:var(--tx2)}
#ucb{position:absolute;top:-5px;right:-5px;background:var(--g);color:#fff;font-size:10px;font-weight:700;min-width:18px;height:18px;border-radius:9px;display:none;align-items:center;justify-content:center;padding:0 4px}
#ucb.show{display:flex}

/* REPLY BAR */
#reply-bar{display:none;align-items:center;padding:8px 16px;background:var(--panel);border-top:1px solid var(--brdr);gap:10px;flex-shrink:0}
#reply-bar.show{display:flex}
.rb-c{flex:1;background:var(--sbg);border-left:3px solid var(--g);padding:6px 10px;border-radius:4px}
.rb-s{font-size:12px;font-weight:600;color:var(--g)}
.rb-t{font-size:13px;color:var(--tx2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.rb-x{background:none;border:none;cursor:pointer;display:flex;padding:4px}
.rb-x svg{width:20px;height:20px;fill:var(--tx2)}

/* INPUT */
#input-area{display:flex;align-items:flex-end;padding:8px 16px 10px;gap:10px;background:var(--sbg);flex-shrink:0}
.iw{flex:1;background:var(--panel);border-radius:24px;padding:9px 16px;display:flex;align-items:flex-end;gap:10px;box-shadow:0 1px 3px var(--shd);min-height:46px}
#msg-input{flex:1;border:none;outline:none;font-size:15px;color:var(--tx);resize:none;max-height:120px;overflow-y:auto;font-family:inherit;line-height:21px;background:transparent}
#msg-input::placeholder{color:var(--tx2)}
#send-btn{width:50px;height:50px;border-radius:50%;background:var(--g);border:none;display:flex;align-items:center;justify-content:center;cursor:pointer;flex-shrink:0;transition:background .2s;box-shadow:0 2px 8px rgba(0,168,132,.4)}
#send-btn:hover{background:var(--gd)}
#send-btn svg{width:22px;height:22px;fill:#fff}
.att-btn{width:42px;height:42px;border-radius:50%;background:transparent;border:none;display:flex;align-items:center;justify-content:center;cursor:pointer;flex-shrink:0;color:var(--tx2)}
.att-btn:hover{color:var(--g)}
.att-btn svg{width:24px;height:24px;fill:currentColor}
.ch-back{display:none;background:none;border:none;cursor:pointer;font-size:30px;line-height:1;color:var(--tx);padding:0 4px 0 2px;flex-shrink:0}
#att-preview{display:none;align-items:center;gap:10px;padding:6px 16px;background:var(--sbg);border-top:1px solid var(--brdr);flex-shrink:0}
#att-preview.show{display:flex}
#att-thumb{width:40px;height:40px;border-radius:8px;object-fit:cover;background:var(--hov)}
#att-name{font-size:13px;color:var(--tx);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
#att-remove{background:none;border:none;cursor:pointer;color:var(--tx2);font-size:18px;line-height:1}

/* MOBILE / RESPONSIVE */
@media (max-width: 768px) {
  .ch-back{display:flex}
  #sidebar{position:absolute;inset:0;width:100%;z-index:30}
  #sidebar.chat-open{transform:translateX(-100%);display:none}
  #chatview{position:absolute;inset:0;width:100%;z-index:31;transform:translateX(100%)}
  #chatview.show{transform:translateX(0)}
  .s-hdr .hdr-btns{overflow-x:auto;max-width:60vw}
  .ci-av,.grp-av{width:40px;height:40px;font-size:16px}
  .m-row{padding:4px 8px}
  .m-bub{max-width:82%}
}
@media (min-width: 769px) {
  #chatview{transform:none!important;display:flex!important}
  #sidebar{transform:none!important;display:flex!important}
}
/* CONTEXT MENU */
#ctx{position:fixed;background:var(--panel);border-radius:8px;box-shadow:0 4px 24px rgba(0,0,0,.2);z-index:1000;display:none;min-width:160px;overflow:hidden}
.cti{padding:10px 16px;cursor:pointer;font-size:13.5px;color:var(--tx);display:flex;align-items:center;gap:10px;transition:background .1s}
.cti:hover{background:var(--hov)}
.cti svg{width:16px;height:16px;fill:var(--tx2)}
.ct-sep{height:1px;background:var(--brdr)}
.cti.red{color:#ea0038}
.cti.red svg{fill:#ea0038}

/* QR */
.qr-ov{position:fixed;inset:0;background:rgba(0,0,0,.9);display:flex;align-items:center;justify-content:center;z-index:9999}
.qr-box{background:var(--panel);border-radius:20px;padding:44px 36px;text-align:center;max-width:420px;box-shadow:0 24px 60px rgba(0,0,0,.5)}
.qr-logo{font-size:44px;margin-bottom:12px}
.qr-box h2{font-size:22px;font-weight:400;color:var(--tx);margin-bottom:8px}
.qr-box p{font-size:13.5px;color:var(--tx2);line-height:1.7;margin-bottom:20px}
.qr-box img{width:240px;height:240px;border-radius:8px}
.qr-steps{text-align:left;margin-top:20px;display:flex;flex-direction:column;gap:8px}
.qr-step{display:flex;align-items:flex-start;gap:10px;font-size:13px;color:var(--tx2)}
.qr-step-n{background:var(--g);color:#fff;width:20px;height:20px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;flex-shrink:0;margin-top:1px}

/* TOAST */
#tc{position:fixed;bottom:20px;left:50%;transform:translateX(-50%);display:flex;flex-direction:column;gap:8px;z-index:9000;pointer-events:none;align-items:center}
.toast{background:rgba(32,44,51,.96);color:#fff;padding:10px 20px;border-radius:8px;font-size:13.5px;animation:su .3s ease;box-shadow:0 4px 14px rgba(0,0,0,.3);max-width:380px;text-align:center}
@keyframes su{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:none}}
/* REPORT MODAL */
.rp-box{background:var(--panel);border-radius:12px;padding:24px;width:340px;box-shadow:0 12px 24px var(--shd);display:flex;flex-direction:column;gap:16px}
.rp-box h3{color:var(--tx);font-size:18px;font-weight:600}
.rp-sel{padding:10px;border-radius:8px;background:var(--sbg);border:1px solid var(--brdr);color:var(--tx);outline:none;font-family:inherit;width:100%}
.rp-btns{display:flex;justify-content:flex-end;gap:10px;margin-top:8px}
/* SCHEDULER */
.sch-box{background:var(--panel);width:450px;padding:24px;border-radius:16px;box-shadow:0 20px 40px rgba(0,0,0,.5);display:flex;flex-direction:column;gap:15px}
.sch-inp{background:var(--sbg);border:1px solid var(--brdr);color:var(--tx);padding:12px;border-radius:8px;outline:none;font-family:inherit;width:100%}
.sch-row{display:flex;align-items:center;justify-content:space-between;gap:10px;color:var(--tx)}
.sch-list{max-height:250px;overflow-y:auto;border-top:1px solid var(--brdr);padding-top:10px;margin-top:5px}
.sch-item{background:var(--sbg);padding:10px;border-radius:8px;margin-bottom:8px;display:flex;justify-content:space-between;align-items:center;font-size:13px;border-left:3px solid var(--g)}
.sch-item.cond{border-left-color:#ff9800}
.sch-del{color:#ef5350;cursor:pointer;padding:5px}
/* SCHEDULER TABS & PROGRESS */
.sch-tabs{display:flex;border-bottom:1px solid var(--brdr);margin-bottom:15px}
.sch-tab{padding:10px 15px;cursor:pointer;font-size:13px;font-weight:600;color:var(--tx2);border-bottom:2px solid transparent;flex:1;text-align:center}
.sch-tab.active{color:var(--g);border-bottom-color:var(--g)}
.sch-prog-bg{height:4px;background:var(--brdr);border-radius:2px;margin-top:8px;overflow:hidden;position:relative}
.sch-prog-bar{height:100%;background:var(--g);width:0%;transition:width 1s linear}
.sch-cd{font-size:11px;color:var(--tx2);margin-top:4px;display:flex;justify-content:space-between}
/* MENTIONS */
#mention-box{position:absolute;bottom:60px;left:16px;background:var(--panel);border-radius:8px;box-shadow:0 -4px 12px var(--shd);max-height:200px;overflow-y:auto;width:300px;z-index:50;display:flex;flex-direction:column}
.mn-item{padding:8px 12px;cursor:pointer;display:flex;align-items:center;gap:10px;border-bottom:1px solid var(--brdr)}
.mn-item:hover{background:var(--hov)}
::-webkit-scrollbar{width:5px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:rgba(0,0,0,.12);border-radius:3px}
[data-theme=dark] ::-webkit-scrollbar-thumb{background:rgba(255,255,255,.08)}
/* ═══ GMAIL MODE ═══════════════════════════════════════════ */
#gmail-sidebar{display:none;flex-direction:column;flex:1;overflow:hidden;background:#fff;font-family:'Google Sans',Roboto,sans-serif}
[data-theme=dark] #gmail-sidebar{background:#111b21}
.gm-compose{margin:8px 16px 4px;background:#c2e7ff;color:#001d35;border:none;border-radius:16px;padding:16px 24px;font-size:14px;font-weight:500;cursor:pointer;display:flex;align-items:center;gap:12px;box-shadow:0 1px 3px rgba(0,0,0,.2);transition:box-shadow .2s}
.gm-compose:hover{box-shadow:0 2px 8px rgba(0,0,0,.3)}
.gm-compose svg{width:20px;height:20px}
.gm-nav{padding:4px 0;flex-shrink:0}
.gm-nav-item{display:flex;align-items:center;gap:16px;padding:4px 16px 4px 26px;border-radius:0 24px 24px 0;cursor:pointer;font-size:14px;color:#202124;height:36px;position:relative;transition:background .1s}
[data-theme=dark] .gm-nav-item{color:#e8eaed}
.gm-nav-item:hover{background:rgba(0,0,0,.06)}
[data-theme=dark] .gm-nav-item:hover{background:rgba(255,255,255,.08)}
.gm-nav-item.active{background:#d3e3fd;font-weight:600}
[data-theme=dark] .gm-nav-item.active{background:#283141}
.gm-nav-item svg{width:20px;height:20px;fill:#444746;flex-shrink:0}
[data-theme=dark] .gm-nav-item svg{fill:#c4c7c5}
.gm-nav-item.active svg{fill:#001d35}
.gm-unread-badge{background:#1a73e8;color:#fff;font-size:11px;font-weight:700;padding:1px 6px;border-radius:8px;margin-left:auto}
.gm-section-label{font-size:11px;font-weight:600;color:#444746;padding:8px 26px 4px;text-transform:uppercase;letter-spacing:.8px}
[data-theme=dark] .gm-section-label{color:#8e918f}
.gm-email-list{flex:1;overflow-y:auto}
.gm-email-list::-webkit-scrollbar{width:4px}
.gm-email-list::-webkit-scrollbar-thumb{background:rgba(0,0,0,.15);border-radius:2px}
.gm-email-row{display:flex;align-items:center;padding:0 16px;height:52px;cursor:pointer;border-bottom:1px solid rgba(0,0,0,.06);position:relative;transition:background .1s;gap:8px}
.gm-email-row:hover{box-shadow:0 1px 3px rgba(0,0,0,.15);background:#f2f6fc;z-index:1}
[data-theme=dark] .gm-email-row:hover{background:#1c2938}
.gm-email-row.unread{background:#fff}
[data-theme=dark] .gm-email-row.unread{background:#1a2433}
.gm-email-row.unread .gm-row-sender{font-weight:700}
.gm-email-row.unread .gm-row-subject{font-weight:700;color:#202124}
[data-theme=dark] .gm-email-row.unread .gm-row-subject{color:#e8eaed}
.gm-row-star{width:20px;height:20px;fill:#ccc;cursor:pointer;flex-shrink:0}
.gm-row-star.on{fill:#f4b400}
.gm-row-av{width:32px;height:32px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:600;color:#fff;flex-shrink:0;text-transform:uppercase}
.gm-row-content{flex:1;min-width:0;display:flex;align-items:center;gap:8px}
.gm-row-sender{font-size:14px;color:#202124;width:140px;flex-shrink:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
[data-theme=dark] .gm-row-sender{color:#e8eaed}
.gm-row-body{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:14px;color:#202124}
[data-theme=dark] .gm-row-body{color:#e8eaed}
.gm-row-subject{color:#202124;font-weight:500}
[data-theme=dark] .gm-row-subject{color:#e8eaed}
.gm-row-snippet{color:#5f6368;font-weight:400}
.gm-row-time{font-size:12px;color:#5f6368;white-space:nowrap;flex-shrink:0;margin-left:8px}
/* Gmail Main Area */
#gmail-main{display:none;flex-direction:column;height:100%;background:#f6f8fc;font-family:'Google Sans',Roboto,sans-serif}
[data-theme=dark] #gmail-main{background:#0d1418}
.gm-toolbar{display:flex;align-items:center;padding:8px 16px;gap:8px;background:var(--hdr);height:59px;flex-shrink:0}
.gm-toolbar-title{font-size:20px;font-weight:400;color:#fff;flex:1}
.gm-tbtn{background:none;border:none;color:#aebac1;cursor:pointer;width:40px;height:40px;border-radius:50%;display:flex;align-items:center;justify-content:center}
.gm-tbtn:hover{background:rgba(255,255,255,.1)}
.gm-tbtn svg{width:22px;height:22px;fill:#aebac1}
/* Gmail No Email Selected */
#gmail-no-email{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:16px}
.gm-empty-ring{width:120px;height:120px;border-radius:50%;background:#e8f0fe;display:flex;align-items:center;justify-content:center}
.gm-empty-ring svg{width:56px;height:56px;fill:#1a73e8;opacity:.7}
/* Gmail Email Reader */
#gmail-reader{display:none;flex-direction:column;height:100%;overflow:hidden}
.gm-reader-hdr{padding:20px 24px 12px;background:#f6f8fc;flex-shrink:0;border-bottom:1px solid #e0e0e0}
[data-theme=dark] .gm-reader-hdr{background:#1c2a32;border-color:#2a3942}
.gm-reader-subject{font-size:22px;font-weight:400;color:#202124;margin-bottom:12px}
[data-theme=dark] .gm-reader-subject{color:#e8eaed}
.gm-reader-meta{display:flex;align-items:center;gap:12px}
.gm-reader-av{width:40px;height:40px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:16px;font-weight:600;color:#fff;flex-shrink:0;text-transform:uppercase}
.gm-reader-from{flex:1}
.gm-reader-name{font-size:14px;font-weight:600;color:#202124}
[data-theme=dark] .gm-reader-name{color:#e8eaed}
.gm-reader-addr{font-size:12px;color:#5f6368}
.gm-reader-date{font-size:12px;color:#5f6368}
.gm-reader-body{flex:1;overflow-y:auto;padding:20px 24px;font-size:14px;color:#202124;line-height:1.7;white-space:pre-wrap;word-break:break-word;background:#fff}
[data-theme=dark] .gm-reader-body{background:#1a2433;color:#e8eaed}
.gm-reply-box{padding:12px 16px;background:#f6f8fc;border-top:1px solid #e0e0e0;display:flex;align-items:flex-end;gap:10px;flex-shrink:0}
[data-theme=dark] .gm-reply-box{background:#111b21;border-color:#2a3942}
.gm-reply-input{flex:1;background:#fff;border:1px solid #e0e0e0;border-radius:24px;padding:10px 16px;font-size:14px;outline:none;resize:none;max-height:100px;font-family:inherit;color:#202124}
[data-theme=dark] .gm-reply-input{background:#2a3942;border-color:#3a4a52;color:#e8eaed}
.gm-reply-input::placeholder{color:#9aa0a6}
.gm-send-btn{width:46px;height:46px;border-radius:50%;background:#1a73e8;border:none;cursor:pointer;display:flex;align-items:center;justify-content:center;flex-shrink:0}
.gm-send-btn:hover{background:#1557b0}
.gm-send-btn svg{width:20px;height:20px;fill:#fff}
/* Gmail Login Screen */
#gmail-login{display:none;flex-direction:column;align-items:center;justify-content:center;flex:1;padding:24px;text-align:center}
.gm-login-card{background:#fff;border-radius:24px;padding:40px 36px;max-width:420px;width:100%;box-shadow:0 1px 8px rgba(0,0,0,.15)}
[data-theme=dark] .gm-login-card{background:#1c2a32}
.gm-logo{display:flex;align-items:center;justify-content:center;gap:8px;margin-bottom:24px}
.gm-logo svg{width:32px;height:32px}
.gm-logo-text{font-size:26px;color:#5f6368;font-weight:300;letter-spacing:-1px}
.gm-logo-text span{color:#1a73e8}
.gm-login-h{font-size:22px;font-weight:400;color:#202124;margin-bottom:8px}
[data-theme=dark] .gm-login-h{color:#e8eaed}
.gm-login-p{font-size:13px;color:#5f6368;margin-bottom:24px;line-height:1.6}
.gm-inp{width:100%;padding:14px 16px;border:1px solid #dadce0;border-radius:8px;font-size:15px;outline:none;font-family:inherit;color:#202124;margin-bottom:12px;transition:border .2s;background:#fff}
[data-theme=dark] .gm-inp{background:#2a3942;border-color:#3a4a52;color:#e8eaed}
.gm-inp:focus{border-color:#1a73e8;box-shadow:0 0 0 2px rgba(26,115,232,.2)}
.gm-login-btn{width:100%;background:#1a73e8;color:#fff;border:none;border-radius:24px;padding:14px;font-size:15px;font-weight:500;cursor:pointer;margin-top:4px;font-family:inherit}
.gm-login-btn:hover{background:#1557b0}
.gm-step2{display:none;flex-direction:column;gap:12px;margin-top:16px}
.gm-auth-link{color:#1a73e8;font-size:13px;text-decoration:none;word-break:break-all;display:block;padding:10px;background:#e8f0fe;border-radius:8px;margin-bottom:8px}
.gm-compose-modal{position:fixed;bottom:0;right:24px;width:480px;background:#fff;border-radius:12px 12px 0 0;box-shadow:0 8px 40px rgba(0,0,0,.4);z-index:500;display:none;flex-direction:column}
[data-theme=dark] .gm-compose-modal{background:#2a3942}
.gm-compose-header{background:#404040;color:#fff;padding:10px 16px;border-radius:12px 12px 0 0;display:flex;justify-content:space-between;align-items:center;font-size:14px;font-weight:500}
.gm-compose-body{padding:8px 16px;display:flex;flex-direction:column;gap:4px}
.gm-compose-field{border:none;border-bottom:1px solid #e0e0e0;padding:10px 4px;font-size:14px;outline:none;font-family:inherit;background:transparent;color:#202124;width:100%}
[data-theme=dark] .gm-compose-field{color:#e8eaed;border-color:#3a4a52}
.gm-compose-textarea{border:none;padding:10px 4px;font-size:14px;resize:none;height:160px;outline:none;font-family:inherit;background:transparent;color:#202124;width:100%}
[data-theme=dark] .gm-compose-textarea{color:#e8eaed}
.gm-compose-footer{padding:10px 16px;display:flex;justify-content:space-between;align-items:center;border-top:1px solid #e0e0e0}
[data-theme=dark] .gm-compose-footer{border-color:#3a4a52}
.gm-compose-send{background:#1a73e8;color:#fff;border:none;border-radius:24px;padding:10px 24px;font-size:14px;font-weight:500;cursor:pointer;font-family:inherit}
/* ADVANCED GMAIL SWITCHER */
.gm-sb-header{padding:16px;flex-shrink:0;position:relative}
.gm-account-trigger{display:flex;align-items:center;gap:12px;cursor:pointer;padding:8px 12px;border-radius:12px;transition:background .2s;background:var(--sbg);border:1px solid var(--brdr)}
.gm-account-trigger:hover{background:var(--hov)}
.gm-sb-av{width:32px;height:32px;border-radius:50%;background:#ccc;color:#fff;display:flex;align-items:center;justify-content:center;font-weight:600;font-size:14px;flex-shrink:0}
.gm-sb-email{font-size:13.5px;font-weight:600;color:var(--tx);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.gm-sb-sub{font-size:11px;color:var(--tx2);margin-top:1px}
.gm-account-menu{position:absolute;top:70px;left:16px;right:16px;background:var(--panel);border-radius:12px;box-shadow:0 8px 24px rgba(0,0,0,.25);z-index:200;display:none;flex-direction:column;overflow:hidden;border:1px solid var(--brdr);animation:fi .15s ease}
.gm-acc-item{display:flex;align-items:center;gap:12px;padding:12px 14px;cursor:pointer;border-bottom:1px solid var(--brdr);transition:background .15s}
.gm-acc-item:hover{background:var(--hov)}
.gm-search-box{background:var(--sbg);border-radius:24px;padding:10px 16px;display:flex;align-items:center;gap:10px;margin-bottom:12px}
</style>
</head>
<body>
<div id="app">

<!-- SIDEBAR -->
<div id="sidebar">
  <div class="s-hdr">
    <div class="s-hdr-l">
      <div class="my-av" id="main-avatar" onclick="toggleGmailMode()" title="Switch to Gmail" style="cursor:pointer">M</div>
      <span class="app-name">D4RKAXIS</span>
    </div>
    <div class="hdr-btns" style="overflow-x:auto;gap:5px;padding-bottom:2px">
      <button class="hbtn" onclick="UI.toggleGhost()" title="Ghost Mode (Stealth)">
        <svg id="gh-icon" viewBox="0 0 24 24"><path d="M12 4.5C7 4.5 2.73 7.61 1 12c1.73 4.39 6 7.5 11 7.5s9.27-3.11 11-7.5c-1.73-4.39-6-7.5-11-7.5zM12 17c-2.76 0-5-2.24-5-5s2.24-5 5-5 5 2.24 5 5-2.24 5-5 5zm0-8c-1.66 0-3 1.34-3 3s1.34 3 3 3 3-1.34 3-3-1.34-3-3-3z"/></svg>
      </button>
      <button class="hbtn" onclick="UI.scheduler()" title="Smart Scheduler">
        <svg viewBox="0 0 24 24"><path d="M11.99 2C6.47 2 2 6.48 2 12s4.47 10 9.99 10C17.52 22 22 17.52 22 12S17.52 2 11.99 2zM12 20c-4.42 0-8-3.58-8-8s3.58-8 8-8 8 3.58 8 8-8 3.58-8 8zm.5-13H11v6l5.25 3.15.75-1.23-4.5-2.67z"/></svg>
      </button>
      <button class="hbtn" onclick="UI.dark()" title="Dark Mode">
        <svg viewBox="0 0 24 24"><path d="M12 3c-4.97 0-9 4.03-9 9s4.03 9 9 9 9-4.03 9-9c0-.46-.04-.92-.1-1.36-.98 1.37-2.58 2.26-4.4 2.26-2.98 0-5.4-2.42-5.4-5.4 0-1.81.89-3.42 2.26-4.4-.44-.06-.9-.1-1.36-.1z"/></svg>
      </button>
      <button class="hbtn" onclick="UI.export()" title="Export">
        <svg viewBox="0 0 24 24"><path d="M19 9h-4V3H9v6H5l7 7 7-7zM5 18v2h14v-2H5z"/></svg>
      </button>
      <button class="hbtn" onclick="UI.clickReport()" oncontextmenu="event.preventDefault();UI.reportModal()" title="Reports (Right-click to select group)">
        <svg viewBox="0 0 24 24"><path d="M14 2H6c-1.1 0-1.99.9-1.99 2L4 20c0 1.1.89 2 1.99 2H18c1.1 0 2-.9 2-2V8l-6-6zm2 16H8v-2h8v2zm0-4H8v-2h8v2zm-3-5V3.5L18.5 9H13z"/></svg>
      </button>
      <button class="hbtn" onclick="logout()" title="Reset Session & Re-Scan (Get Full History)">
        <svg viewBox="0 0 24 24"><path d="M17 7l-1.41 1.41L18.17 11H8v2h10.17l-2.58 2.58L17 17l5-5zM4 5h8V3H4c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h8v-2H4V5z"/></svg>
      </button>
      <button class="hbtn" onclick="resync()" title="Clear local cache &amp; resync everything">
        <svg viewBox="0 0 24 24"><path d="M17.65 6.35A7.95 7.95 0 0 0 12 4c-4.42 0-7.99 3.58-7.99 8s3.57 8 7.99 8c3.73 0 6.84-2.55 7.73-6h-2.08A5.99 5.99 0 0 1 12 18c-3.31 0-6-2.69-6-6s2.69-6 6-6c1.66 0 3.14.69 4.22 1.78L13 11h7V4l-2.35 2.35z"/></svg>
      </button>
      <button class="hbtn" onclick="showShareQR()" title="Share access — let others use this WhatsApp from their device">
        <svg viewBox="0 0 24 24"><path d="M15 12c2.21 0 4-1.79 4-4s-1.79-4-4-4-4 1.79-4 4 1.79 4 4 4zm0-6c1.1 0 2 .9 2 2s-.9 2-2 2-2-.9-2-2 .9-2 2-2zm0 6.4c-2.55 0-4.83.98-6.53 2.57A8.06 8.06 0 0 0 9 19.95V22h12v-2.05c0-2.53-1.62-4.73-3.81-5.55A9.7 9.7 0 0 0 15 12.4zM4 13a6 6 0 0 1 6-6c.34 0 .67.03.99.08C9.7 7.2 8.5 7 7.3 7 4.49 7 2 9.49 2 12.3V14h2v-1z"/></svg>
      </button>
    </div>
  </div>
  <div class="s-search">
    <div class="s-si">
      <svg viewBox="0 0 24 24"><path d="M15.5 14h-.79l-.28-.27A6.47 6.47 0 0 0 16 9.5 6.5 6.5 0 1 0 9.5 16c1.61 0 3.09-.59 4.23-1.57l.27.28v.79l5 4.99L20.49 19l-4.99-5zm-6 0C7.01 14 5 11.99 5 9.5S7.01 5 9.5 5 14 7.01 14 9.5 11.99 14 9.5 14z"/></svg>
      <input id="chat-search" placeholder="Search or type number..." oninput="handleSearch(this.value)">
    </div>
  </div>
  <div class="s-tabs">
    <div class="stab on" onclick="setTab('all',this)">All</div>
    <div class="stab" onclick="setTab('unread',this)">Unread</div>
    <div class="stab" onclick="setTab('groups',this)">Groups</div>
    <div class="stab" onclick="setTab('personal',this)">Personal</div>
  </div>
  <div id="chats-list"><div style="padding:30px;text-align:center;color:var(--tx2);font-size:14px">Connecting...</div></div>
<!-- GMAIL SIDEBAR -->
<div id="gmail-sidebar" style="display:none;flex-direction:column;flex:1;overflow:hidden;background:var(--sid)">
  <!-- NEW HEADER & SWITCHER -->
  <div class="gm-sb-header">
    <div class="gm-account-trigger" onclick="toggleAccountMenu()">
       <div id="gm-cur-av" class="gm-sb-av">?</div>
       <div style="flex:1;overflow:hidden">
          <div id="gm-cur-email" class="gm-sb-email">Select Account</div>
          <div class="gm-sb-sub">Tap to switch accounts</div>
       </div>
       <svg viewBox="0 0 24 24" width="20" height="20" fill="var(--tx2)"><path d="M7 10l5 5 5-5z"/></svg>
    </div>
    <div id="gm-account-menu" class="gm-account-menu"></div>
  </div>

  <!-- SEARCH -->
  <div style="padding:0 16px;flex-shrink:0">
    <div class="gm-search-box">
      <svg width="20" height="20" viewBox="0 0 24 24" style="fill:var(--tx2)"><path d="M15.5 14h-.79l-.28-.27A6.47 6.47 0 0 0 16 9.5 6.5 6.5 0 1 0 9.5 16c1.61 0 3.09-.59 4.23-1.57l.27.28v.79l5 4.99L20.49 19l-4.99-5zm-6 0C7.01 14 5 11.99 5 9.5S7.01 5 9.5 5 14 7.01 14 9.5 11.99 14 9.5 14z"/></svg>
      <input id="gm-search-input" placeholder="Search mail" style="border:none;background:none;outline:none;font-size:15px;color:var(--tx);width:100%;font-family:inherit" onkeydown="if(event.key==='Enter')gmailSearch(this.value)">
      <button onclick="gmailSearch(document.getElementById('gm-search-input').value)" style="background:none;border:none;cursor:pointer;padding:0;display:flex">
        <svg width="20" height="20" viewBox="0 0 24 24" style="fill:#1a73e8;opacity:0;width:0"><path d="M0 0h24v24H0z" fill="none"/></svg>
      </button>
    </div>
  </div>
  <!-- Compose Button -->
  <button onclick="gmailCompose()" style="margin:4px 16px 8px;background:#c2e7ff;color:#001d35;border:none;border-radius:16px;padding:14px 20px;font-size:14px;font-weight:500;cursor:pointer;display:flex;align-items:center;gap:10px;box-shadow:0 1px 3px rgba(0,0,0,.15);flex-shrink:0">
    <svg width="20" height="20" viewBox="0 0 24 24" style="fill:#001d35"><path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25zM20.71 7.04a1 1 0 0 0 0-1.41l-2.34-2.34a1 1 0 0 0-1.41 0l-1.83 1.83 3.75 3.75 1.83-1.83z"/></svg>
    Compose
  </button>
  <!-- Nav -->
  <div id="gm-nav-list" style="padding:0 0 8px;flex-shrink:0"></div>
  <!-- Email List -->
  <div style="flex:1;overflow-y:auto;overflow-x:hidden;border-top:1px solid var(--brdr)">
    <div id="gm-email-list"></div>
    <!-- Load More -->
    <div id="gm-load-more" style="display:none;padding:12px;text-align:center">
      <button onclick="gmailLoadMore()" style="background:none;border:1px solid #1a73e8;color:#1a73e8;border-radius:20px;padding:8px 24px;font-size:13px;cursor:pointer;font-family:inherit">Load more emails</button>
    </div>
    <div id="gm-loading" style="display:none;padding:20px;text-align:center">
      <div style="border:3px solid #e8f0fe;border-top:3px solid #1a73e8;border-radius:50%;width:24px;height:24px;animation:spin .8s linear infinite;margin:0 auto"></div>
    </div>
  </div>
</div>
</div>

<!-- CHAT AREA -->
<div id="chat-area">
  <div class="wa-bg"></div>
<!-- GMAIL LOGIN -->
<!-- EMAIL ACCOUNT MANAGER -->
<div id="gmail-login" style="display:none;flex-direction:column;align-items:center;justify-content:center;flex:1;padding:24px;text-align:center;position:absolute;inset:0;z-index:5;background:var(--bg);overflow-y:auto">
  <div style="background:var(--panel);border-radius:24px;padding:32px;max-width:480px;width:100%;box-shadow:0 2px 16px rgba(0,0,0,.15)">

    <!-- Saved accounts -->
    <div id="gm-accounts-section">
      <h3 style="font-size:17px;font-weight:500;color:var(--tx);margin-bottom:4px">Email Accounts</h3>
      <p style="font-size:12px;color:#5f6368;margin-bottom:16px">Switch between accounts or add a new one</p>
      <div id="gm-accounts-list" style="margin-bottom:16px"></div>
      <div style="display:flex;gap:8px;flex-wrap:wrap">
        <button onclick="showAddWork()" style="flex:1;background:#0078d4;color:#fff;border:none;border-radius:20px;padding:11px;font-size:13px;font-weight:500;cursor:pointer;font-family:inherit;display:flex;align-items:center;justify-content:center;gap:6px">
          <svg width="16" height="16" viewBox="0 0 24 24" style="fill:#fff"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm5 11h-4v4h-2v-4H7v-2h4V7h2v4h4v2z"/></svg>
          Add Work Email
        </button>
        <button onclick="showAddGmail()" style="flex:1;background:#ea4335;color:#fff;border:none;border-radius:20px;padding:11px;font-size:13px;font-weight:500;cursor:pointer;font-family:inherit;display:flex;align-items:center;justify-content:center;gap:6px">
          <svg width="16" height="16" viewBox="0 0 24 24" style="fill:#fff"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm5 11h-4v4h-2v-4H7v-2h4V7h2v4h4v2z"/></svg>
          Add Gmail
        </button>
      </div>
    </div>

    <!-- Add work email form -->
    <div id="gm-add-work" style="display:none">
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:16px">
        <button onclick="showAccountsList()" style="background:none;border:none;cursor:pointer;font-size:20px;color:var(--tx2)">←</button>
        <h3 style="font-size:16px;font-weight:500;color:var(--tx)">Add Work / Microsoft Email</h3>
      </div>
      <div style="background:#e8f4fd;border-radius:10px;padding:12px;margin-bottom:14px;font-size:12px;color:#0056b3;text-align:left">
        <strong>One-time setup:</strong><br>
        1. Go to <a href="https://portal.azure.com" target="_blank" style="color:#0078d4">portal.azure.com</a><br>
        2. App registrations → New → Name it anything<br>
        3. Add redirect URI: <code style="background:#fff;padding:1px 4px;border-radius:3px">http://localhost:3001/auth/callback</code><br>
        4. API permissions → Graph → Delegated: Mail.Read, Mail.Send, Mail.ReadWrite, offline_access, User.Read → Grant admin consent<br>
        5. Copy Client ID + create a Client Secret
      </div>
      <input id="graph-client-id" placeholder="Application (Client) ID" value="80b70a8a-a6df-4e0c-98ec-0ced131e466b" style="width:100%;padding:12px;border:1px solid var(--brdr);border-radius:8px;font-size:13px;outline:none;margin-bottom:8px;font-family:inherit;color:var(--tx);background:var(--panel)">
      <input id="graph-client-secret" type="password" placeholder="Client Secret Value" style="width:100%;padding:12px;border:1px solid var(--brdr);border-radius:8px;font-size:13px;outline:none;margin-bottom:12px;font-family:inherit;color:var(--tx);background:var(--panel)">
      <button onclick="graphGetAuthUrl()" style="width:100%;background:#0078d4;color:#fff;border:none;border-radius:20px;padding:13px;font-size:14px;font-weight:500;cursor:pointer;font-family:inherit">Sign in with Microsoft →</button>
      <div id="graph-step2" style="display:none;margin-top:12px;padding:12px;background:var(--sbg);border-radius:8px;font-size:13px;color:var(--tx)">
        <p style="margin-bottom:8px">✅ Browser opened — sign in with <strong>idris.adeleke@fob.ng</strong> and allow access. This tab will auto-connect.</p>
        <div style="border:3px solid var(--sbg);border-top:3px solid #0078d4;border-radius:50%;width:20px;height:20px;animation:spin .8s linear infinite;margin:0 auto"></div>
      </div>
    </div>

    <!-- Add Gmail form -->
    <div id="gm-add-gmail" style="display:none">
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:16px">
        <button onclick="showAccountsList()" style="background:none;border:none;cursor:pointer;font-size:20px;color:var(--tx2)">←</button>
        <h3 style="font-size:16px;font-weight:500;color:var(--tx)">Add Gmail Account</h3>
      </div>
      <p style="font-size:12px;color:#5f6368;margin-bottom:12px;text-align:left">
        Get credentials at <a href="https://console.cloud.google.com/apis/credentials" target="_blank" style="color:#ea4335">console.cloud.google.com</a> → Create Project → Enable Gmail API → OAuth 2.0 Client ID → Desktop App
      </p>
      <div id="gm-step1">
        <input id="gm-client-id" placeholder="Client ID (.apps.googleusercontent.com)" style="width:100%;padding:12px;border:1px solid var(--brdr);border-radius:8px;font-size:13px;outline:none;margin-bottom:8px;font-family:inherit;color:var(--tx);background:var(--panel)">
        <input id="gm-client-secret" type="password" placeholder="Client Secret" style="width:100%;padding:12px;border:1px solid var(--brdr);border-radius:8px;font-size:13px;outline:none;margin-bottom:12px;font-family:inherit;color:var(--tx);background:var(--panel)">
        <button onclick="gmailGetAuthUrl()" style="width:100%;background:#ea4335;color:#fff;border:none;border-radius:20px;padding:13px;font-size:14px;font-weight:500;cursor:pointer;font-family:inherit">Continue with Google →</button>
      </div>
      <div id="gm-step2" style="display:none;flex-direction:column;gap:8px;margin-top:10px">
        <p style="font-size:12px;color:#5f6368;text-align:left">Open this link, allow access, paste the code:</p>
        <a id="gm-auth-link" href="#" target="_blank" style="color:#1a73e8;font-size:11px;word-break:break-all;display:block;padding:8px;background:#e8f0fe;border-radius:6px;text-decoration:none">Loading...</a>
        <input id="gm-auth-code" placeholder="Paste authorization code" style="width:100%;padding:12px;border:1px solid var(--brdr);border-radius:8px;font-size:13px;outline:none;font-family:inherit;color:var(--tx);background:var(--panel)">
        <button onclick="gmailExchangeCode()" style="width:100%;background:#34a853;color:#fff;border:none;border-radius:20px;padding:12px;font-size:14px;font-weight:500;cursor:pointer;font-family:inherit">Connect Gmail ✓</button>
      </div>
    </div>

  </div>
</div>

<!-- GMAIL MAIN -->
<div id="gmail-main" style="display:none;flex-direction:column;height:100%;position:absolute;inset:0;z-index:4;background:#f6f8fc">
  <!-- Gmail reader toolbar -->
  <div id="gm-reader-toolbar" style="display:flex;align-items:center;padding:8px 16px;gap:4px;background:#f6f8fc;border-bottom:1px solid #e0e0e0;height:52px;flex-shrink:0">
    <button onclick="gmailBackToList()" title="Back" style="background:none;border:none;cursor:pointer;width:36px;height:36px;border-radius:50%;display:flex;align-items:center;justify-content:center">
      <svg width="20" height="20" viewBox="0 0 24 24" style="fill:#444746"><path d="M20 11H7.83l5.59-5.59L12 4l-8 8 8 8 1.41-1.41L7.83 13H20v-2z"/></svg>
    </button>
    <button id="gm-star-btn" onclick="gmailToggleStar()" title="Star" style="background:none;border:none;cursor:pointer;width:36px;height:36px;border-radius:50%;display:flex;align-items:center;justify-content:center">
      <svg width="20" height="20" viewBox="0 0 24 24" style="fill:#ccc"><path d="M12 17.27L18.18 21l-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.73L5.82 21z"/></svg>
    </button>
    <button onclick="gmailTrash()" title="Delete" style="background:none;border:none;cursor:pointer;width:36px;height:36px;border-radius:50%;display:flex;align-items:center;justify-content:center">
      <svg width="20" height="20" viewBox="0 0 24 24" style="fill:#444746"><path d="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z"/></svg>
    </button>
    <div style="flex:1"></div>
    <button onclick="gmailRefresh()" title="Refresh" style="background:none;border:none;cursor:pointer;width:36px;height:36px;border-radius:50%;display:flex;align-items:center;justify-content:center">
      <svg width="20" height="20" viewBox="0 0 24 24" style="fill:#444746"><path d="M17.65 6.35C16.2 4.9 14.21 4 12 4c-4.42 0-7.99 3.58-7.99 8s3.57 8 7.99 8c3.73 0 6.84-2.55 7.73-6h-2.08c-.82 2.33-3.04 4-5.65 4-3.31 0-6-2.69-6-6s2.69-6 6-6c1.66 0 3.14.69 4.22 1.78L13 11h7V4l-2.35 2.35z"/></svg>
    </button>
    <button onclick="gmailCompose()" title="Compose" style="background:none;border:none;cursor:pointer;width:36px;height:36px;border-radius:50%;display:flex;align-items:center;justify-content:center">
      <svg width="20" height="20" viewBox="0 0 24 24" style="fill:#444746"><path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25zM20.71 7.04a1 1 0 0 0 0-1.41l-2.34-2.34a1 1 0 0 0-1.41 0l-1.83 1.83 3.75 3.75 1.83-1.83z"/></svg>
    </button>
  </div>
  <!-- No email selected placeholder -->
  <div id="gm-reader-placeholder" style="flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:16px">
    <div style="width:120px;height:120px;border-radius:50%;background:#e8f0fe;display:flex;align-items:center;justify-content:center">
      <svg width="56" height="56" viewBox="0 0 24 24" style="fill:#1a73e8;opacity:.7"><path d="M20 4H4c-1.1 0-1.99.9-1.99 2L2 18c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V6c0-1.1-.9-2-2-2zm0 4l-8 5-8-5V6l8 5 8-5v2z"/></svg>
    </div>
    <h3 style="font-size:20px;font-weight:400;color:var(--tx)">Select an email to read</h3>
    <p style="font-size:14px;color:#5f6368">Your emails load instantly from cache</p>
  </div>
  <!-- Email reader -->
  <div id="gmail-reader" style="display:none;flex-direction:column;height:100%;overflow:hidden">
    <div style="padding:20px 24px 14px;background:#fff;flex-shrink:0;border-bottom:1px solid #e0e0e0">
      <div id="gm-reader-subject" style="font-size:22px;font-weight:400;color:#202124;margin-bottom:14px;line-height:1.3"></div>
      <div style="display:flex;align-items:center;gap:12px">
        <div id="gm-reader-av" style="width:42px;height:42px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:17px;font-weight:600;color:#fff;flex-shrink:0"></div>
        <div style="flex:1;min-width:0">
          <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
            <div id="gm-reader-name" style="font-size:14px;font-weight:600;color:#202124"></div>
            <div id="gm-reader-addr" style="font-size:12px;color:#5f6368"></div>
          </div>
          <div id="gm-reader-to" style="font-size:12px;color:#5f6368;margin-top:2px"></div>
        </div>
        <div id="gm-reader-date" style="font-size:12px;color:#5f6368;white-space:nowrap;flex-shrink:0"></div>
      </div>
    </div>
    <!-- Email Body - supports HTML -->
    <div id="gm-reader-body" style="flex:1;overflow-y:auto;background:#fff"></div>
    <!-- Reply box -->
    <div style="padding:12px 16px 14px;background:#f6f8fc;border-top:1px solid #e0e0e0;flex-shrink:0">
      <div style="background:#fff;border:1px solid #e0e0e0;border-radius:12px;padding:12px 16px;box-shadow:0 1px 3px rgba(0,0,0,.08)">
        <div style="font-size:12px;color:#1a73e8;font-weight:500;margin-bottom:8px">↩ Reply to <span id="gm-reply-to-name"></span></div>
        <textarea id="gm-reply-input" rows="2" placeholder="Write a reply..." style="width:100%;border:none;outline:none;font-size:14px;resize:none;max-height:120px;font-family:inherit;color:#202124;background:transparent"></textarea>
        <div style="display:flex;justify-content:flex-end;gap:8px;margin-top:8px">
          <button onclick="gmailReply()" style="background:#1a73e8;color:#fff;border:none;border-radius:20px;padding:8px 20px;font-size:13px;font-weight:500;cursor:pointer;font-family:inherit;display:flex;align-items:center;gap:6px">
            <svg width="16" height="16" viewBox="0 0 24 24" style="fill:#fff"><path d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z"/></svg>
            Send
          </button>
        </div>
      </div>
    </div>
  </div>
</div>

<!-- GMAIL COMPOSE MODAL -->
<div id="gm-compose-modal" style="display:none;position:fixed;bottom:0;right:24px;width:480px;background:#fff;border-radius:12px 12px 0 0;box-shadow:0 8px 40px rgba(0,0,0,.4);z-index:600;flex-direction:column">
  <div style="background:#404040;color:#fff;padding:10px 16px;border-radius:12px 12px 0 0;display:flex;justify-content:space-between;align-items:center;font-size:14px;font-weight:500">
    New Message
    <button onclick="document.getElementById('gm-compose-modal').style.display='none'" style="background:none;border:none;color:#fff;cursor:pointer;font-size:20px;line-height:1">✕</button>
  </div>
  <div style="padding:0 16px;border-bottom:1px solid #e0e0e0">
    <input id="gm-to" placeholder="To" style="width:100%;padding:10px 4px;border:none;border-bottom:1px solid #e0e0e0;font-size:14px;outline:none;font-family:inherit;color:#202124;background:transparent">
    <input id="gm-subject" placeholder="Subject" style="width:100%;padding:10px 4px;border:none;font-size:14px;outline:none;font-family:inherit;color:#202124;background:transparent">
  </div>
  <textarea id="gm-body" placeholder="Write your email..." style="border:none;padding:12px 16px;font-size:14px;resize:none;height:200px;outline:none;font-family:inherit;color:#202124;width:100%"></textarea>
  <div style="padding:10px 16px;display:flex;justify-content:space-between;align-items:center;border-top:1px solid #e0e0e0">
    <button onclick="gmailSendCompose()" style="background:#1a73e8;color:#fff;border:none;border-radius:24px;padding:10px 28px;font-size:14px;font-weight:500;cursor:pointer;font-family:inherit">Send</button>
    <button onclick="document.getElementById('gm-compose-modal').style.display='none'" style="background:none;border:none;cursor:pointer;color:#5f6368;font-size:22px">🗑</button>
  </div>
</div>
  <div id="no-chat">
    <div class="nc-ring"><svg viewBox="0 0 24 24"><path d="M20 2H4c-1.1 0-2 .9-2 2v18l4-4h14c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2z"/></svg></div>
    <h2 class="nc-h">∷ᒷ⊣╎𝙹リᓭ 𝙹⎓ ᓵᔑ℘╎ℸ ̣ᔑꖎ</h2>
    <p class="nc-p">ᓭᒷリ↸ ᔑリ↸ ∷ᒷᓵᒷ╎⍊ᒷ ᒲᒷᓭᓭᔑ⊣ᒷᓭ ∴╎ℸ ̣⍑𝙹⚍ℸ ̣ ꖌᒷᒷ℘╎リ⊣ ||𝙹⚍∷ ℘⍑𝙹リᒷ 𝙹リꖎ╎リᒷ. ⚍ᓭᒷ ∴⍑ᔑℸ ̣ᓭᔑ℘℘ 𝙹リ ⚍℘ ℸ ̣𝙹 4 ꖎ╎リꖌᒷ↸ ↸ᒷ⍊╎ᓵᒷᓭ.</p>
    <div class="nc-enc"><svg viewBox="0 0 24 24"><path d="M18 8h-1V6c0-2.76-2.24-5-5-5S7 3.24 7 6v2H6c-1.1 0-2 .9-2 2v10c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2V10c0-1.1-.9-2-2-2zm-6 9c-1.1 0-2-.9-2-2s.9-2 2-2 2 .9 2 2-.9 2-2 2zM8.9 8V6c0-1.71 1.39-3.1 3.1-3.1 1.71 0 3.1 1.39 3.1 3.1v2H8.9z"/></svg>End-to-end encrypted</div>
  </div>
  <div id="active-chat">
    <div class="c-hdr">
      <button class="ch-back" onclick="closeChat()" title="Back to chats">‹</button>
      <div class="c-hdr-av u" id="c-av">?</div>
      <div class="c-hdr-info" onclick="UI.showInfo()">
        <div class="c-hdr-name" id="c-name">—</div>
        <div class="c-hdr-sub" id="c-sub">click here for contact info</div>
      </div>
      <div class="c-hdr-btns">
        <button class="chbtn" onclick="UI.search()"><svg viewBox="0 0 24 24"><path d="M15.5 14h-.79l-.28-.27A6.47 6.47 0 0 0 16 9.5 6.5 6.5 0 1 0 9.5 16c1.61 0 3.09-.59 4.23-1.57l.27.28v.79l5 4.99L20.49 19l-4.99-5zm-6 0C7.01 14 5 11.99 5 9.5S7.01 5 9.5 5 14 7.01 14 9.5 11.99 14 9.5 14z"/></svg><span class="tip">Search</span></button>
        <button class="chbtn" onclick="UI.filter()"><svg viewBox="0 0 24 24"><path d="M10 18h4v-2h-4v2zM3 6v2h18V6H3zm3 7h12v-2H6v2z"/></svg><span class="tip">Filter</span></button>
        <button class="chbtn" onclick="UI.stats()"><svg viewBox="0 0 24 24"><path d="M19 3H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm-7 14H7v-2h5v2zm5-4H7v-2h10v2zm0-4H7V7h10v2z"/></svg><span class="tip">Stats</span></button>
        <button class="chbtn" onclick="UI.sync()" title="Sync History"><svg viewBox="0 0 24 24"><path d="M12 4V1L8 5l4 4V6c3.31 0 6 2.69 6 6 0 1.01-.25 1.97-.7 2.8l1.46 1.46C19.54 15.03 20 13.57 20 12c0-4.42-3.58-8-8-8zm0 14c-3.31 0-6-2.69-6-6 0-1.01.25-1.97.7-2.8L5.24 7.74C4.46 8.97 4 10.43 4 12c0 4.42 3.58 8 8 8v3l4-4-4-4v3z"/></svg><span class="tip">Sync</span></button>
        <button class="chbtn" onclick="UI.export()"><svg viewBox="0 0 24 24"><path d="M19 9h-4V3H9v6H5l7 7 7-7zM5 18v2h14v-2H5z"/></svg><span class="tip">Export</span></button>
        <button class="chbtn" onclick="UI.markRead()"><svg viewBox="0 0 24 24"><path d="M18 7l-1.41-1.41-6.34 6.34 1.41 1.41L18 7zm4.24-1.41L11.66 16.17 7.48 12l-1.41 1.41L11.66 19l12-12-1.42-1.41zM.41 13.41L6 19l1.41-1.41L1.83 12 .41 13.41z"/></svg><span class="tip">Mark read</span></button>
      </div>
    </div>
    <div id="filter-bar">
      <input class="fi" id="fi-kw" placeholder="Keywords (comma)">
      <input class="fi" id="fi-rx" placeholder="Regex pattern">
      <input class="fi" id="fi-snd" placeholder="Sender">
      <select class="fi" id="fi-ty">
        <option value="">All types</option>
        <option value="chat">Text</option>
        <option value="image">Images</option>
        <option value="video">Video</option>
        <option value="document">Docs</option>
        <option value="audio">Audio</option>
      </select>
      <button class="fbtn go" onclick="applyFilter()">Apply</button>
      <button class="fbtn cl" onclick="clearFilter()">Clear</button>
      <span class="fct" id="fct"></span>
    </div>
    <div id="messages">
      <div id="lm"><div class="loader"></div></div>
    </div>
    <div id="reply-bar">
      <div class="rb-c">
        <div class="rb-s" id="rb-s"></div>
        <div class="rb-t" id="rb-t"></div>
      </div>
      <button class="rb-x" onclick="cancelReply()"><svg viewBox="0 0 24 24"><path d="M19 6.41L17.59 5 12 10.59 6.41 5 5 6.41 10.59 12 5 17.59 6.41 19 12 13.41 17.59 19 19 17.59 13.41 12z"/></svg></button>
    </div>
    <div id="mention-box" style="display:none"></div>
    <div id="att-preview">
      <img id="att-thumb" style="display:none">
      <span id="att-name"></span>
      <button id="att-remove" onclick="clearAttach()">✕</button>
    </div>
    <div id="input-area">
      <button id="attach-btn" class="att-btn" onclick="document.getElementById('file-input').click()" title="Attach image, video, audio or file">
        <svg viewBox="0 0 24 24"><path d="M16.5 6v11.5a4 4 0 1 1-8 0V5a2.5 2.5 0 0 1 5 0v10.5a1 1 0 1 1-2 0V6H10v9.5a2.5 2.5 0 0 0 5 0V5a4 4 0 0 0-8 0v12.5a5.5 5.5 0 0 0 11 0V6h-1.5z"/></svg>
      </button>
      <input id="file-input" type="file" accept="image/*,video/*,audio/*,application/*,.pdf,.doc,.docx,.xls,.xlsx,.txt" style="display:none" onchange="handleAttach(this.files)">
      <div class="iw">
        <textarea id="msg-input" rows="1" placeholder="Type a message"></textarea>
      </div>
      <button id="send-btn" onclick="send()">
        <svg viewBox="0 0 24 24"><path d="M1.101 21.757L23.8 12.028 1.101 2.3l.011 7.912 13.623 1.816-13.623 1.817-.011 7.912z"/></svg>
      </button>
    </div>
    <button id="s2b" onclick="scrollBot()"><svg viewBox="0 0 24 24"><path d="M7.41 8.59L12 13.17l4.59-4.58L18 10l-6 6-6-6 1.41-1.41z"/></svg><div id="ucb"></div></button>
    <!-- Search panel -->
    <div id="sp">
      <div class="sp-hdr">
        <h3>Search Messages</h3>
        <div class="sp-si">
          <svg viewBox="0 0 24 24"><path d="M15.5 14h-.79l-.28-.27A6.47 6.47 0 0 0 16 9.5 6.5 6.5 0 1 0 9.5 16c1.61 0 3.09-.59 4.23-1.57l.27.28v.79l5 4.99L20.49 19l-4.99-5zm-6 0C7.01 14 5 11.99 5 9.5S7.01 5 9.5 5 14 7.01 14 9.5 11.99 14 9.5 14z"/></svg>
          <input id="sp-in" placeholder="Search..." oninput="doSearch(this.value)">
        </div>
      </div>
      <div id="sr"><div class="sp-empty">Type to search messages</div></div>
      <button class="sp-close" onclick="UI.search()" style="position:absolute;top:12px;right:12px"><svg viewBox="0 0 24 24"><path d="M19 6.41L17.59 5 12 10.59 6.41 5 5 6.41 10.59 12 5 17.59 6.41 19 12 13.41 17.59 19 19 17.59 13.41 12z"/></svg></button>
    </div>
    <!-- Contact Info Panel -->
    <div id="ci-panel">
      <div class="sp-hdr" style="display:flex;align-items:center;gap:15px">
        <button class="sp-close" onclick="UI.showInfo()" style="position:static;margin:0;background:none"><svg viewBox="0 0 24 24" style="fill:#fff"><path d="M19 6.41L17.59 5 12 10.59 6.41 5 5 6.41 10.59 12 5 17.59 6.41 19 12 13.41 17.59 19 19 17.59 13.41 12z"/></svg></button>
        <h3 style="margin:0">Contact Info</h3>
      </div>
      <div id="ci-content">
        <div style="padding:40px;text-align:center;color:var(--tx2)">Loading info...</div>
      </div>
    </div>
    <!-- Stats panel -->
    <div id="stats-panel">
      <button class="sp-close" onclick="UI.stats()"><svg viewBox="0 0 24 24"><path d="M19 6.41L17.59 5 12 10.59 6.41 5 5 6.41 10.59 12 5 17.59 6.41 19 12 13.41 17.59 19 19 17.59 13.41 12z"/></svg></button>
      <h3 style="font-size:18px;font-weight:500;color:var(--tx);margin-bottom:20px">Chat Statistics</h3>
      <div id="stats-c"><div style="color:var(--tx2);font-size:14px">Loading...</div></div>
    </div>
  </div>
</div>
</div>

<!-- Context menu -->
<div id="ctx">
  <div class="cti" onclick="ctx_reply()"><svg viewBox="0 0 24 24"><path d="M10 9V5l-7 7 7 7v-4.1c5 0 8.5 1.6 11 5.1-1-5-4-10-11-11z"/></svg>Reply</div>
  <div class="cti" onclick="ctx_copy()"><svg viewBox="0 0 24 24"><path d="M16 1H4c-1.1 0-2 .9-2 2v14h2V3h12V1zm3 4H8c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h11c1.1 0 2-.9 2-2V7c0-1.1-.9-2-2-2zm0 16H8V7h11v14z"/></svg>Copy</div>
  <div class="ct-sep"></div>
  <div class="cti" onclick="ctx_sched()"><svg viewBox="0 0 24 24"><path d="M11.99 2C6.47 2 2 6.48 2 12s4.47 10 9.99 10C17.52 22 22 17.52 22 12S17.52 2 11.99 2zM12 20c-4.42 0-8-3.58-8-8s3.58-8 8-8 8 3.58 8 8-8 3.58-8 8zm.5-13H11v6l5.25 3.15.75-1.23-4.5-2.67z"/></svg>Schedule Check</div>
  <div class="cti" onclick="ctx_star()"><svg viewBox="0 0 24 24"><path d="M12 17.27L18.18 21l-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.73L5.82 21z"/></svg><span id="ctx-sl">Star</span></div>
  <div class="cti" onclick="ctx_info()"><svg viewBox="0 0 24 24"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-6h2v6zm0-8h-2V7h2v2z"/></svg>Info</div>
</div>

<!-- Report Modal -->
<div id="report-modal" class="qr-ov" style="display:none">
  <div class="rp-box">
    <h3>Select Group for Report</h3>
    <select id="rp-sel" class="rp-sel"><option>Loading...</option></select>
    <div class="rp-btns">
      <button class="fbtn cl" onclick="document.getElementById('report-modal').style.display='none'">Cancel</button>
      <button class="fbtn go" onclick="UI.openReport()">View Report</button>
    </div>
  </div>
</div>

<!-- Share Access Modal -->
<div id="share-modal" class="qr-ov" style="display:none">
  <div class="rp-box">
    <h3>Share WhatsApp Access</h3>
    <p style="font-size:13px;color:var(--tx2);margin:0">Scan this QR with any phone camera to open the web client on <b id="share-host"></b>. Anyone who opens it can view &amp; send from THIS WhatsApp number — one number, many devices.</p>
    <div id="share-qr" style="display:flex;justify-content:center;padding:8px"><div class="loader"></div></div>
    <div class="rp-btns">
      <button class="fbtn cl" onclick="document.getElementById('share-modal').style.display='none'">Close</button>
      <button class="fbtn go" onclick="copyShareLink()">Copy Link</button>
    </div>
  </div>
</div>

<!-- Scheduler Modal -->
<div id="sch-modal" class="qr-ov" style="display:none">
  <div class="sch-box">
    <h3 style="color:var(--tx)">Smart Scheduler</h3>
    <div class="sch-tabs">
      <div class="sch-tab active" onclick="UI.schTab('new')">New Task</div>
      <div class="sch-tab" onclick="UI.schTab('pending')">Pending</div>
      <div class="sch-tab" onclick="UI.schTab('history')">History</div>
    </div>
    
    <!-- NEW TASK VIEW -->
    <div id="sch-v-new">
      <select id="sch-who" class="sch-inp" style="margin-bottom:10px"><option>Loading contacts...</option></select>
      <textarea id="sch-msg" class="sch-inp" rows="3" placeholder="Message to send..." style="margin-bottom:10px"></textarea>
      <div class="sch-row" style="margin-bottom:10px">
        <input type="datetime-local" id="sch-time" class="sch-inp" step="1" title="Select date and time (seconds supported)">
      </div>
      <label style="display:flex;align-items:center;gap:8px;font-size:13px;cursor:pointer;color:var(--tx);margin-bottom:10px"><input type="checkbox" id="sch-cond"> Send ONLY if no reply from:</label>
      <select id="sch-cond-who" class="sch-inp" style="display:none;margin-bottom:10px"></select>
      <div class="rp-btns">
        <button class="fbtn cl" onclick="document.getElementById('sch-modal').style.display='none'">Close</button>
        <button class="fbtn go" onclick="UI.addTask()">Schedule Task</button>
      </div>
    </div>

    <!-- LIST VIEWS -->
    <div id="sch-list-cont" class="sch-list" style="display:none"></div>
  </div>
</div>

<!-- QR modal -->
<div id="qr-modal" class="qr-ov" style="display:none">
  <div class="qr-box">
    <div class="qr-logo">💬</div>
    <h2>Use WhatsApp on your computer</h2>
    <p>Open WhatsApp on your phone and scan this code to link your device.</p>
    <div id="qr-img"></div>
    <div class="qr-steps">
      <div class="qr-step"><div class="qr-step-n">1</div>Open WhatsApp on your phone</div>
      <div class="qr-step"><div class="qr-step-n">2</div>Tap Menu ⋮ or Settings → Linked Devices</div>
      <div class="qr-step"><div class="qr-step-n">3</div>Tap "Link a Device" and scan this QR code</div>
    </div>
  </div>
</div>
<div id="tc"></div>

<script>
const socket = io();

let S = {
  chats:[], tab:'all',
  chatId:null, chatName:'', isGroup:false,
  msgs:[], msgIds:new Set(),
  hasMore:true, loadingOlder:false,
  replyTo:null, ctxMsg:null,
  unreadNew:0, atBottom:true,
  filterFn:null, participants:[], mentions:[],
  schTab:'new', schTasks:[]
};

// ── ADVANCED SCROLL LOADER ──────────────────────────────
const observer = new IntersectionObserver((entries) => {
  if (entries[0].isIntersecting) {
    if(!S.loadingOlder && S.hasMore) loadOlder();
  }
}, {
  root: document.getElementById('messages')
});

// ── SOCKET ──────────────────────────────────────────────
socket.on('qr', url => {
  document.getElementById('qr-modal').style.display = 'flex';
  document.getElementById('qr-img').innerHTML = '<img src="'+url+'" style="width:240px;height:240px;border-radius:8px;margin-top:16px">';
});
socket.on('ready', () => {
  document.getElementById('qr-modal').style.display = 'none';
  socket.emit('get_chats');
  toast('Connected ✓');
});
socket.on('wa_disconnected', () => toast('Disconnected — reconnecting...'));
socket.on('wa_logged_out', () => {
  toast('Session unlinked. Scan the new QR to reconnect.');
  const b = document.getElementById('state-bar');
  if (b) { b.textContent = 'Session unlinked — scan the QR code to reconnect'; b.style.display = 'block'; }
});

socket.on('chats_list', list => { 
    S.chats = list; 
    renderChats(); 
    // Force update header name if active chat is open
    if (S.chatId) {
        const c = S.chats.find(x => x.id === S.chatId);
        if (c) document.getElementById('c-name').textContent = c.name;
    }
});

socket.on('chat_opened', ({ messages, hasMore, participants }) => {
  S.msgs = messages;
  S.msgIds = new Set(messages.map(m => m.id));
  S.hasMore = hasMore;
  S.unreadNew = 0;
  S.participants = participants || [];
  renderAll();
  document.getElementById('lm').style.display = hasMore ? 'block' : 'none';
  scrollBot(true);
});

socket.on('chat_deleted', id => {
    if(S.chatId === id) { document.getElementById('active-chat').style.display='none'; document.getElementById('no-chat').style.display='flex'; S.chatId=null; }
    socket.emit('get_chats');
});

socket.on('contact_details', d => {
  const el = document.getElementById('ci-content');
  const av = d.picUrl ? '<img src="'+d.picUrl+'">' : (d.name||'?')[0].toUpperCase();
  const sub = d.isGroup ? 'Group · '+d.groupParticipants+' participants' : d.number;
  const aboutTitle = d.isGroup ? 'Description' : 'About';
  
  let mediaHtml = '<div style="padding:15px;text-align:center;font-size:13px;color:var(--tx2)">No recent media</div>';
  if(d.media && d.media.length) {
    mediaHtml = '<div class="ci-media-grid">' + d.media.slice(0,6).map(m => 
      '<div class="ci-m-item" onclick="viewImg(\''+m.id+'\')">'+
      (m.mediaType.startsWith('video') ? '<video src="'+m.mediaUrl+'"></video>' : '<img src="'+m.mediaUrl+'">')
      +'</div>'
    ).join('') + '</div>';
    if (d.media.length > 6) mediaHtml += '<div style="padding:10px 0;text-align:center;color:var(--g);font-size:13px;cursor:pointer">View all ('+d.media.length+')</div>';
  }

  const checked = d.isCallBlocked ? 'checked' : '';
  el.innerHTML = '<div class="ci-hero"><div class="ci-big-av">'+av+'</div><div class="ci-t1">'+esc(d.name)+' <svg class="ci-edit" onclick="UI.rename(\''+d.id+'\',\''+esc(d.name)+'\')" viewBox="0 0 24 24"><path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25zM20.71 7.04c.39-.39.39-1.02 0-1.41l-2.34-2.34c-.39-.39-1.02-.39-1.41 0l-1.83 1.83 3.75 3.75 1.83-1.83z"/></svg></div><div class="ci-t2">'+sub+'</div></div>'
    + '<div class="ci-sect" style="display:flex;align-items:center"><label class="switch"><input type="checkbox" '+checked+' onchange="UI.toggleCallBlock(\''+d.id+'\',this)"><span class="slider"></span></label><span>Auto-reject calls</span></div>'
    + '<div class="ci-sect"><h4>'+aboutTitle+'</h4><div class="ci-body">'+esc(d.about||'~')+'</div></div>'
    + '<div class="ci-sect"><h4>Media, Links and Docs</h4>'+mediaHtml+'</div>'
    + '<div class="ci-act" onclick="UI.block(\''+d.id+'\')"><svg viewBox="0 0 24 24"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-2h2v2zm0-4h-2V7h2v6z"/></svg> Block '+esc(d.name)+'</div>'
    + '<div style="text-align:center; margin:20px 0;">'
    + '    <button onclick="refreshCurrentPic()" style="padding:10px 20px; background:#00a884; color:white; border:none; border-radius:20px; cursor:pointer; font-weight:500;">'
    + '        Refresh Profile Picture'
    + '    </button>'
    + '</div>'
    + '<div class="ci-act" onclick="UI.deleteChat(\''+d.id+'\')"><svg viewBox="0 0 24 24"><path d="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z"/></svg> Delete chat</div>';
});

socket.on('wa_state', state => {
  let b = document.getElementById('state-bar');
  if (state === 'CONNECTED') {
    if(b) b.style.display = 'none';
  } else {
    if (!b) {
      b = document.createElement('div'); b.id = 'state-bar';
      b.style.cssText = 'background:#e53935;color:#fff;padding:6px;text-align:center;font-size:12px;font-weight:600';
      document.querySelector('.s-hdr').after(b);
    }
    b.textContent = state; b.style.display = 'block';
  }
});

socket.on('older_messages', ({ messages, hasMore }) => {
  S.loadingOlder = false;
  const el = document.getElementById('messages');
  const prevH = el.scrollHeight;
  const prevT = el.scrollTop; // Capture scroll top BEFORE DOM changes

  const fresh = messages.filter(m => !S.msgIds.has(m.id));
  
  // STOP INFINITE LOOP: If server says hasMore but returned 0 fresh messages, stop.
  if (fresh.length === 0 && hasMore) {
      hasMore = false;
  }

  fresh.forEach(m => S.msgIds.add(m.id));
  S.msgs = [...fresh, ...S.msgs];
  S.hasMore = hasMore;
  if (fresh.length) {
    prependMsgs(fresh);
    el.scrollTop = prevT + (el.scrollHeight - prevH);
  }
  document.getElementById('lm').style.display = hasMore ? 'block' : 'none';
  if (!hasMore) toast('All messages loaded ✓');
});

socket.on('group_participants', ({ chatId, participants }) => {
    // If scheduler modal is open and matching chat
    if (document.getElementById('sch-modal').style.display !== 'none' && document.getElementById('sch-who').value === chatId) {
        const condSelect = document.getElementById('sch-cond-who');
        condSelect.innerHTML = '<option value="">Anyone in group</option>' 
            + participants.map(p => {
                // Try to find a name from our chats list, or custom names
                const name = S.chats.find(c=>c.id===p.id)?.name || p.id.split('@')[0];
                return '<option value="'+p.id+'">'+esc(name)+'</option>';
            }).join('');
    }
});

socket.on('names_updated', () => {
    if (S.chatId) {
        socket.emit('open_chat', { chatId: S.chatId }); // Reload current chat to refresh names
    }
    socket.emit('get_chats'); // Refresh sidebar
});

socket.on('new_message', msg => {
  const c = S.chats.find(x => x.id === msg.chatId);
  if (c) {
    c.lastMsg = msg.body;
    c.lastMsgTime = msg.timestamp;
    c.lastMsgType = msg.type;
    c.lastMsgFromMe = msg.isOutgoing;
    if (msg.chatId !== S.chatId) c.unreadCount = (c.unreadCount||0)+1;
    S.chats.sort((a,b) => (a.isPinned===b.isPinned) ? b.lastMsgTime-a.lastMsgTime : (a.isPinned?-1:1));
    renderChats();
  }
  
  if (msg.chatId === S.chatId) {
    if (S.msgIds.has(msg.id)) return;
    S.msgIds.add(msg.id); S.msgs.push(msg);
    appendMsg(msg);
    if (!S.atBottom && !msg.isOutgoing) {
      S.unreadNew++;
      const b = document.getElementById('ucb');
      b.textContent = S.unreadNew; b.className = 'show';
      document.getElementById('s2b').className = 'show';
    } else if (!msg.isOutgoing) {
      socket.emit('mark_read', { chatId: S.chatId });
    }
  }
});

socket.on('msg_ack_change', ({ id, ack }) => {
    const bubble = document.querySelector('[data-id="'+id+'"]');
    if (!bubble) return;
    const t1 = bubble.querySelector('.tick-1');
    const t2 = bubble.querySelector('.tick-2');
    const color = ack >= 3 ? '#53bdeb' : '#8696a0';
    
    if(t1) t1.style.fill = color;
    if(t2) {
        t2.style.fill = color;
        t2.style.display = ack >= 2 ? 'block' : 'none';
    }
});

socket.on('msg_revoked', ({ id }) => {
    const el = document.querySelector('[data-id="'+id+'"] .bubble');
    if (el) {
        el.innerHTML = '<div class="revoked"><svg viewBox="0 0 24 24" style="width:16px;height:16px;fill:var(--tx2)"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm0 18c-4.41 0-8-3.59-8-8s3.59-8 8-8 8 3.59 8 8-3.59 8-8 8zm-1-13h2v6h-2zm0 8h2v2h-2z"/></svg> This message was deleted</div><div class="b-foot"><span class="b-time"></span></div>';
    }
});

socket.on('chat_metadata', ({ chatId, url, name, number }) => {
    let c = S.chats.find(x => x.id === chatId);
    if (!c && chatId) {
        // Add stub entry so pic shows when chat list refreshes
        c = { id: chatId, picUrl: null, name: name || '' };
        S.chats.push(c);
    }
    if (c) {
        if(url) c.picUrl = url;
        if(name) c.name = name;
        if(!name && number) c.name = '+' + number;
        
        renderChats(); // Update sidebar
        // If active chat, update header
        if (S.chatId === chatId) {
            if(url) {
                const av = document.getElementById('c-av');
                av.innerHTML = '<img src="'+url+'" onerror="this.parentElement.textContent=\''+(c.name||'?')[0].toUpperCase()+'\'">';
            }
            if(c.name) document.getElementById('c-name').textContent = c.name;
        }
    }
});

socket.on('search_results', res => {
  const el = document.getElementById('sr');
  const q = document.getElementById('sp-in').value;
  if (!res.length) { el.innerHTML = '<div class="sp-empty">No results found</div>'; return; }
  el.innerHTML = res.reverse().map(m => {
    const body = hlQ(esc(m.body||'[media]'), q);
    return '<div class="sr-item" onclick="jumpTo(\''+m.id+'\')">'
      +'<div class="sr-sender">'+esc(m.senderName)+'</div>'
      +'<div class="sr-body">'+body+'</div>'
      +'<div class="sr-time">'+dtStr(m.timestamp)+'</div></div>';
  }).join('');
});

socket.on('chat_stats', renderStats);
socket.on('export_ready', ({ content, format }) => {
  const a = document.createElement('a');
  a.href = URL.createObjectURL(new Blob([content],{type:'text/plain'}));
  a.download = S.chatName.replace(/[^a-z0-9]/gi,'_')+'.'+(format==='csv'?'csv':'txt');
  a.click(); toast('Exported ✓');
});
socket.on('sync_complete', n => toast('Synced '+n+' old messages ✓'));
socket.on('ghost_status', isGhost => {
  const ic = document.getElementById('gh-icon');
  if (ic) {
    ic.style.fill = isGhost ? '#34b7f1' : 'var(--hicon)'; // Blue when active
    ic.innerHTML = isGhost ? '<path d="M12 6c3.79 0 7.17 2.13 8.82 5.5-.59 1.22-1.42 2.27-2.41 3.12l1.41 1.41c1.39-1.23 2.49-2.77 3.18-4.53C21.27 7.11 17 4 12 4c-1.27 0-2.49.2-3.64.57l1.65 1.65C10.66 6.09 11.32 6 12 6zm-1.07 1.14L13 9.21c.57.25 1.03.71 1.28 1.28l2.07 2.07c.08-.34.14-.7.14-1.07C16.5 9.01 14.99 7.5 13.14 7.14zM2.01 3.87l2.68 2.68C3.06 7.83 1.77 9.53 1 11.5 2.73 15.89 7 19 12 19c1.52 0 3.04-.3 4.46-.86l2.68 2.68 1.41-1.41L3.42 2.46 2.01 3.87zm7.5 7.5l2.61 2.61c-.04.01-.08.02-.12.02-1.38 0-2.5-1.12-2.5-2.5 0-.05.01-.08.01-.13zm-3.4-3.4l1.75 1.75c-.23.55-.36 1.15-.36 1.78 0 2.48 2.02 4.5 4.5 4.5.63 0 1.23-.13 1.77-.36l1.2 1.2c-.9.4-1.89.66-2.97.66-4.97 0-9-4.03-9-9 0-1.08.26-2.07.66-2.97z"/>' : '<path d="M12 4.5C7 4.5 2.73 7.61 1 12c1.73 4.39 6 7.5 11 7.5s9.27-3.11 11-7.5c-1.73-4.39-6-7.5-11-7.5zM12 17c-2.76 0-5-2.24-5-5s2.24-5 5-5 5 2.24 5 5-2.24 5-5 5zm0-8c-1.66 0-3 1.34-3 3s1.34 3 3 3 3-1.34 3-3-1.34-3-3-3z"/>';
  }
});
socket.on('schedule_updated', tasks => { S.schTasks = tasks; UI.renderSch(); });
socket.on('number_verified', ({ id }) => openChat(id, formatPhone(id.split('@')[0]), false));
socket.on('number_error', msg => toast('Error: '+msg));

// ── CHATS ────────────────────────────────────────────────
function setTab(t, el) {
  S.tab = t;
  document.querySelectorAll('.stab').forEach(x=>x.classList.remove('on'));
  el.classList.add('on'); renderChats();
}
function renderChats() {
  const q = document.getElementById('chat-search').value.toLowerCase();
  const c = document.getElementById('chats-list');
  const list = S.chats.filter(ch => {
    if (S.tab==='unread' && !ch.unreadCount) return false;
    if (S.tab==='groups' && !ch.isGroup) return false;
    if (S.tab==='personal' && ch.isGroup) return false;
    if (q && !ch.name.toLowerCase().includes(q)) return false;
    return true;
  });
  if (!list.length) { c.innerHTML = '<div style="padding:30px;text-align:center;color:var(--tx2);font-size:14px">No chats</div>'; return; }
  c.innerHTML = list.map(ch => {
    const av = ch.picUrl 
      ? '<img src="'+ch.picUrl+'" onerror="this.parentElement.innerHTML=\''+(ch.name||'?')[0].toUpperCase()+'\'">' 
      : (ch.name||'?')[0].toUpperCase();
    const g = ch.isGroup?'g':'u';
    const ub = ch.unreadCount ? '<span class="ubadge">'+(ch.unreadCount>99?'99+':ch.unreadCount)+'</span>' : '';
    const pin = ch.isPinned ? '<span style="color:var(--tx2);margin-right:4px">📌</span>' : '';
    const prev = prvText(ch);
    const tc = ch.unreadCount ? 'ci-time u' : 'ci-time';
    
    // Advanced Number Formatting
    let dispName = esc(ch.name);
    const rawNum = ch.id.split('@')[0];
    if(dispName === rawNum) {
        dispName = formatPhone(rawNum);
    }

    return '<div class="ci'+(ch.id===S.chatId?' on':'')+'" onclick="openChat(\''+ch.id+'\',\''+dispName.replace(/'/g,"\\'")+'\',' +ch.isGroup+')" oncontextmenu="event.preventDefault();UI.togglePin(\''+ch.id+'\','+ch.isPinned+')">'
      +'<div class="ci-av '+g+'">'+av+'</div>'
      +'<div class="ci-info">'
        +'<div class="ci-top"><span class="ci-name">'+pin+dispName+'</span><span class="'+tc+'">'+tFmt(ch.lastMsgTime)+'</span></div>'
        +'<div class="ci-bot"><span class="ci-prev">'+esc(prev)+'</span><div>'+ub+'</div></div>'
      +'</div></div>';
  }).join('');
}
function prvText(ch) {
  const icons = {image:'📷 Photo',video:'🎥 Video',audio:'🎵 Audio',document:'📄 Document',sticker:'🎭 Sticker',ptt:'🎤 Voice'};
  return (ch.lastMsgFromMe?'You: ':'') + (icons[ch.lastMsgType] || ch.lastMsg || (ch.isGroup?'Group':''));
}

function handleSearch(q) {
    renderChats();
    const c = document.getElementById('chats-list');
    // If input looks like a phone number (7+ digits) and not in list
    if (q.match(/^\+?\d{7,}$/) && !S.chats.some(x=>x.name.includes(q) || x.id.includes(q))) {
        const btn = document.createElement('div');
        btn.className = 'ci';
        btn.style.borderTop = '1px solid var(--brdr)';
        btn.innerHTML = '<div class="ci-av u" style="background:#00a884">+</div><div class="ci-info"><div class="ci-name">Start chat with '+q+'</div><div class="ci-bot" style="color:var(--g)">Click to verify and open</div></div>';
        btn.onclick = () => {
            toast('Verifying number...');
            socket.emit('check_number', { number: q });
        };
        c.insertBefore(btn, c.firstChild);
    }
}

function formatPhone(num) {
    if(!num || isNaN(num)) return num;
    if(num.length > 10) {
        // Try to split Country Code (approximate 1-3 digits)
        // Generic formatter: +CC NNN NNN NNNN
        return '+' + num.slice(0,3) + ' ' + num.slice(3,6) + ' ' + num.slice(6,9) + ' ' + num.slice(9);
    }
    return '+' + num;
}

// ── OPEN CHAT ────────────────────────────────────────────
function openChat(id, name, isGroup, el) {
  if (S.chatId === id) return;
  S.chatId = id; S.chatName = name; S.isGroup = isGroup;
  S.msgs = []; S.msgIds = new Set(); S.hasMore = true;
  S.replyTo = null; S.unreadNew = 0;
  cancelReply(); S.filterFn = null;
  const c = S.chats.find(x=>x.id===id); if(c) c.unreadCount = 0; S.participants = [];
  renderChats();

  document.getElementById('no-chat').style.display = 'none';
  const ac = document.getElementById('active-chat');
  ac.style.display = 'flex'; ac.style.flexDirection = 'column'; ac.style.height = '100%';

  document.getElementById('c-av').className = 'c-hdr-av '+(isGroup?'g':'u');
  // Check for pic
  const chatObj = S.chats.find(x=>x.id===id);
  if(chatObj && chatObj.picUrl) document.getElementById('c-av').innerHTML = '<img src="'+chatObj.picUrl+'" onerror="this.parentElement.textContent=\''+name[0].toUpperCase()+'\'">';
  else document.getElementById('c-av').textContent = name[0].toUpperCase();

  // Fix 6: Immediate fetch request
  if (!chatObj?.picUrl) socket.emit('fetch_pic', { chatId: id });

  document.getElementById('c-name').textContent = name;
  document.getElementById('c-sub').textContent = isGroup ? 'Group chat' : 'click here for contact info';
  document.getElementById('messages').innerHTML = '<div class="empty-msgs">⏳ Loading...</div>';

  socket.emit('set_active_chat', id);
  socket.emit('open_chat', { chatId: id });
  socket.emit('mark_read', { chatId: id });

  document.getElementById('sp').classList.remove('open');
  document.getElementById('stats-panel').classList.remove('open');
  document.getElementById('filter-bar').classList.remove('show');
  document.getElementById('ci-panel').classList.remove('open');
  clearFilter();
  document.getElementById('ucb').className = '';
  document.getElementById('s2b').className = '';
  // Mobile: slide sidebar out, slide chat in
  document.getElementById('sidebar').classList.add('chat-open');
  document.getElementById('chatview').classList.add('show');
}

function closeChat() {
  document.getElementById('sidebar').classList.remove('chat-open');
  document.getElementById('chatview').classList.remove('show');
}

// ── LOAD OLDER ───────────────────────────────────────────
function loadOlder() {
  if (!S.hasMore || S.loadingOlder || !S.chatId) return;
  S.loadingOlder = true;
  
  // Advanced: Failsafe timeout to prevent infinite spinning
  setTimeout(() => {
      if(S.loadingOlder) { S.loadingOlder = false; document.getElementById('lm').style.display = 'none'; }
  }, 15000);

  socket.emit('load_older', { chatId: S.chatId });
}
document.getElementById('messages').addEventListener('scroll', function() {
  S.atBottom = this.scrollHeight - this.scrollTop - this.clientHeight < 80;
  document.getElementById('s2b').className = S.atBottom ? '' : 'show';
  if (S.atBottom) { S.unreadNew=0; document.getElementById('ucb').className=''; document.getElementById('s2b').className=''; }
});

// ── RENDER ───────────────────────────────────────────────
function renderAll() {
  const el = document.getElementById('messages');
  el.innerHTML = '<div id="lm"><div class="loader"></div></div>';
  observer.observe(document.getElementById('lm'));
  const msgs = S.filterFn ? S.msgs.filter(S.filterFn) : S.msgs;
  if (!msgs.length) { el.innerHTML = '<div class="empty-msgs">No messages</div>'; return; }
  let lastDate = null;
  const frag = document.createDocumentFragment();
  msgs.forEach(m => {
    const ts = typeof m.timestamp === 'object' ? (m.timestamp.low || 0) : Number(m.timestamp) || 0;
    const ds = ts ? dStr(new Date(ts < 9999999999 ? ts*1000 : ts)) : 'Unknown Date';
    if (ds !== lastDate) { lastDate = ds; frag.appendChild(mkDateDiv(ds)); }
    frag.appendChild(mkBubble(m));
  });
  el.appendChild(frag);
}
function prependMsgs(msgs) {
  const el = document.getElementById('messages');
  const lmEl = document.getElementById('lm');
  const frag = document.createDocumentFragment();
  let ld = null;
  const firstDate = el.querySelector('.date-div')?.dataset?.d;
  msgs.forEach(m => {
    const ds = dStr(new Date(m.timestamp*1000));
    if (ds !== ld) { ld = ds; if (ds !== firstDate) frag.appendChild(mkDateDiv(ds)); }
    frag.appendChild(mkBubble(m));
  });
  lmEl.after(frag);
}
function appendMsg(m) {
  const el = document.getElementById('messages');
  const ds = dStr(new Date(m.timestamp*1000));
  const last = [...el.querySelectorAll('.date-div')].pop();
  if (!last || last.dataset.d !== ds) el.appendChild(mkDateDiv(ds));
  el.appendChild(mkBubble(m));
  if (S.atBottom) el.scrollTop = el.scrollHeight;
}
function mkDateDiv(ds) {
  const d = document.createElement('div');
  d.className = 'date-div'; d.dataset.d = ds;
  d.innerHTML = '<span>'+ds+'</span>'; return d;
}
function mkBubble(m) {
  const w = document.createElement('div');
  w.className = 'mw '+(m.isOutgoing?'out':'in');
  w.dataset.id = m.id;
  w.addEventListener('contextmenu', e => { e.preventDefault(); showCtx(e, m); });

  const sender = (!m.isOutgoing && m.senderName) ? 
    '<div class="b-sender" onclick="event.stopPropagation();UI.rename(\''+m.from+'\',\''+esc(m.senderName)+'\')" title="Click to rename '+m.from+'">'+esc(m.senderName)+'</div>' : '';
  const reply = (m.hasQuotedMsg && m.quotedMsg) ?
    '<div class="b-reply"><div class="b-reply-s">Quoted</div><div class="b-reply-t">'+esc(m.quotedMsg.body)+'</div></div>' : '';

  let media = '';
  if (m.mediaUrl) {
    if (m.mediaType?.startsWith('image/')) media = '<img class="m-img" src="'+m.mediaUrl+'" loading="lazy" onclick="viewImg(\''+m.id+'\')">';
    else if (m.mediaType?.startsWith('video/')) media = '<video class="m-vid" controls><source src="'+m.mediaUrl+'" type="'+m.mediaType+'"></video>';
    else if (m.mediaType?.startsWith('audio/')) media = '<audio class="m-audio" controls><source src="'+m.mediaUrl+'" type="'+m.mediaType+'"></audio>';
    else media = '<a class="m-doc" href="'+m.mediaUrl+'" download><svg viewBox="0 0 24 24"><path d="M14 2H6c-1.1 0-2 .9-2 2v16c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2V8l-6-6zm4 18H6V4h7v5h5v11z"/></svg>Download File</a>';
  }

  const viewOnceBadge = m.isViewOnce ? '<span style="color:#25d366;font-weight:bold;margin-right:5px" title="View Once Media">➊</span>' : '';

  const body = m.body ? '<div class="b-body">'+viewOnceBadge+esc(m.body)+'</div>' : (m.isViewOnce ? '<div class="b-body">'+viewOnceBadge+'View Once Media</div>' : '');
  const star = m.isStarred ? '<span class="b-star">★</span>' : '';
  const time = t12(m.timestamp);
  
  const c = m.ack >= 3 ? '#53bdeb' : '#8696a0';
  const t1 = '<path class="tick-1" d="M15.01 3.316l-.478-.372a.365.365 0 0 0-.51.063L8.666 9.88a.32.32 0 0 1-.484.033L5.208 6.889a.37.37 0 0 0-.626.029l-.378.567a.819.819 0 0 0 .33.483l3.79 2.87a.32.32 0 0 0 .484-.032l6.658-8.045a.366.366 0 0 0-.065-.513z" fill="'+c+'"/>';
  const t2 = '<path class="tick-2" style="display:'+(m.ack>=2?'block':'none')+'" d="M10.91 3.316l-.478-.372a.365.365 0 0 0-.51.063L3.566 9.88a.32.32 0 0 1-.484.033L1.891 7.769a.37.37 0 0 0-.626.029l-.378.567a.819.819 0 0 0 .33.483l3.79 2.87a.32.32 0 0 0 .484-.032l6.67-8.045a.365.365 0 0 0-.065-.513z" fill="'+c+'"/>';
  
  const tick = m.isOutgoing ? '<span class="tick"><svg viewBox="0 0 16 11">'+t1+t2+'</svg></span>' : '';

  w.innerHTML = '<div class="bubble">'+sender+reply+media+body+'<div class="b-foot">'+star+'<span class="b-time">'+time+'</span>'+tick+'</div></div>';
  return w;
}

// ── FILTER ───────────────────────────────────────────────
function applyFilter() {
  const kws = document.getElementById('fi-kw').value.split(',').map(s=>s.trim()).filter(Boolean);
  const rxS = document.getElementById('fi-rx').value.trim();
  const snd = document.getElementById('fi-snd').value.toLowerCase();
  const ty = document.getElementById('fi-ty').value;
  let rx = null; try { if(rxS) rx = new RegExp(rxS,'i'); } catch {}
  S.filterFn = m => {
    const body = (m.body||'').toLowerCase(), sender = (m.senderName||'').toLowerCase();
    if (ty && m.type !== ty) return false;
    if (snd && !sender.includes(snd)) return false;
    if (kws.length && !kws.some(k => body.includes(k.toLowerCase()))) return false;
    if (rx && !rx.test(m.body||'')) return false;
    return true;
  };
  const filtered = S.msgs.filter(S.filterFn);
  document.getElementById('fct').textContent = filtered.length+' of '+S.msgs.length;
  renderAll(); scrollBot(true);
}
function clearFilter() {
  S.filterFn = null;
  ['fi-kw','fi-rx','fi-snd'].forEach(id => document.getElementById(id).value = '');
  document.getElementById('fi-ty').value = '';
  document.getElementById('fct').textContent = '';
  if (S.msgs.length) renderAll();
}

// ── SEARCH ───────────────────────────────────────────────
let stimer = null;
function doSearch(q) {
  if (!q.trim()) { document.getElementById('sr').innerHTML = '<div class="sp-empty">Type to search messages</div>'; return; }
  clearTimeout(stimer);
  stimer = setTimeout(() => {
    document.getElementById('sr').innerHTML = '<div class="sp-empty">Searching...</div>';
    socket.emit('search_messages', { chatId: S.chatId, query: q });
  }, 400);
}
function jumpTo(id) {
  const el = document.querySelector('[data-id="'+id+'"]');
  if (el) { el.scrollIntoView({behavior:'smooth',block:'center'}); el.querySelector('.bubble').style.outline = '2px solid var(--g)'; setTimeout(()=>el.querySelector('.bubble').style.outline='',2000); }
  else toast('Message not in current view');
}

// ── STATS ─────────────────────────────────────────────────
function renderStats(d) {
  const mx = d.senders[0]?.[1]||1, mxH = Math.max(...d.hourly,1);
  document.getElementById('stats-c').innerHTML =
    '<div class="stats-grid">'
    +'<div class="sc"><div class="sc-n">'+d.total+'</div><div class="sc-l">Total Messages</div></div>'
    +'<div class="sc"><div class="sc-n">'+d.mediaCount+'</div><div class="sc-l">Media Files</div></div>'
    +'<div class="sc"><div class="sc-n">'+d.textCount+'</div><div class="sc-l">Text Messages</div></div>'
    +'<div class="sc"><div class="sc-n">'+d.avgLength+'</div><div class="sc-l">Avg Chars</div></div>'
    +'</div>'
    +'<div class="ss"><h4>Top Senders</h4>'
    +d.senders.map(([n,c])=>'<div class="sb"><div class="sb-name">'+esc(n)+'</div><div class="sb-track"><div class="sb-fill" style="width:'+Math.round(c/mx*100)+'%"></div></div><div class="sb-count">'+c+'</div></div>').join('')
    +'</div>'
    +'<div class="ss"><h4>Activity by Hour</h4>'
    +'<div class="hc">'+d.hourly.map((h,i)=>'<div class="hcb" style="height:'+Math.max(2,Math.round(h/mxH*56))+'px" title="'+i+':00 — '+h+'"></div>').join('')+'</div>'
    +'<div class="hcl"><span>12am</span><span>6am</span><span>12pm</span><span>6pm</span><span>11pm</span></div>'
    +'</div>';
}

// ── CONTEXT MENU ─────────────────────────────────────────
let ctxMsg = null;
function showCtx(e, m) {
  ctxMsg = m;
  document.getElementById('ctx-sl').textContent = m.isStarred ? 'Unstar' : 'Star';
  const menu = document.getElementById('ctx');
  menu.style.cssText = 'display:block;left:'+Math.min(e.clientX,window.innerWidth-180)+'px;top:'+Math.min(e.clientY,window.innerHeight-180)+'px';
}
document.addEventListener('click', () => document.getElementById('ctx').style.display = 'none');
function ctx_reply() {
  if (!ctxMsg) return;
  S.replyTo = ctxMsg;
  document.getElementById('reply-bar').className = 'show';
  document.getElementById('rb-s').textContent = ctxMsg.senderName;
  document.getElementById('rb-t').textContent = ctxMsg.body || '[media]';
  document.getElementById('msg-input').focus();
}
function ctx_copy() { if (ctxMsg?.body) { navigator.clipboard.writeText(ctxMsg.body); toast('Copied'); } }
function ctx_star() {
  if (!ctxMsg) return;
  ctxMsg.isStarred = !ctxMsg.isStarred;
  socket.emit('star_message', { chatId: S.chatId, msgId: ctxMsg.id, star: ctxMsg.isStarred });
  const foot = document.querySelector('[data-id="'+ctxMsg.id+'"] .b-foot');
  if (foot) {
    const existing = foot.querySelector('.b-star');
    if (ctxMsg.isStarred) { if (!existing) foot.insertAdjacentHTML('afterbegin','<span class="b-star">★</span>'); }
    else { existing?.remove(); }
  }
  toast(ctxMsg.isStarred ? '★ Starred' : 'Unstarred');
}
function ctx_info() {
  if (ctxMsg) toast('Sent: '+dtStr(ctxMsg.timestamp)+' · Type: '+ctxMsg.type);
}
function cancelReply() { S.replyTo = null; document.getElementById('reply-bar').className = ''; }

// ── SEND ─────────────────────────────────────────────────
let pendingAttach = null; // {data, mime, filename, ptt}
function handleAttach(files) {
  const f = files && files[0]; if (!f) return;
  const reader = new FileReader();
  reader.onload = () => {
    pendingAttach = { data: reader.result, mime: f.type || 'application/octet-stream', filename: f.name, ptt: f.type?.startsWith('audio/') };
    const pv = document.getElementById('att-preview');
    pv.classList.add('show');
    document.getElementById('att-name').textContent = f.name;
    const thumb = document.getElementById('att-thumb');
    if (f.type?.startsWith('image/')) { thumb.src = reader.result; thumb.style.display = 'block'; }
    else { thumb.style.display = 'none'; }
  };
  reader.readAsDataURL(f);
}
function clearAttach() {
  pendingAttach = null;
  document.getElementById('att-preview').classList.remove('show');
  document.getElementById('file-input').value = '';
}
function send() {
  if (!S.chatId) return;
  const inp = document.getElementById('msg-input');
  const text = inp.value.trim();
  if (pendingAttach) {
    const a = pendingAttach;
    const asDocument = !a.mime.startsWith('image/') && !a.mime.startsWith('video/') && !a.mime.startsWith('audio/');
    socket.emit('send_media', { chatId: S.chatId, data: a.data, mime: a.mime, filename: a.filename, asDocument, ptt: a.ptt });
    clearAttach();
    inp.value = ''; inp.style.height = 'auto';
    return;
  }
  if (!text) return;
  socket.emit('send_message', { chatId: S.chatId, text, replyTo: S.replyTo?.id||null, mentions: S.mentions });
  inp.value = ''; inp.style.height = 'auto';
  clearTimeout(typingTimer);
  socket.emit('clear_typing', { chatId: S.chatId });
  cancelReply();
}

function checkMention(val) {
    const match = val.match(/@(\w*)$/);
    const box = document.getElementById('mention-box');
    if (match && S.isGroup) {
        const q = match[1].toLowerCase();
        const cands = S.participants.filter(p => {
            const name = (S.chats.find(c=>c.id===p.id)?.name || p.id).toLowerCase();
            return name.includes(q);
        }).slice(0, 10);
        if (cands.length) {
            box.innerHTML = cands.map(p => {
                const name = S.chats.find(c=>c.id===p.id)?.name || p.id.split('@')[0];
                return '<div class="mn-item" onclick="addMention(\''+p.id+'\',\''+esc(name)+'\')">'+esc(name)+'</div>';
            }).join('');
            box.style.display = 'flex';
            return;
        }
    }
    box.style.display = 'none';
}
function addMention(id, name) {
    const inp = document.getElementById('msg-input');
    inp.value = inp.value.replace(/@(\w*)$/, '@'+name+' ');
    S.mentions.push(id);
    document.getElementById('mention-box').style.display = 'none';
    inp.focus();
}

let typingTimer;
const msgInput = document.getElementById('msg-input');
msgInput.addEventListener('input', function() { this.style.height='auto'; this.style.height=Math.min(this.scrollHeight,120)+'px'; checkMention(this.value); });
msgInput.addEventListener('keydown', e => {
    if(e.key==='Enter'&&!e.shiftKey){ e.preventDefault(); send(); return; }
    clearTimeout(typingTimer);
    socket.emit('send_typing', { chatId: S.chatId });
    typingTimer = setTimeout(() => socket.emit('clear_typing', { chatId: S.chatId }), 2500);
});

// ── MEDIA LIGHTBOX ────────────────────────────────────────
function viewImg(id) {
  const m = S.msgs.find(x=>x.id===id); if(!m?.mediaUrl) return;
  const ov = document.createElement('div');
  ov.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.96);z-index:9999;display:flex;align-items:center;justify-content:center;cursor:zoom-out';
  ov.innerHTML = '<img src="'+m.mediaUrl+'" style="max-width:92vw;max-height:92vh;object-fit:contain;border-radius:4px">';
  ov.onclick = () => ov.remove();
  document.body.appendChild(ov);
}

// ── UI TOGGLES ────────────────────────────────────────────
const UI = {
  dark() { const t = document.documentElement.dataset.theme==='dark'?'':'dark'; document.documentElement.dataset.theme=t; localStorage.setItem('wa-theme',t); },
  search() { document.getElementById('sp').classList.toggle('open'); },
  filter() { document.getElementById('filter-bar').classList.toggle('show'); },
  stats() { const p=document.getElementById('stats-panel'); p.classList.toggle('open'); if(p.classList.contains('open')&&S.chatId) socket.emit('get_stats',{chatId:S.chatId}); },
  toggleGhost() { socket.emit('toggle_ghost'); toast('Switching stealth mode...'); },
  export() { if(!S.chatId){toast('Open a chat first');return;} const f=confirm('Export as CSV?\nCancel = plain text')?'csv':'txt'; socket.emit('export_chat',{chatId:S.chatId,format:f}); toast('Preparing...'); },
  showInfo() {
    const p = document.getElementById('ci-panel');
    p.classList.toggle('open');
    if(p.classList.contains('open') && S.chatId) {
      socket.emit('get_contact_info', { chatId: S.chatId });
    }
  },
  scheduler() {
    const s = document.getElementById('sch-who');
    s.innerHTML = S.chats.map(c => '<option value="'+c.id+'">'+esc(c.name)+'</option>').join('');
    if(S.chatId) s.value = S.chatId;
    // Trigger group check immediately in case current chat is pre-selected
    s.onchange = () => { UI.updateConditionalSender(); };
    document.getElementById('sch-modal').style.display = 'flex';
    document.getElementById('sch-cond').onchange = UI.updateConditionalSender;
    UI.updateConditionalSender();
    UI.schTab('new');
  },
  updateConditionalSender() {
    const condSelect = document.getElementById('sch-cond-who');
    const isChecked = document.getElementById('sch-cond').checked;
    const chatId = document.getElementById('sch-who').value;
    const chat = S.chats.find(c => c.id === chatId);

    if (isChecked) {
        condSelect.style.display = 'block';
        if (chat && chat.isGroup) {
            condSelect.innerHTML = '<option>Loading participants...</option>';
            socket.emit('get_group_participants', { chatId });
        } else {
            condSelect.innerHTML = '<option value="">(Not a group)</option>';
        }
    } else {
        condSelect.style.display = 'none';
    }
  },
  addTask() {
    const chatId = document.getElementById('sch-who').value;
    const text = document.getElementById('sch-msg').value;
    const timeVal = document.getElementById('sch-time').value;
    const time = new Date(timeVal).getTime();
    const type = document.getElementById('sch-cond').checked ? 'conditional' : 'simple';
    const condWhoEl = document.getElementById('sch-cond-who');
    const conditionalAuthorId = (type === 'conditional' && condWhoEl.value) ? condWhoEl.value : null;
    if(!chatId || !text || !timeVal || time < Date.now()) { toast('Invalid task details'); return; }
    socket.emit('add_schedule', { chatId, text, time, type, conditionalAuthorId });
    toast('Task scheduled ✓');
    UI.schTab('pending'); // Switch to pending view
    document.getElementById('sch-msg').value = '';
    document.getElementById('sch-time').value = '';
    document.getElementById('sch-cond').checked = false;
    UI.updateConditionalSender();
  },
  schTab(t) {
    S.schTab = t;
    document.querySelectorAll('.sch-tab').forEach(el => el.classList.remove('active'));
    document.querySelectorAll('.sch-tab')[t==='new'?0:t==='pending'?1:2].classList.add('active');
    document.getElementById('sch-v-new').style.display = t==='new' ? 'block' : 'none';
    document.getElementById('sch-list-cont').style.display = t==='new' ? 'none' : 'block';
    UI.renderSch();
  },
  renderSch() {
    if(S.schTab === 'new') return;
    const el = document.getElementById('sch-list-cont');
    const list = S.schTasks.filter(t => S.schTab==='pending' ? t.status==='pending' : t.status!=='pending');
    if(!list.length) { el.innerHTML = '<div style="text-align:center;color:var(--tx2);padding:20px">No '+S.schTab+' tasks</div>'; return; }
    
    el.innerHTML = list.sort((a,b)=>b.time-a.time).map(t => {
        const name = S.chats.find(c=>c.id===t.chatId)?.name || t.chatId;
        let typeStr = '📨 Simple';
        if (t.type === 'conditional') {
             const cName = t.conditionalAuthorId ? (S.chats.find(c=>c.id===t.conditionalAuthorId)?.name||'Target') : 'Anyone';
             typeStr = '⚠️ No-Reply ('+esc(cName)+')';
        }
        const dateStr = new Date(t.time).toLocaleString();
        
        let extra = '';
        if(S.schTab === 'pending') {
            // Countdown logic is handled in timer
            extra = '<div class="sch-prog-bg"><div class="sch-prog-bar" id="pb-'+t.id+'"></div></div>'
                  + '<div class="sch-cd"><span id="cd-'+t.id+'">Wait...</span><span>'+dateStr+'</span></div>';
        } else {
            const st = t.status==='sent' ? '<span style="color:var(--g)">✓ Sent</span>' 
                     : t.status==='skipped' ? '<span style="color:#ff9800">⊘ Skipped (Replied)</span>' 
                     : '<span style="color:#ef5350">✕ Failed</span>';
            extra = '<div class="sch-cd" style="font-size:12px;margin-top:4px">'+st+' · '+dateStr+'</div>';
        }

        return '<div class="sch-item '+(t.type==='conditional'?'cond':'')+'">'
          +'<div style="width:100%"><div style="font-weight:600;display:flex;justify-content:space-between">'+esc(name)+'<span style="font-weight:400;color:var(--tx2);font-size:11px">'+typeStr+'</span></div>'
          +'<div style="color:var(--tx2);font-style:italic;margin-top:2px;font-size:12.5px">'+esc(t.text)+'</div>'
          + extra
          +'</div>'
          +'<div class="sch-del" onclick="socket.emit(\'del_schedule\',\''+t.id+'\')" style="margin-left:8px">✖</div></div>';
    }).join('');
  },
  sync() { if(S.chatId){ toast('Syncing history in background...'); socket.emit('sync_chat',{chatId:S.chatId}); } },
  markRead() { if(S.chatId){socket.emit('mark_read',{chatId:S.chatId});toast('Marked as read');} },
  reportModal() {
    const s = document.getElementById('rp-sel');
    // Pre-select if saved
    const saved = localStorage.getItem('wa-report-group');
    
    s.innerHTML = S.chats.filter(c => c.isGroup).map(c => '<option value="'+c.id+'">'+esc(c.name)+'</option>').join('');
    if(saved) s.value = saved;
    if(!s.innerHTML) s.innerHTML = '<option>No groups found</option>';
    document.getElementById('report-modal').style.display = 'flex';
  },
  clickReport() {
    const saved = localStorage.getItem('wa-report-group');
    if (saved) UI.openReport(saved);
    else UI.reportModal();
  },
  openReport(id) {
    if (!id) id = document.getElementById('rp-sel').value;
    if (!id || !id.includes('@')) return;
    localStorage.setItem('wa-report-group', id);
    document.getElementById('report-modal').style.display = 'none';
    openChat(id, S.chats.find(c=>c.id===id)?.name, true);
    S.filterFn = m => /Fob\d+|AREA|SUPPORT|RELOCATION|INSTALLATION/i.test(m.body||'');
    const filtered = S.msgs.filter(S.filterFn);
    document.getElementById('fct').textContent = filtered.length + ' reports found';
    renderAll();
    toast('Report filter applied');
  },
  rename(id, current) {
    const name = prompt('Rename this contact/number ('+id+') to:', current);
    if (name !== null) {
        socket.emit('rename_contact', { id, name });
        if (S.chatId === id) { 
            document.getElementById('c-name').textContent = name;
            const ci = document.querySelector('.ci-t1'); if(ci) ci.childNodes[0].textContent = name + ' ';
        }
    }
  }
  ,block(id) {
      if(confirm('Block this contact?')) { socket.emit('block_contact', { chatId: id }); toast('Blocked'); }
  },
  deleteChat(id) {
      if(confirm('Delete this chat? Irreversible.')) socket.emit('delete_chat', { chatId: id });
  },
  toggleCallBlock(id, el) {
      socket.emit('toggle_call_block', { chatId: id, active: el.checked });
      toast(el.checked ? 'Calls will be auto-rejected' : 'Calls allowed');
  },
  togglePin(id, state) {
      socket.emit('pin_chat', { chatId: id, pin: !state });
      const c = S.chats.find(x => x.id === id);
      if(c) c.isPinned = !state;
      S.chats.sort((a,b) => (a.isPinned===b.isPinned) ? b.lastMsgTime-a.lastMsgTime : (a.isPinned?-1:1));
      renderChats();
  }
};
function ctx_sched() {
  if(!ctxMsg) return;
  UI.scheduler();
  document.getElementById('sch-who').value = ctxMsg.from;
  document.getElementById('sch-cond').checked = true;
  document.getElementById('sch-msg').focus();
}

function logout() { if(confirm('Disconnect and re-scan QR?\\n\\nThis is required to fetch FULL chat history again.')) socket.emit('logout_session'); }
function resync() {
  if(!confirm('Clear local cache + Supabase database, then resync EVERYTHING?\\n\\nThis wipes all cached messages, contacts, profile pics and the Supabase tables, then shows a NEW QR to scan. Your phone will re-push the full history.')) return;
  toast('Clearing database & resyncing...');
  socket.emit('clear_and_resync', {}, (ack) => {
    const ok = ack && ack.ok;
    toast(ok ? 'Cleared ✓ Scan the new QR to resync' : 'Resync failed: ' + ((ack&&ack.error)||'unknown'));
  });
}
socket.on('clear_and_resync_done', (r) => {
  if (r && r.ok) { toast('Cleared ✓ Scan the new QR'); S.chats = []; renderChats(); }
  else toast('Resync failed: ' + ((r&&r.error)||'unknown'));
});

// ── SHARE ACCESS QR ─────────────────────────────────────
async function showShareQR() {
  const m = document.getElementById('share-modal');
  m.style.display = 'flex';
  document.getElementById('share-qr').innerHTML = '<div class="loader"></div>';
  try {
    const r = await fetch('/api/share-qr').then(x => x.json());
    if (!r.ok) { document.getElementById('share-qr').innerHTML = '<div style="color:var(--tx2);font-size:13px;text-align:center">'+ (r.error||'Not configured') +'<br><br>Set PUBLIC_URL (e.g. https://cx.fob.net.ng) on the server.</div>'; return; }
    document.getElementById('share-host').textContent = new URL(r.url).host;
    document.getElementById('share-qr').innerHTML = '<img src="'+r.qr+'" style="width:240px;height:240px;border-radius:8px;background:#fff;padding:8px">';
    window.__shareUrl = r.url;
  } catch (e) {
    document.getElementById('share-qr').innerHTML = '<div style="color:var(--tx2);font-size:13px">Failed to load</div>';
  }
}
function copyShareLink() {
  if (window.__shareUrl) navigator.clipboard.writeText(window.__shareUrl).then(()=>toast('Link copied'));
}
function scrollBot(instant) {
  const el = document.getElementById('messages');
  if(instant) el.scrollTop = el.scrollHeight;
  else el.scrollTo({top:el.scrollHeight,behavior:'smooth'});
  S.atBottom=true; S.unreadNew=0;
  document.getElementById('ucb').className='';
  document.getElementById('s2b').className='';
}

function refreshCurrentPic() {
    if (!S.chatId) {
        toast("No chat open");
        return;
    }
    toast("Refreshing profile picture...");
    // Force server to re-fetch (bypassing its own cache checks)
    socket.emit('fetch_pic_force', { chatId: S.chatId });
}

// ── TOAST ─────────────────────────────────────────────────
function toast(msg, dur=2800) {
  const c = document.getElementById('tc');
  const t = document.createElement('div');
  t.className='toast'; t.textContent=msg; c.appendChild(t);
  setTimeout(()=>{t.style.opacity='0';t.style.transition='opacity .3s';setTimeout(()=>t.remove(),300);},dur);
}

// ── UTILS ─────────────────────────────────────────────────
function esc(s) { return s?String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'):''; }
function hlQ(txt,q) { if(!q) return txt; try{return txt.replace(new RegExp(esc(q),'gi'),m=>'<span class="sr-hl">'+m+'</span>');}catch{return txt;} }

function t12(ts) { 
  const t = typeof ts === 'object' ? (ts.low || 0) : Number(ts);
  if (!t) return '';
  return new Date(t < 9999999999 ? t*1000 : t).toLocaleTimeString('en-US',{hour:'2-digit',minute:'2-digit',hour12:true}); 
}
function tFmt(ts) {
  const t = typeof ts === 'object' ? (ts.low || 0) : Number(ts);
  if (!t) return '';
  const ms = t < 9999999999 ? t*1000 : t;
  const d=new Date(ms),now=new Date();
  if(d.toDateString()===now.toDateString()) return d.toLocaleTimeString('en-US',{hour:'2-digit',minute:'2-digit',hour12:true});
  const diff=Math.floor((now-d)/86400000);
  if(diff<7) return d.toLocaleDateString('en-US',{weekday:'short'});
  return d.toLocaleDateString('en-US',{month:'2-digit',day:'2-digit',year:'2-digit'});
}
function dStr(d) {
  if (!d || isNaN(d.getTime())) return 'Unknown Date';
  const now=new Date(),today=new Date(now.getFullYear(),now.getMonth(),now.getDate()),day=new Date(d.getFullYear(),d.getMonth(),d.getDate());
  const diff=Math.round((today-day)/86400000);
  if(diff===0) return 'Today';
  if(diff===1) return 'Yesterday';
  return d.toLocaleDateString('en-US',{month:'long',day:'numeric',year:'numeric'});
}
function dtStr(ts) {
  const t = typeof ts === 'object' ? (ts.low || 0) : Number(ts);
  if (!t) return '';
  const ms = t < 9999999999 ? t*1000 : t;
  return new Date(ms).toLocaleString('en-US',{month:'short',day:'numeric',year:'numeric',hour:'2-digit',minute:'2-digit',hour12:true});
}

// ── INIT ──────────────────────────────────────────────────
// ═══ GMAIL TOGGLE — standalone function (not inside UI object) ═══
// ═══════════════════════════════════════════════════════
// UNIFIED EMAIL ENGINE — Gmail + Microsoft Graph
// ═══════════════════════════════════════════════════════
let gmailMode       = false;
let gmailEmails     = [];
let currentOpenEmailFrom    = '';
let currentOpenEmailSubject = '';
let currentOpenEmailId      = '';
let currentOpenEmailStarred = false;
let gmailNextPageToken      = null;
let gmailCurrentLabel       = 'INBOX';
let gmailLoadingMore        = false;
let activeEmailAccount      = null; // { type:'gmail'|'graph', email }
let emailAccounts           = [];

const GM_LABELS = [
  { id:'INBOX',   icon:'M20 4H4c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V6c0-1.1-.9-2-2-2zm0 4l-8 5-8-5V6l8 5 8-5v2z', label:'Inbox' },
  { id:'STARRED', icon:'M12 17.27L18.18 21l-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.73L5.82 21z', label:'Starred' },
  { id:'SENT',    icon:'M2.01 21L23 12 2.01 3 2 10l15 2-15 2z', label:'Sent' },
  { id:'DRAFT',   icon:'M14 2H6c-1.1 0-2 .9-2 2v16c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2V8l-6-6zm2 16H8v-2h8v2zm0-4H8v-2h8v2zm-3-5V3.5L18.5 9H13z', label:'Drafts' },
  { id:'SPAM',    icon:'M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-2h2v2zm0-4h-2V7h2v6z', label:'Spam' },
  { id:'TRASH',   icon:'M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z', label:'Trash' },
];

function gmailAvatarColor(name) {
  const colors = ['#1a73e8','#ea4335','#34a853','#fbbc04','#9c27b0','#00acc1','#ff7043','#43a047'];
  let h = 0; for (let i=0; i<(name||'?').length; i++) h=(name.charCodeAt(i)+h*31)&0xffffffff;
  return colors[Math.abs(h) % colors.length];
}

function toggleGmailMode() {
  gmailMode = !gmailMode;
  const cl=document.getElementById('chats-list'), gs=document.getElementById('gmail-sidebar');
  const nc=document.getElementById('no-chat'), ac=document.getElementById('active-chat');
  const gm=document.getElementById('gmail-main'), gl=document.getElementById('gmail-login');
  const tabs=document.querySelector('.s-tabs'), av=document.getElementById('main-avatar');
  const ss=document.querySelector('.s-search'), hdr=document.querySelector('.s-hdr .hdr-btns');
  if (gmailMode) {
    if(cl) cl.style.display='none'; if(tabs) tabs.style.display='none';
    if(ss) ss.style.display='none'; if(hdr) hdr.style.display='none';
    if(nc) nc.style.display='none'; if(ac) ac.style.display='none';
    if(gs) gs.style.display='flex';
    if(av){av.style.background='#EA4335';av.textContent='G';av.title='Back to WhatsApp';}
    gmailRenderNav();
    // Load accounts then decide what to show
    socket.emit('email_op', { action: 'get_accounts' });
  } else {
    if(gs) gs.style.display='none'; if(gm) gm.style.display='none'; if(gl) gl.style.display='none';
    if(cl) cl.style.display='block'; if(tabs) tabs.style.display='flex';
    if(ss) ss.style.display='block'; if(hdr) hdr.style.display='flex';
    if(av){av.style.background='';av.textContent='M';av.title='Switch to Email';}
    if(S.chatId){if(ac)ac.style.display='flex';if(nc)nc.style.display='none';}
    else{if(nc)nc.style.display='flex';}
  }
}

function loadEmailWithAccount(acc) {
  activeEmailAccount = acc;
  const gl = document.getElementById('gmail-login'); if(gl) gl.style.display='none';
  const gm = document.getElementById('gmail-main'); if(gm) gm.style.display='flex';
  // Update Sidebar Header
  document.getElementById('gm-cur-email').textContent = acc.email;
  document.getElementById('gm-cur-av').textContent = acc.email[0].toUpperCase();
  document.getElementById('gm-cur-av').style.background = gmailAvatarColor(acc.email);
  document.getElementById('gm-account-menu').style.display = 'none'; // Close menu

  gmailEmails = []; gmailNextPageToken = null;
  document.getElementById('gm-email-list').innerHTML='';
  document.getElementById('gm-loading').style.display='block';
  gmailBackToList();
  // Update account switcher label
  const accEl = document.getElementById('gm-active-account');
  if(accEl) accEl.textContent = acc.email;
  // Load
  if (acc.type === 'graph') {
    socket.emit('email_op', { action:'graph_load', data:{ email:acc.email, folder:gmailCurrentLabel }});
  } else {
    socket.emit('email_op', { action:'gmail_load', data:{ label:gmailCurrentLabel, accountEmail:acc.email }});
  }
}

function showAccountsList() {
  document.getElementById('gm-accounts-section').style.display='block';
  document.getElementById('gm-add-work').style.display='none';
  document.getElementById('gm-add-gmail').style.display='none';
  document.getElementById('gmail-login').style.display='flex';
  document.getElementById('gmail-main').style.display='none';
  socket.emit('email_op', { action: 'get_accounts' });
}
function showAddWork()  { document.getElementById('gm-accounts-section').style.display='none'; document.getElementById('gm-add-work').style.display='block'; document.getElementById('gm-add-gmail').style.display='none'; }
function showAddGmail() { document.getElementById('gm-accounts-section').style.display='none'; document.getElementById('gm-add-work').style.display='none'; document.getElementById('gm-add-gmail').style.display='block'; }

function renderAccountsList(accounts) {
  emailAccounts = accounts;
  
  // Render Sidebar Dropdown
  const menu = document.getElementById('gm-account-menu');
  menu.innerHTML = accounts.map(a => {
      const icon = a.type==='graph' ? '<svg width="20" height="20" viewBox="0 0 24 24" style="fill:#0078d4"><path d="M21 18v1c0 1.1-.9 2-2 2H5c-1.11 0-2-.9-2-2V5c0-1.1.89-2 2-2h14c1.1 0 2 .9 2 2v1h-9c-1.11 0-2 .9-2 2v8c0 1.1.89 2 2 2h9zm-9-2h10V8H12v8zm4-2.5c-.83 0-1.5-.67-1.5-1.5s.67-1.5 1.5-1.5 1.5.67 1.5 1.5-.67 1.5-1.5 1.5z"/></svg>'
                   : '<svg width="20" height="20" viewBox="0 0 24 24" style="fill:#ea4335"><path d="M20 4H4c-1.1 0-1.99.9-1.99 2L2 18c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V6c0-1.1-.9-2-2-2zm0 4l-8 5-8-5V6l8 5 8-5v2z"/></svg>';
      return '<div class="gm-acc-item" onclick="loadEmailWithAccount({type:\''+a.type+'\',email:\''+a.email+'\'})">'
             + icon
             + '<div style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:13px;color:var(--tx)">'+a.email+'</div>'
             + '</div>';
  }).join('') + '<div class="gm-acc-item" onclick="showAccountsList();document.getElementById(\'gm-account-menu\').style.display=\'none\'" style="color:#1a73e8;font-weight:600">+ Add / Manage Accounts</div>';

  const list = document.getElementById('gm-accounts-list');
  if (!accounts.length) { list.innerHTML = '<div style="text-align:center;padding:16px;color:var(--tx2);font-size:13px">No accounts yet. Add one below.</div>'; return; }
  list.innerHTML = accounts.map(a => {
    const isActive = activeEmailAccount?.email === a.email;
    const icon = a.type==='graph'
      ? '<svg width="20" height="20" viewBox="0 0 24 24" style="fill:#0078d4;flex-shrink:0"><path d="M21 18v1c0 1.1-.9 2-2 2H5c-1.11 0-2-.9-2-2V5c0-1.1.89-2 2-2h14c1.1 0 2 .9 2 2v1h-9c-1.11 0-2 .9-2 2v8c0 1.1.89 2 2 2h9zm-9-2h10V8H12v8zm4-2.5c-.83 0-1.5-.67-1.5-1.5s.67-1.5 1.5-1.5 1.5.67 1.5 1.5-.67 1.5-1.5 1.5z"/></svg>'
      : '<svg width="20" height="20" viewBox="0 0 48 48" style="flex-shrink:0"><path fill="#4285F4" d="M45.12 24.5c0-1.56-.14-3.06-.4-4.5H24v8.51h11.84c-.51 2.75-2.06 5.08-4.39 6.64v5.52h7.11c4.16-3.83 6.56-9.47 6.56-16.17z"/><path fill="#34A853" d="M24 46c5.94 0 10.92-1.97 14.56-5.33l-7.11-5.52c-1.97 1.32-4.49 2.1-7.45 2.1-5.73 0-10.58-3.87-12.32-9.07H4.34v5.7C7.96 41.07 15.4 46 24 46z"/><path fill="#FBBC05" d="M11.68 28.18A13.9 13.9 0 0 1 10.96 24c0-1.45.25-2.86.72-4.18v-5.7H4.34A23.93 23.93 0 0 0 0 24c0 3.87.93 7.54 2.56 10.78l7.12-5.52z"/><path fill="#EA4335" d="M24 9.5c3.23 0 6.13 1.11 8.41 3.29l6.31-6.31C34.91 2.99 29.93.5 24 .5 15.4.5 7.96 5.93 4.34 13.12l7.34 5.7C13.42 13.37 18.27 9.5 24 9.5z"/></svg>';
    return '<div style="display:flex;align-items:center;gap:10px;padding:10px 12px;border-radius:10px;background:'+(isActive?'#e8f0fe':'var(--sbg)')+';margin-bottom:6px;cursor:pointer" onclick="loadEmailWithAccount('+JSON.stringify(a)+')">'
      + icon
      + '<div style="flex:1;min-width:0"><div style="font-size:13px;font-weight:500;color:var(--tx);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+esc(a.email)+'</div><div style="font-size:11px;color:#5f6368">'+(a.type==='graph'?'Microsoft 365 / Exchange':'Gmail')+'</div></div>'
      + (isActive ? '<span style="font-size:10px;background:#1a73e8;color:#fff;padding:2px 7px;border-radius:8px">Active</span>' : '')
      + '<button onclick="event.stopPropagation();removeEmailAccount(\''+a.type+'\',\''+a.email+'\')" style="background:none;border:none;cursor:pointer;color:#ea4335;font-size:14px;padding:4px 6px" title="Remove">✕</button>'
    + '</div>';
  }).join('');
}

function removeEmailAccount(type, email) {
  if (!confirm('Remove ' + email + '?')) return;
  socket.emit('email_op', { action:'remove_account', data:{ type, email }});
}

function toggleAccountMenu() {
    const m = document.getElementById('gm-account-menu');
    m.style.display = m.style.display === 'flex' ? 'none' : 'flex';
}

// ─── Microsoft Graph Auth ─────────────────────────────
function graphGetAuthUrl() {
  const clientId = document.getElementById('graph-client-id').value.trim();
  const clientSecret = document.getElementById('graph-client-secret').value.trim();
  if (!clientId || !clientSecret) { toast('Enter Client ID and Client Secret'); return; }
  socket.emit('email_op', { action:'graph_get_auth_url', data:{ clientId, clientSecret }});
  toast('Opening Microsoft login...');
}

socket.on('graph_auth_code', code => {
  toast('Authorization received, connecting...');
  socket.emit('email_op', { action:'graph_exchange_code', data:{ code }});
});

// ─── Gmail Auth ───────────────────────────────────────
function gmailGetAuthUrl() {
  const clientId = document.getElementById('gm-client-id').value.trim();
  const clientSecret = document.getElementById('gm-client-secret').value.trim();
  if (!clientId || !clientSecret) { toast('Enter Client ID and Client Secret'); return; }
  socket.emit('email_op', { action:'gmail_auth_url', data:{ clientId, clientSecret }});
}

function gmailExchangeCode() {
  const clientId = document.getElementById('gm-client-id').value.trim();
  const clientSecret = document.getElementById('gm-client-secret').value.trim();
  const code = document.getElementById('gm-auth-code').value.trim();
  if (!code) { toast('Paste the code first'); return; }
  socket.emit('email_op', { action:'gmail_exchange_code', data:{ clientId, clientSecret, code }});
}

// ─── Nav / Search / Load ──────────────────────────────
function gmailRenderNav() {
  const el = document.getElementById('gm-nav-list'); if (!el) return;
  el.innerHTML = GM_LABELS.map(l => {
    const isActive = l.id === gmailCurrentLabel;
    return '<div onclick="gmailNav(\''+l.id+'\',this)" style="display:flex;align-items:center;gap:14px;padding:4px 16px 4px 24px;border-radius:0 24px 24px 0;cursor:pointer;font-size:14px;height:36px;margin-right:16px;transition:background .1s;'+(isActive?'background:#d3e3fd;font-weight:600;color:#001d35':'color:var(--tx)')+'" onmouseover="if(this.style.background!==\'rgb(211,227,253)\')this.style.background=\'rgba(0,0,0,.04)\'" onmouseout="if(this.style.background!==\'rgb(211,227,253)\')this.style.background=\'\'">'
      +'<svg width="20" height="20" viewBox="0 0 24 24" style="fill:'+(isActive?'#001d35':'#444746')+';flex-shrink:0"><path d="'+l.icon+'"/></svg>'
      +l.label
      +(l.id==='INBOX'?'<span id="gm-unread-count" style="display:none;background:#1a73e8;color:#fff;font-size:11px;font-weight:700;padding:1px 7px;border-radius:10px;margin-left:auto"></span>':'')
      +'</div>';
  }).join('');
}

function gmailNav(label) {
  gmailCurrentLabel = label; gmailNextPageToken = null; gmailEmails = [];
  const si = document.getElementById('gm-search-input'); if(si) si.value='';
  gmailRenderNav();
  document.getElementById('gm-email-list').innerHTML='';
  document.getElementById('gm-loading').style.display='block';
  gmailBackToList();
  if (!activeEmailAccount) return;
  if (activeEmailAccount.type==='graph') socket.emit('email_op',{action:'graph_load',data:{email:activeEmailAccount.email,folder:label}});
  else socket.emit('email_op',{action:'gmail_load',data:{label, accountEmail:activeEmailAccount.email}});
}

function gmailSearch(q) {
  if (!q.trim()) { gmailNav(gmailCurrentLabel); return; }
  gmailEmails=[]; document.getElementById('gm-email-list').innerHTML=''; document.getElementById('gm-loading').style.display='block';
  gmailBackToList(); toast('Searching...');
  if (!activeEmailAccount) return;
  if (activeEmailAccount.type==='graph') socket.emit('email_op',{action:'graph_load',data:{email:activeEmailAccount.email,folder:'INBOX',search:q}});
  else socket.emit('email_op',{action:'gmail_search',data:{query:q, accountEmail:activeEmailAccount.email}});
}

function gmailLoadMore() {
  if (!gmailNextPageToken || gmailLoadingMore) return;
  gmailLoadingMore=true;
  document.getElementById('gm-load-more').style.display='none';
  document.getElementById('gm-loading').style.display='block';
  if (!activeEmailAccount) return;
  if (activeEmailAccount.type==='graph') socket.emit('email_op',{action:'graph_load',data:{email:activeEmailAccount.email,folder:gmailCurrentLabel,nextLink:gmailNextPageToken}});
  else socket.emit('email_op',{action:'gmail_load',data:{label:gmailCurrentLabel,pageToken:gmailNextPageToken, accountEmail:activeEmailAccount.email}});
}

function gmailRefresh() {
  gmailEmails=[]; gmailNextPageToken=null;
  document.getElementById('gm-email-list').innerHTML='';
  document.getElementById('gm-loading').style.display='block';
  gmailNav(gmailCurrentLabel); toast('Refreshing...');
}

function gmailBackToList() {
  const ph=document.getElementById('gm-reader-placeholder'), gr=document.getElementById('gmail-reader'), gm=document.getElementById('gmail-main');
  if(ph) ph.style.display='flex'; if(gr) gr.style.display='none'; if(gm) gm.style.display='flex';
}

// ─── Render email list ────────────────────────────────
function renderGmailList(emails, append) {
  const el = document.getElementById('gm-email-list'); if (!el) return;
  document.getElementById('gm-loading').style.display='none';
  if (!append) el.innerHTML='';
  if (!emails.length && !append) { el.innerHTML='<div style="padding:40px;text-align:center;color:#5f6368;font-size:14px">No emails found</div>'; return; }
  const frag = document.createDocumentFragment();
  emails.forEach(m => {
    const name=(m.from||'').replace(/<.*>/,'').replace(/"/g,'').trim()||'?';
    const init=(name[0]||'?').toUpperCase(), color=gmailAvatarColor(name);
    const time=(() => { try { const d=new Date(m.date),now=new Date(); return d.toDateString()===now.toDateString()?d.toLocaleTimeString('en-US',{hour:'2-digit',minute:'2-digit',hour12:true}):d.toLocaleDateString('en-US',{month:'short',day:'numeric'}); } catch{return '';} })();
    const isSelected = m.id===currentOpenEmailId;
    const row = document.createElement('div');
    row.dataset.id = m.id;
    row.style.cssText='display:flex;align-items:center;padding:0 16px;height:52px;cursor:pointer;border-bottom:1px solid rgba(0,0,0,.06);gap:10px;transition:background .1s;'+(isSelected?'background:#e8f0fe;':(m.unread?'background:#fff':'background:#f6f8fc'));
    row.onmouseover=function(){if(this.dataset.id!==currentOpenEmailId)this.style.background='#f2f6fc';};
    row.onmouseout=function(){if(this.dataset.id!==currentOpenEmailId)this.style.background=(m.unread?'#fff':'#f6f8fc');};
    row.onclick=function(){gmailOpenEmail(m.id);};
    row.innerHTML='<svg width="18" height="18" viewBox="0 0 24 24" style="fill:'+(m.starred?'#f4b400':'#ccc')+';flex-shrink:0;cursor:pointer" onclick="event.stopPropagation();gmailToggleStarInList(\''+m.id+'\','+(!m.starred)+',this)"><path d="M12 17.27L18.18 21l-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.73L5.82 21z"/></svg>'
      +'<div style="width:32px;height:32px;border-radius:50%;background:'+color+';display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:600;color:#fff;flex-shrink:0">'+init+'</div>'
      +'<div style="flex:1;min-width:0;display:flex;align-items:center;gap:8px">'
        +'<div style="font-size:14px;color:#202124;width:130px;flex-shrink:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;'+(m.unread?'font-weight:700':'')+'">'+esc(name)+'</div>'
        +'<div style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:14px">'
          +'<span style="color:#202124;'+(m.unread?'font-weight:700':'')+'">'+esc(m.subject||'(no subject)')+'</span>'
          +(m.snippet?'<span style="color:#5f6368"> — '+esc(m.snippet.substring(0,60))+'</span>':'')
        +'</div>'
      +'</div>'
      +'<div style="font-size:12px;color:#5f6368;white-space:nowrap;flex-shrink:0">'+time+'</div>';
    frag.appendChild(row);
  });
  el.appendChild(frag);
}

// ─── Open email ───────────────────────────────────────
function gmailOpenEmail(id) {
  currentOpenEmailId=id;
  const email=gmailEmails.find(m=>m.id===id);
  document.querySelectorAll('#gm-email-list > div').forEach(r => { const isMe=r.dataset.id===id; r.style.background=isMe?'#e8f0fe':(r.dataset.unread==='1'?'#fff':'#f6f8fc'); });
  const b=document.getElementById('gm-reader-body'), ph=document.getElementById('gm-reader-placeholder'), gr=document.getElementById('gmail-reader'), gm=document.getElementById('gmail-main');
  if(b) b.innerHTML='<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--tx2)">Loading message...</div>';
  if(ph) ph.style.display='none'; if(gr) gr.style.display='flex'; if(gm) gm.style.display='flex';
  if (email) {
    const senderName=email.from.replace(/<.*>/,'').replace(/"/g,'').trim()||email.from;
    const av=document.getElementById('gm-reader-av'); if(av){av.textContent=senderName[0].toUpperCase();av.style.background=gmailAvatarColor(senderName);}
    const sEl=document.getElementById('gm-reader-subject');if(sEl)sEl.textContent=email.subject;
    const nEl=document.getElementById('gm-reader-name');if(nEl)nEl.textContent=senderName;
    const aEl=document.getElementById('gm-reader-addr');if(aEl)aEl.textContent=email.from;
    const dEl=document.getElementById('gm-reader-date');if(dEl)dEl.textContent=email.date?new Date(email.date).toLocaleString():'';
    const rEl=document.getElementById('gm-reply-to-name');if(rEl)rEl.textContent=senderName;
    currentOpenEmailFrom=email.from; currentOpenEmailSubject=email.subject; currentOpenEmailStarred=email.starred;
    const starBtn=document.getElementById('gm-star-btn'); if(starBtn)starBtn.querySelector('svg').style.fill=email.starred?'#f4b400':'#ccc';
    email.unread=false;
  }
  if (!activeEmailAccount) return;
  if (activeEmailAccount.type==='graph') socket.emit('email_op',{action:'graph_open',data:{id,accountEmail:activeEmailAccount.email}});
  else socket.emit('email_op',{action:'gmail_open',data:{id,accountEmail:activeEmailAccount.email}});
}

function gmailToggleStar() {
  if (!currentOpenEmailId||!activeEmailAccount) return;
  currentOpenEmailStarred=!currentOpenEmailStarred;
  const starBtn=document.getElementById('gm-star-btn'); if(starBtn)starBtn.querySelector('svg').style.fill=currentOpenEmailStarred?'#f4b400':'#ccc';
  if (activeEmailAccount.type==='graph') socket.emit('email_op',{action:'graph_star',data:{id:currentOpenEmailId,accountEmail:activeEmailAccount.email,starred:currentOpenEmailStarred}});
  else socket.emit('email_op',{action:'gmail_toggle_star',data:{id:currentOpenEmailId,starred:currentOpenEmailStarred,accountEmail:activeEmailAccount.email}});
}

function gmailToggleStarInList(id, starred, svgEl) {
  svgEl.style.fill=starred?'#f4b400':'#ccc';
  const email=gmailEmails.find(m=>m.id===id); if(email)email.starred=starred;
  if (!activeEmailAccount) return;
  if (activeEmailAccount.type==='graph') socket.emit('email_op',{action:'graph_star',data:{id,accountEmail:activeEmailAccount.email,starred}});
  else socket.emit('email_op',{action:'gmail_toggle_star',data:{id,starred,accountEmail:activeEmailAccount.email}});
}

function gmailTrash() {
  if (!currentOpenEmailId||!activeEmailAccount) return;
  if (!confirm('Move to Trash?')) return;
  if (activeEmailAccount.type==='graph') socket.emit('email_op',{action:'graph_trash',data:{id:currentOpenEmailId,accountEmail:activeEmailAccount.email}});
  else socket.emit('email_op',{action:'gmail_trash',data:{id:currentOpenEmailId,accountEmail:activeEmailAccount.email}});
}

function gmailReply() {
  const body=document.getElementById('gm-reply-input').value.trim();
  if (!body||!currentOpenEmailFrom) { toast('Nothing to reply to'); return; }
  if (!activeEmailAccount) return;
  if (activeEmailAccount.type==='graph') socket.emit('email_op',{action:'graph_send',data:{accountEmail:activeEmailAccount.email,to:currentOpenEmailFrom,subject:'Re: '+(currentOpenEmailSubject||''),body}});
  else socket.emit('email_op',{action:'gmail_send',data:{to:currentOpenEmailFrom,subject:'Re: '+(currentOpenEmailSubject||''),body,accountEmail:activeEmailAccount.email}});
  document.getElementById('gm-reply-input').value=''; toast('Reply sent ✓');
}

function gmailCompose() { document.getElementById('gm-compose-modal').style.display='flex'; document.getElementById('gm-to').focus(); }

function gmailSendCompose() {
  const to=document.getElementById('gm-to').value.trim(), subject=document.getElementById('gm-subject').value.trim(), body=document.getElementById('gm-body').value.trim();
  if (!to||!body) { toast('Fill To and message'); return; }
  if (!activeEmailAccount) return;
  if (activeEmailAccount.type==='graph') socket.emit('email_op',{action:'graph_send',data:{accountEmail:activeEmailAccount.email,to,subject,body}});
  else socket.emit('email_op',{action:'gmail_send',data:{to,subject,body,accountEmail:activeEmailAccount.email}});
  document.getElementById('gm-compose-modal').style.display='none';
  ['gm-to','gm-subject','gm-body'].forEach(id=>document.getElementById(id).value='');
}

// ─── Socket responses ─────────────────────────────────
socket.on('email_res', res => {
  if (res.type === 'accounts_list') {
    emailAccounts = res.accounts;
    renderAccountsList(res.accounts);
    const gl = document.getElementById('gmail-login');
    const isLoginVisible = gl && gl.style.display !== 'none';
    if (res.accounts.length > 0 && gmailMode && !isLoginVisible) {
      // Auto-load last active or first account
      const acc = activeEmailAccount && res.accounts.find(a=>a.email===activeEmailAccount.email) ? activeEmailAccount : res.accounts[0];
      loadEmailWithAccount(acc);
    } else if (gmailMode) {
      if(gl)gl.style.display='flex';
      const gm=document.getElementById('gmail-main'); if(gm)gm.style.display='none';
    }
  }
  if (res.type === 'account_removed') {
    toast('Account removed'); socket.emit('email_op',{action:'get_accounts'});
  }
  if (res.type === 'graph_auth_url') {
    window.open(res.url, '_blank');
    document.getElementById('graph-step2').style.display='block';
    toast('Sign in with your Microsoft account in the new tab');
  }
  if (res.type === 'graph_ready') {
    toast('✓ Connected: ' + res.email);
    socket.emit('email_op',{action:'get_accounts'});
  }
  if (res.type === 'no_config' && gmailMode) {
    const gm=document.getElementById('gmail-main'); if(gm)gm.style.display='none';
    const gl=document.getElementById('gmail-login'); if(gl)gl.style.display='flex';
    showAccountsList();
  }
  if (res.type === 'auth_url') {
    document.getElementById('gm-auth-link').href=res.url;
    document.getElementById('gm-auth-link').textContent='Click here to authorize Gmail access →';
    document.getElementById('gm-step1').style.display='none';
    document.getElementById('gm-step2').style.display='flex';
  }
  if (res.type === 'gmail_ready') {
    toast('Gmail connected ✓');
    socket.emit('email_op',{action:'get_accounts'});
  }
  if (res.type === 'inbox' && gmailMode) {
    const newEmails=res.msgs||[], isAppend=gmailLoadingMore;
    gmailLoadingMore=false;
    if (isAppend) gmailEmails=gmailEmails.concat(newEmails); else gmailEmails=newEmails;
    gmailNextPageToken=res.nextPageToken||null;
    renderGmailList(newEmails, isAppend);
    const lm=document.getElementById('gm-load-more'); if(lm)lm.style.display=gmailNextPageToken?'block':'none';
    const unread=gmailEmails.filter(m=>m.unread).length;
    const badge=document.getElementById('gm-unread-count'); if(badge){badge.textContent=unread>99?'99+':unread;badge.style.display=unread?'inline-block':'none';}
    if (!res.fromCache) toast(newEmails.length+' emails loaded'+(gmailNextPageToken?' · more available':''));
  }
  if (res.type === 'email_body') {
    const senderName=(res.from||'').replace(/<.*>/,'').replace(/"/g,'').trim()||res.from||'?';
    const av=document.getElementById('gm-reader-av'); if(av){av.textContent=(senderName[0]||'?').toUpperCase();av.style.background=gmailAvatarColor(senderName);}
    const sEl=document.getElementById('gm-reader-subject');if(sEl)sEl.textContent=res.subject||'(no subject)';
    const nEl=document.getElementById('gm-reader-name');if(nEl)nEl.textContent=senderName;
    const aEl=document.getElementById('gm-reader-addr');if(aEl)aEl.textContent=res.from;
    const dEl=document.getElementById('gm-reader-date');if(dEl)dEl.textContent=res.date?new Date(res.date).toLocaleString():'';
    const rEl=document.getElementById('gm-reply-to-name');if(rEl)rEl.textContent=senderName;
    currentOpenEmailFrom=res.from; currentOpenEmailSubject=res.subject;
    const b=document.getElementById('gm-reader-body');
    if (b) {
      if (res.isHtml) {
        const iframe=document.createElement('iframe'); iframe.style.cssText='width:100%;border:none;min-height:400px;flex:1'; iframe.sandbox='allow-same-origin';
        b.innerHTML=''; b.style.padding='0'; b.style.display='flex'; b.style.flexDirection='column'; b.appendChild(iframe);
        iframe.onload=()=>{try{iframe.contentDocument.open();iframe.contentDocument.write('<base target="_blank">'+res.body);iframe.contentDocument.close();iframe.style.height=iframe.contentDocument.body.scrollHeight+'px';}catch(e){}};
        iframe.src='about:blank';
      } else {
        b.style.cssText='padding:20px 24px;font-size:14px;color:#202124;line-height:1.7;white-space:pre-wrap;word-break:break-word;overflow-y:auto;flex:1';
        b.textContent=res.body||'(No content)';
      }
    }
  }
  if (res.type==='trashed') { toast('Moved to Trash ✓'); gmailEmails=gmailEmails.filter(m=>m.id!==res.id); renderGmailList(gmailEmails,false); gmailBackToList(); currentOpenEmailId=''; }
  if (res.type==='sent') toast('Email sent ✓');
  if (res.type==='error') toast('Error: '+res.error);
});
const savedTheme = localStorage.getItem('wa-theme');
if (savedTheme) document.documentElement.dataset.theme = savedTheme;

// Scheduler Countdown Timer
setInterval(() => {
    if(document.getElementById('sch-modal').style.display === 'none' || S.schTab !== 'pending') return;
    const now = Date.now();
    S.schTasks.filter(t => t.status==='pending').forEach(t => {
        const bar = document.getElementById('pb-'+t.id);
        const txt = document.getElementById('cd-'+t.id);
        if(bar && txt) {
            const total = t.time - t.createdAt;
            const elapsed = now - t.createdAt;
            const pct = Math.min(100, Math.max(0, (elapsed / total) * 100));
            bar.style.width = pct + '%';
            const rem = Math.max(0, Math.ceil((t.time - now)/1000));
            
            let remStr = '';
            if(rem > 86400) remStr = Math.floor(rem/86400)+'d '+Math.floor((rem%86400)/3600)+'h';
            else if(rem > 3600) remStr = Math.floor(rem/3600)+'h '+Math.floor((rem%3600)/60)+'m';
            else if(rem > 60) remStr = Math.floor(rem/60)+'m '+ (rem%60)+'s';
            else remStr = rem + 's';
            
            txt.textContent = 'Sending in ' + remStr;
        }
    });
}, 1000);
</script>
</body>
</html>`;