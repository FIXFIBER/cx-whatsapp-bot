// ── CX ACCESS LAYER ───────────────────────────────────────
// Head/gateway model:
//   • The SERVER (deployed on Render) is the GATEWAY / HEAD.
//   • The main WhatsApp phone pairs to the gateway via the native Baileys QR.
//   • The gateway has an ADMIN (head) key. The person who holds it is "the head".
//     - If ADMIN_KEY env is set, only that key can become/act as head.
//     - If ADMIN_KEY is NOT set AND no devices exist yet, the first client to
//       call `auth` becomes the head (bootstrap mode) so you can claim it on
//       first deploy without a pre-shared secret.
//   • Other devices join ONLY by scanning an INVITE QR the head generates.
//     Each invite code is single-use and expires (15 min).
//   • Device registry persists on the gateway's disk (DATA_DIR) so it survives
//     restarts — no dependency on the operator's local laptop after deploy.

const fs = require('fs');
const crypto = require('crypto');
const DATA_DIR = process.env.DATA_DIR || '.';
const REG_FILE = DATA_DIR + '/devices.json';
const ADMIN_KEY = process.env.ADMIN_KEY || '';   // set on Render; empty = bootstrap first device

function load() {
  try { return JSON.parse(fs.readFileSync(REG_FILE, 'utf8')); }
  catch { return { devices: {}, invites: {}, adminKey: '' }; }
}
function save(db) {
  try { fs.mkdirSync(DATA_DIR, { recursive: true }); fs.writeFileSync(REG_FILE, JSON.stringify(db, null, 2)); } catch {}
}
let db = load();

// Ensure a stable admin key (head identity persists across restarts).
if (!db.adminKey) {
  db.adminKey = ADMIN_KEY || crypto.randomBytes(24).toString('hex');
  save(db);
}
const HEAD_KEY = db.adminKey;

function isHead(key) { return !!key && key === HEAD_KEY; }

// Bootstrap: no ADMIN_KEY set AND zero registered devices → first auth claims head.
function isBootstrap() { return !ADMIN_KEY && Object.keys(db.devices).length === 0; }

// Create the head device (used during bootstrap).
function makeHead(name) {
  const id = 'head_' + crypto.randomBytes(8).toString('hex');
  db.devices[id] = { name: name || 'Head', role: 'head', approved: true, addedAt: Date.now(), isHead: true };
  save(db);
  return id;
}

function listDevices() {
  return Object.entries(db.devices).map(([id, d]) => ({
    id, name: d.name, role: d.role, approved: d.approved, addedAt: d.addedAt
  }));
}

// Create a single-use invite code (valid 15 min). Head or any approved device may mint one.
function createInvite(createdBy) {
  const code = crypto.randomBytes(16).toString('hex');
  db.invites[code] = { createdBy: createdBy || 'unknown', expires: Date.now() + 15 * 60 * 1000 };
  save(db);
  return code;
}

// Redeem an invite code → register a new device (pending approval by head).
function redeemInvite(code, name) {
  const inv = db.invites[code];
  if (!inv) return { ok: false, reason: 'invalid' };
  if (inv.expires < Date.now()) { delete db.invites[code]; save(db); return { ok: false, reason: 'expired' }; }
  delete db.invites[code];
  const id = 'dev_' + crypto.randomBytes(8).toString('hex');
  db.devices[id] = { name: name || 'Device', role: 'member', approved: false, addedAt: Date.now() };
  save(db);
  return { ok: true, id, role: 'member', approved: false };
}

// Head approves a pending device.
function approveDevice(id) {
  if (!db.devices[id]) return false;
  db.devices[id].approved = true;
  save(db);
  return true;
}
function removeDevice(id) {
  if (!db.devices[id]) return false;
  delete db.devices[id];
  save(db);
  return true;
}

// Is a device allowed to use the gateway (chat/read/send)?
function canUse(id) {
  const d = db.devices[id];
  return !!(d && d.approved);
}

module.exports = {
  HEAD_KEY, isHead, isBootstrap, makeHead, listDevices, createInvite, redeemInvite,
  approveDevice, removeDevice, canUse, REG_FILE
};
