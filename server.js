import express from 'express';
import multer from 'multer';
import pino from 'pino';
import { MongoClient, GridFSBucket } from 'mongodb';
import makeWASocket, {
  Browsers, DisconnectReason, initAuthCreds, BufferJSON, proto,
  fetchLatestBaileysVersion
} from '@whiskeysockets/baileys';

const { MONGO_URI, API_KEY, PORT = 3000 } = process.env;
const mongo = new MongoClient(MONGO_URI);
await mongo.connect();
const db = mongo.db('wa');
const authCol = db.collection('auth');
const msgs = db.collection('messages');
const bucket = new GridFSBucket(db, { bucketName: 'files' });

// WhatsApp session saved in MongoDB, so restarts never unlink you
async function useMongoAuthState(col) {
  const write = (data, id) => col.replaceOne({ _id: id },
    JSON.parse(JSON.stringify(data, BufferJSON.replacer)), { upsert: true });
  const read = async id => {
    const d = await col.findOne({ _id: id });
    return d ? JSON.parse(JSON.stringify(d), BufferJSON.reviver) : null;
  };
  const creds = (await read('creds')) || initAuthCreds();
  return {
    state: {
      creds,
      keys: {
        get: async (type, ids) => {
          const out = {};
          await Promise.all(ids.map(async id => {
            let v = await read(`${type}-${id}`);
            if (type === 'app-state-sync-key' && v)
              v = proto.Message.AppStateSyncKeyData.fromObject(v);
            out[id] = v;
          }));
          return out;
        },
        set: async data => {
          const tasks = [];
          for (const cat in data) for (const id in data[cat]) {
            const v = data[cat][id], key = `${cat}-${id}`;
            tasks.push(v ? write(v, key) : col.deleteOne({ _id: key }));
          }
          await Promise.all(tasks);
        }
      }
    },
    saveCreds: () => write(creds, 'creds')
  };
}

let sock = null, linked = false;

async function startSock() {
  const { state, saveCreds } = await useMongoAuthState(authCol);
  const { version } = await fetchLatestBaileysVersion();
  sock = makeWASocket({
    version, auth: state, logger: pino({ level: 'silent' }),
    browser: Browsers.ubuntu('Chrome'), printQRInTerminal: false
  });
  sock.ev.on('creds.update', saveCreds);
  sock.ev.on('connection.update', ({ connection, lastDisconnect }) => {
    if (connection === 'open') linked = true;
    if (connection === 'close') {
      linked = false;
      const code = lastDisconnect?.error?.output?.statusCode;
      if (code === DisconnectReason.loggedOut)
        authCol.deleteMany({}).then(startSock);   // unlinked from phone: reset
      else setTimeout(startSock, 3000);
    }
  });
}
await startSock();

async function sendOne(m) {
  const jid = m.phone + '@s.whatsapp.net';
  if (!m.fileId) return sock.sendMessage(jid, { text: m.text });
  const chunks = [];
  for await (const c of bucket.openDownloadStream(m.fileId)) chunks.push(c);
  const buf = Buffer.concat(chunks);
  let content;
  if (m.mime?.startsWith('image/')) content = { image: buf, caption: m.text };
  else if (m.mime?.startsWith('video/')) content = { video: buf, caption: m.text };
  else content = { document: buf, fileName: m.fileName,
    mimetype: m.mime || 'application/octet-stream', caption: m.text };
  return sock.sendMessage(jid, content);
}

// Scheduler: every 5s sends anything due (also catches up after a restart)
let busy = false;
setInterval(async () => {
  if (busy || !linked) return;
  busy = true;
  try {
    for (;;) {
      const m = await msgs.findOneAndUpdate(
        { status: 'pending', sendAt: { $lte: new Date() } },
        { $set: { status: 'sending' } });
      if (!m) break;
      try {
        await sendOne(m);
        await msgs.updateOne({ _id: m._id }, { $set: { status: 'sent', sentAt: new Date() } });
      } catch (e) {
        await msgs.updateOne({ _id: m._id }, { $set: { status: 'failed', error: String(e.message || e) } });
      }
      await new Promise(r => setTimeout(r, 2000)); // gap between sends lowers ban risk
    }
  } finally { busy = false; }
}, 5000);

const app = express();
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 60 * 1024 * 1024 } });
app.use(express.json());
app.get('/health', (_, res) => res.send('ok'));
app.use((req, res, next) =>
  req.get('x-api-key') === API_KEY ? next() : res.status(401).json({ error: 'unauthorized' }));

app.get('/status', (_, res) => res.json({ linked }));

app.post('/link', async (req, res) => {
  try {
    if (linked) return res.json({ linked: true });
    const phone = String(req.body.phone || '').replace(/\D/g, '');
    res.json({ code: await sock.requestPairingCode(phone) });
  } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
});

app.post('/schedule', upload.single('file'), async (req, res) => {
  try {
    const { phone, text = '', sendAt } = req.body;
    const when = new Date(sendAt);
    if (!phone || isNaN(when)) return res.status(400).json({ error: 'bad input' });
    let fileId = null, fileName = null, mime = null;
    if (req.file) {
      fileName = req.file.originalname; mime = req.file.mimetype;
      fileId = await new Promise((ok, fail) => {
        const up = bucket.openUploadStream(fileName);
        up.on('finish', () => ok(up.id)); up.on('error', fail);
        up.end(req.file.buffer);
      });
    }
    const r = await msgs.insertOne({ phone: phone.replace(/\D/g, ''), text, sendAt: when,
      status: 'pending', fileId, fileName, mime, createdAt: new Date() });
    res.json({ id: r.insertedId });
  } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
});

app.get('/messages', async (_, res) =>
  res.json(await msgs.find({}, { projection: { fileId: 0 } })
    .sort({ createdAt: -1 }).limit(50).toArray()));

app.listen(PORT, '0.0.0.0');
