console.log(">>> SERVER FILE VERSION: PATCHED-FULL-8 <<<");

const express = require("express");
const archiver = require("archiver");
const http = require("http");
const https = require("https");
const { URL } = require("url");
const fs = require("fs");
const path = require("path");
const app = express();

// =========================================================
// UNIVERSAL CORS
// =========================================================
function allowCors(res, req) {
  // If the browser sends credentials (cookies), we cannot use "*" for ACAO.
  // We keep a small allowlist for known frontends, and fall back to "*" otherwise.
  const origin = req && req.headers ? req.headers.origin : "";
  const allowList = String(process.env.CORS_ALLOW_ORIGINS || "https://vmpix.onrender.com,https://vmpix.smugmug.com")
    .split(/[,;]+/)
    .map(s => s.trim())
    .filter(Boolean);

  const isAllowed = origin && allowList.includes(origin);

  if (isAllowed) {
    res.set("Access-Control-Allow-Origin", origin);
    res.set("Vary", "Origin");
    res.set("Access-Control-Allow-Credentials", "true");
  } else {
    // Public, non-credentialed requests
    res.set("Access-Control-Allow-Origin", "*");
  }

  res.set("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  // Include common headers used by fetch() and browsers
  res.set("Access-Control-Allow-Headers", "Content-Type, Accept, Authorization, X-Requested-With, X-Analytics-Key");
  // Optional: make it easier to debug in DevTools
  res.set("Access-Control-Expose-Headers", "Content-Type, Content-Length");
}

// Always attach CORS headers (including for static files) and handle OPTIONS fast
app.use((req, res, next) => {
  allowCors(res, req);
  if (req.method === "OPTIONS") return res.status(204).send("");
  next();
});


app.use(express.static(__dirname));
app.use(express.json({ limit: "2mb" }));


// SmugMug API Key
const SMUG_API_KEY = "SQLhhqgXZJd7MzqgVX563bkbjdCfXt9T";

// SmugMug user nickname (used for folder traversal)
const SMUG_NICKNAME = String(process.env.SMUG_NICKNAME || "vmpix").trim();

// Root folder for People Index scanning (albums will be discovered under this folder recursively)
// Example: Music/Archives/Bands
const PEOPLE_INDEX_BANDS_ROOT = String(process.env.PEOPLE_INDEX_BANDS_ROOT || "Music/Archives/Bands").trim();
// Optional safety cap (0 = no cap)
const PEOPLE_INDEX_MAX_ALBUMS = Math.max(0, Number(process.env.PEOPLE_INDEX_MAX_ALBUMS || "0"));

// Google Sheets (your existing CSV sources)
const BANDS_SHEET_URL =
  "https://docs.google.com/spreadsheets/d/e/2PACX-1vTdi19qTDyPeBGzq0PpkdlDS_bNg34XpdRiXy8aBa-Jlu-jg2Wzkj1SnLXtRVFU4TGOh5KHJPK8Lwhc/pub?gid=0&single=true&output=csv";

const SHOWS_SHEET_URL =
  "https://docs.google.com/spreadsheets/d/e/2PACX-1vTdi19qTDyPeBGzq0PpkdlDS_bNg34XpdRiXy8aBa-Jlu-jg2Wzkj1SnLXtRVFU4TGOh5KHJPK8Lwhc/pub?gid=1306635885&single=true&output=csv";

// Stats tab (Fix / Metadata) – gid provided by Chris
// NOTE: Uses the Google Sheet "export?format=csv" URL style.
// This will work as long as the sheet (or at least this tab) is readable without auth.
const STATS_SHEET_URL =
  "https://docs.google.com/spreadsheets/d/12P8b85K24dcyy9jubil_h4DN6xXr3wYFsILz-5LkGkk/export?format=csv&gid=1973247444";


// =========================================================
// ✔ FIXED: SmugMug API helper (must be ABOVE all routes)
// =========================================================
async function smug(endpoint) {
  // endpoint may or may not already include a querystring.
  // Always join APIKey safely to avoid malformed URLs.
  const joiner = String(endpoint || "").includes("?") ? "&" : "?";
  const url = `https://api.smugmug.com/api/v2${endpoint}${joiner}APIKey=${SMUG_API_KEY}`;

  const r = await fetch(url, {
    headers: {
      Accept: "application/json",
      "User-Agent": "SmugProxy/1.0"
    }
  });

  if (!r.ok) {
    let body = "";
    try {
      body = await r.text();
    } catch (_) {}
    const snippet = String(body || "").slice(0, 280);
    throw new Error(
      `SmugMug upstream returned ${r.status} (${endpoint})${snippet ? ": " + snippet : ""}`
    );
  }

  return r.json();
}

// =========================================================
// ✔ NEW: CURATED INDEX (album keywords verified against image metadata)
//
// Computes per-album keyword verification and caches result.
//
// Endpoint:
//   GET /smug/curated-index/:albumKey
// Query:
//   refresh=1  (forces recompute, bypass cache)
// =========================================================

function normKeyword(s) {
  return String(s || "")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

const CURATED_INDEX_TTL_MS = Math.max(
  60_000,
  Number(process.env.CURATED_INDEX_TTL_MS || "21600000") // default 6h
);

const CURATED_CACHE_DIR = path.join(__dirname, ".cache-curated-index");
const curatedMemCache = new Map();

// =========================================================
// PEOPLE INDEX (from photo CAPTIONS; semicolon-delimited)
//
// Endpoint:
//   GET /index/people        (cached)
//   GET /index/people?force=1 (rebuild)
//
// Source of truth for which albums to scan:
//   Shows CSV rows that have at least one band_# filled in.
// Album resolution:
//   show_url is treated as a SmugMug *image* URL; we extract ImageKey (i-XXXX)
//   then resolve the parent AlbumKey via SmugMug API.
//
// Output shape:
//   { generatedAt, albumsScanned, people: [ { name, albums:[{albumKey,title,url}...] } ] }
//
// Notes:
// - Uses disk cache (people-index.json) + memory cache.
// - Render free-tier has no cron; rebuild can be triggered client-side once/day.
// =========================================================

const PEOPLE_INDEX_TTL_MS = Math.max(
  60_000,
  Number(process.env.PEOPLE_INDEX_TTL_MS || String(1000 * 60 * 60 * 24)) // default 24h
);

const PEOPLE_INDEX_FILE = path.join(__dirname, "people-index.json");
let peopleIndexMem = null; // { generatedAt, albumsScanned, people:[...] }
let peopleIndexBuildPromise = null; // prevents concurrent long rebuilds

function safeReadJsonFile(p) {
  try {
    const raw = fs.readFileSync(p, "utf8");
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch (_) {
    return null;
  }
}

function safeWriteJsonFile(p, obj) {
  try {
    fs.writeFileSync(p, JSON.stringify(obj), "utf8");
  } catch (e) {
    console.warn("people-index write failed:", e && e.message ? e.message : e);
  }
}

function isFreshGeneratedAt(iso, ttlMs) {
  const t = Date.parse(String(iso || ""));
  if (!Number.isFinite(t)) return false;
  return Date.now() - t < ttlMs;
}

function parseCsvSimple(csvText) {
  const raw = String(csvText || "").trim();
  if (!raw) return { header: [], rows: [] };
  const lines = raw.split(/\r?\n/).filter((l) => l.trim());
  const headerLine = lines.shift();
  if (!headerLine) return { header: [], rows: [] };

  function parseLine(line) {
    const out = [];
    let cur = "";
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"') {
        if (inQuotes && line[i + 1] === '"') {
          cur += '"';
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (ch === ',' && !inQuotes) {
        out.push(cur);
        cur = "";
      } else {
        cur += ch;
      }
    }
    out.push(cur);
    return out;
  }

  const header = parseLine(headerLine).map((h) => String(h || "").trim());
  const rows = lines.map((l) => parseLine(l));
  return { header, rows };
}

function extractImageKeyFromUrl(url) {
  const u = String(url || "").trim();
  if (!u) return "";
  // common SmugMug image URL contains /i-<ImageKey>
  const m = u.match(/\b\/i-([A-Za-z0-9]+)\b/);
  return m ? String(m[1] || "").trim() : "";
}

function smugEndpointFromUri(uri) {
  const u = String(uri || "").trim();
  if (!u) return "";
  const m = u.match(/\/api\/v2(\/.*)$/i);
  if (m) return String(m[1] || "");
  if (u.startsWith("/")) return u;
  return "";
}

function folderPathToApiPath(folderPath) {
  const parts = String(folderPath || "")
    .split("/")
    .map((s) => String(s || "").trim())
    .filter(Boolean)
    .map((seg) => encodeURIComponent(seg));
  return parts.join("/");
}

async function listAlbumsAndFoldersRecursive(rootFolderPath) {
  const albums = new Map(); // albumKey -> { albumKey, title, url }
  const visitedFolders = new Set();
  const queue = [String(rootFolderPath || "").trim()].filter(Boolean);

  while (queue.length) {
    const folderPath = queue.shift();
    const folderKey = String(folderPath || "");
    if (!folderKey || visitedFolders.has(folderKey)) continue;
    visitedFolders.add(folderKey);

    const apiFolderPath = folderPathToApiPath(folderPath);
    if (!apiFolderPath) continue;

    // Albums in this folder
    try {
      const page = await smug(`/folder/user/${encodeURIComponent(SMUG_NICKNAME)}/${apiFolderPath}!albums?_accept=application/json&_verbosity=1&_expand=Album`);
      const resp = page && page.Response ? page.Response : page;
      const items = resp && (resp.FolderAlbum || resp.Albums || resp.Album)
        ? (resp.FolderAlbum || resp.Albums || resp.Album)
        : [];
      if (Array.isArray(items)) {
        for (const it of items) {
          const a = (it && it.Album) || it;
          const albumKey = a && (a.AlbumKey || a.Key) ? String(a.AlbumKey || a.Key).trim() : "";
          if (!albumKey) continue;
          if (albums.has(albumKey)) continue;
          const title = String(a.Title || a.Name || "").trim();
          const url = String(a.WebUri || a.Url || a.URL || a.Uri || "").trim();
          albums.set(albumKey, { albumKey, title, url });
          if (PEOPLE_INDEX_MAX_ALBUMS && albums.size >= PEOPLE_INDEX_MAX_ALBUMS) {
            return Array.from(albums.values());
          }
        }
      }
    } catch (e) {
      console.warn("people-index: folder albums list failed:", folderPath, e && e.message ? e.message : e);
    }

    // Subfolders
    try {
      const page = await smug(`/folder/user/${encodeURIComponent(SMUG_NICKNAME)}/${apiFolderPath}!folders?_accept=application/json&_verbosity=1`);
      const resp = page && page.Response ? page.Response : page;
      const folders = resp && (resp.Folder || resp.Folders) ? (resp.Folder || resp.Folders) : [];
      if (Array.isArray(folders)) {
        for (const f of folders) {
          const uri =
            (f && (f.Uri || f.URI)) ||
            (f && f.Uris && f.Uris.Folder && (f.Uris.Folder.Uri || f.Uris.Folder.URI)) ||
            "";
          const endpoint = smugEndpointFromUri(uri);
          if (endpoint) {
            const mm = String(endpoint).match(/^\/folder\/user\/[^/]+\/(.+)$/i);
            if (mm && mm[1]) {
              const decoded = String(mm[1])
                .split("/")
                .map((seg) => {
                  try { return decodeURIComponent(seg); } catch (_) { return seg; }
                })
                .join("/");
              if (decoded) queue.push(decoded);
            }
            continue;
          }

          const name = String((f && (f.Name || f.FolderName)) || "").trim();
          if (name) queue.push(`${folderPath.replace(/\/+$/g, "")}/${name}`);
        }
      }
    } catch (e) {
      console.warn("people-index: folder subfolders list failed:", folderPath, e && e.message ? e.message : e);
    }
  }

  return Array.from(albums.values());
}

function extractAlbumKeyFromImageDetail(json) {
  const resp = json && json.Response ? json.Response : json;
  const img = resp && (resp.Image || resp.image || resp);
  if (!img || typeof img !== "object") return "";

  // Direct
  if (img.AlbumKey) return String(img.AlbumKey);
  if (img.Album && img.Album.AlbumKey) return String(img.Album.AlbumKey);

  // Via Uris
  const uri =
    (img.Uris && img.Uris.Album && img.Uris.Album.Uri) ||
    (img.Uris && img.Uris.Album && img.Uris.Album.URI) ||
    (img.Uris && img.Uris.Album && img.Uris.Album.Url) ||
    "";
  const m = String(uri || "").match(/\/album\/([^/?#]+)/i);
  if (m) return String(m[1] || "");

  // Some SmugMug payloads don't include Uris.Album, but do include other
  // Uris entries that embed the album key (e.g., AlbumImage, HighlightImage,
  // LargestImage, etc.). Walk the Uris object and extract the first /album/<key>.
  try {
    const uris = img.Uris && typeof img.Uris === "object" ? img.Uris : null;
    if (uris) {
      const stack = [uris];
      const seen = new Set();
      while (stack.length) {
        const cur = stack.pop();
        if (!cur || typeof cur !== "object") continue;
        if (seen.has(cur)) continue;
        seen.add(cur);

        // Common fields in SmugMug v2 payloads
        const candidates = [cur.Uri, cur.URI, cur.Url, cur.URL];
        for (const c of candidates) {
          const mm = String(c || "").match(/\/album\/([^/?#]+)/i);
          if (mm) return String(mm[1] || "");
        }

        for (const v of Object.values(cur)) {
          if (v && typeof v === "object") stack.push(v);
        }
      }
    }
  } catch (_) {
    // fall through
  }

  return "";
}

function extractAlbumMeta(json) {
  const resp = json && json.Response ? json.Response : json;
  const album = resp && (resp.Album || resp.album || resp);
  if (!album || typeof album !== "object") return { title: "", url: "" };
  const title = String(album.Title || album.Name || "").trim();
  const url = String(album.WebUri || album.Url || album.URL || album.Uri || "").trim();
  return { title, url };
}

function parsePeopleFromCaption(caption) {
  const raw = String(caption || "").trim();
  if (!raw) return [];
  return raw
    .split(/\s*;\s*/g)
    .map((s) => String(s || "").trim())
    .filter(Boolean);
}

async function mapLimit(items, limit, fn) {
  const out = new Array(items.length);
  let i = 0;
  async function worker() {
    while (true) {
      const idx = i++;
      if (idx >= items.length) return;
      out[idx] = await fn(items[idx], idx);
    }
  }
  const workers = Array.from({ length: Math.max(1, limit) }, () => worker());
  await Promise.all(workers);
  return out;
}

async function computePeopleIndexFromShows() {
  // 1) Pull Shows CSV
  const r = await fetch(SHOWS_SHEET_URL);
  const csv = await r.text();
  const { header, rows } = parseCsvSimple(csv);
  const hl = header.map((h) => String(h || "").trim().toLowerCase());

  const normalizeHeader = (s) =>
    String(s || "")
      .toLowerCase()
      .trim()
      .replace(/[^a-z0-9]+/g, "");

  const hn = header.map(normalizeHeader);

  // Accept a few common variants (show_url, show url, showurl, poster_url, etc.)
  const urlIdx = (() => {
    const want = ["showurl", "posterurl"];
    for (const w of want) {
      const ix = hn.indexOf(w);
      if (ix !== -1) return ix;
    }
    // Fallback: any header that looks like show*url / poster*url
    for (let i = 0; i < hn.length; i++) {
      const h = hn[i];
      if (h && (h.startsWith("show") || h.startsWith("poster")) && h.endsWith("url")) return i;
    }
    return -1;
  })();

  // Discover all band columns dynamically: band_1, band 1, band-1, band1, etc.
  const bandIdxs = hn
    .map((h, idx) => {
      const m = /^band(\d+)$/.exec(h || "");
      return m ? { n: Number(m[1] || 0), idx } : null;
    })
    .filter(Boolean)
    .sort((a, b) => a.n - b.n)
    .map((o) => o.idx);

  const showUrls = [];
  for (const cols of rows) {
    if (!Array.isArray(cols) || !cols.length) continue;
    const hasBands = bandIdxs.some((ix) => ix !== -1 && String(cols[ix] || "").trim());
    if (!hasBands) continue;
    const u = urlIdx !== -1 ? String(cols[urlIdx] || "").trim() : "";
    if (u) showUrls.push(u);
  }

  // 2) Resolve unique album keys via show_url image keys
  const albumKeySet = new Set();
  const urlToAlbumKey = new Map();

  await mapLimit(showUrls, 3, async (showUrl) => {
    try {
      const imageKey = extractImageKeyFromUrl(showUrl);
      if (!imageKey) return;
      const img = await smug(`/image/${encodeURIComponent(imageKey)}-0?_accept=application/json&_verbosity=1&_expand=Image`);
      const albumKey = extractAlbumKeyFromImageDetail(img);
      if (!albumKey) return;
      urlToAlbumKey.set(showUrl, albumKey);
      albumKeySet.add(albumKey);
    } catch (e) {
      // continue
      console.warn("people-index: show_url resolve failed:", showUrl, e && e.message ? e.message : e);
    }
  });

  const albumKeys = Array.from(albumKeySet);

  // 3) For each album, scan captions
  const peopleToAlbums = new Map(); // name -> Map(albumKey -> {albumKey,title,url})

  async function ensureAlbumMeta(albumKey) {
    try {
      const meta = await smug(`/album/${encodeURIComponent(albumKey)}?_accept=application/json&_verbosity=1`);
      return extractAlbumMeta(meta);
    } catch (_) {
      return { title: "", url: "" };
    }
  }

  async function scanAlbum(albumKey) {
    const meta = await ensureAlbumMeta(albumKey);

    // page through images
    let start = 1;
    const count = 200;
    while (true) {
      const page = await smug(`/album/${encodeURIComponent(albumKey)}!images?count=${count}&start=${start}&_accept=application/json&_expand=Image`);
      const resp = page && page.Response ? page.Response : page;
      const items = resp && resp.AlbumImage ? resp.AlbumImage : [];
      if (!Array.isArray(items) || items.length === 0) break;

      // Prefer captions available in the page payload.
      const imageKeysNeedingDetail = [];
      for (const ai of items) {
        const cap =
          (ai && ai.Caption) ||
          (ai && ai.Image && ai.Image.Caption) ||
          (ai && ai.Image && ai.Image.CaptionText) ||
          "";
        const names = parsePeopleFromCaption(cap);
        if (names.length) {
          for (const n of names) {
            const key = String(n).trim();
            if (!key) continue;
            if (!peopleToAlbums.has(key)) peopleToAlbums.set(key, new Map());
            peopleToAlbums.get(key).set(albumKey, { albumKey, title: meta.title, url: meta.url });
          }
          continue;
        }

        const ik = (ai && ai.Image && ai.Image.ImageKey) || ai.ImageKey || "";
        if (ik) imageKeysNeedingDetail.push(String(ik));
      }

      // If captions weren't included, fetch per-image detail for the remaining.
      await mapLimit(imageKeysNeedingDetail, 4, async (imageKey) => {
        try {
          const detail = await smug(`/image/${encodeURIComponent(imageKey)}-0?_accept=application/json&_verbosity=1&_expand=Image`);
          const resp2 = detail && detail.Response ? detail.Response : detail;
          const img = resp2 && (resp2.Image || resp2.image || resp2);
          const cap = img && (img.Caption || img.CaptionText || "");
          const names = parsePeopleFromCaption(cap);
          if (!names.length) return;
          for (const n of names) {
            const key = String(n).trim();
            if (!key) continue;
            if (!peopleToAlbums.has(key)) peopleToAlbums.set(key, new Map());
            peopleToAlbums.get(key).set(albumKey, { albumKey, title: meta.title, url: meta.url });
          }
        } catch (_) {
          // continue
        }
      });

      if (items.length < count) break;
      start += count;
    }
  }

  await mapLimit(albumKeys, 2, async (albumKey) => {
    try {
      await scanAlbum(albumKey);
    } catch (e) {
      console.warn("people-index: album scan failed:", albumKey, e && e.message ? e.message : e);
    }
  });

  // 4) Shape payload
  const people = Array.from(peopleToAlbums.entries())
    .map(([name, albumMap]) => {
      const albums = Array.from(albumMap.values());
      return { name, albums };
    })
    .sort((a, b) => String(a.name).localeCompare(String(b.name)));

  return {
    generatedAt: new Date().toISOString(),
    albumsScanned: albumKeys.length,
    people
  };
}

async function computePeopleIndexFromBandsFolder() {
  const discovered = await listAlbumsAndFoldersRecursive(PEOPLE_INDEX_BANDS_ROOT);
  let albumKeys = discovered.map((a) => a.albumKey).filter(Boolean);

  // Safety cap to avoid upstream timeouts. If PEOPLE_INDEX_MAX_ALBUMS is 0, no cap.
  if (PEOPLE_INDEX_MAX_ALBUMS > 0 && albumKeys.length > PEOPLE_INDEX_MAX_ALBUMS) {
    albumKeys = albumKeys.slice(0, PEOPLE_INDEX_MAX_ALBUMS);
  }

  const peopleToAlbums = new Map();

  async function ensureAlbumMeta(albumKey) {
    const pre = discovered.find((x) => x.albumKey === albumKey);
    if (pre && (pre.title || pre.url)) return { title: pre.title || "", url: pre.url || "" };
    try {
      const meta = await smug(`/album/${encodeURIComponent(albumKey)}?_accept=application/json&_verbosity=1`);
      return extractAlbumMeta(meta);
    } catch (_) {
      return { title: "", url: "" };
    }
  }

  async function scanAlbum(albumKey) {
    const meta = await ensureAlbumMeta(albumKey);
    let start = 1;
    const count = 200;
    while (true) {
      // Request higher verbosity so captions are more likely to be present in the page payload,
      // reducing expensive per-image detail calls.
      const page = await smug(`/album/${encodeURIComponent(albumKey)}!images?count=${count}&start=${start}&_accept=application/json&_expand=Image&_verbosity=3`);
      const resp = page && page.Response ? page.Response : page;
      const items = resp && resp.AlbumImage ? resp.AlbumImage : [];
      if (!Array.isArray(items) || items.length === 0) break;

      const imageKeysNeedingDetail = [];
      for (const ai of items) {
        const cap =
          (ai && ai.Caption) ||
          (ai && ai.Image && ai.Image.Caption) ||
          (ai && ai.Image && ai.Image.CaptionText) ||
          "";
        const names = parsePeopleFromCaption(cap);
        if (names.length) {
          for (const n of names) {
            const key = String(n).trim();
            if (!key) continue;
            if (!peopleToAlbums.has(key)) peopleToAlbums.set(key, new Map());
            peopleToAlbums.get(key).set(albumKey, { albumKey, title: meta.title, url: meta.url });
          }
          continue;
        }

        const ik = (ai && ai.Image && ai.Image.ImageKey) || ai.ImageKey || "";
        if (ik) imageKeysNeedingDetail.push(String(ik));
      }

      await mapLimit(imageKeysNeedingDetail, 4, async (imageKey) => {
        try {
          const detail = await smug(`/image/${encodeURIComponent(imageKey)}-0?_accept=application/json&_verbosity=1&_expand=Image`);
          const resp2 = detail && detail.Response ? detail.Response : detail;
          const img = resp2 && (resp2.Image || resp2.image || resp2);
          const cap = img && (img.Caption || img.CaptionText || "");
          const names = parsePeopleFromCaption(cap);
          if (!names.length) return;
          for (const n of names) {
            const key = String(n).trim();
            if (!key) continue;
            if (!peopleToAlbums.has(key)) peopleToAlbums.set(key, new Map());
            peopleToAlbums.get(key).set(albumKey, { albumKey, title: meta.title, url: meta.url });
          }
        } catch (_) {
          // continue
        }
      });

      if (items.length < count) break;
      start += count;
    }
  }

  await mapLimit(albumKeys, 2, async (albumKey) => {
    try {
      await scanAlbum(albumKey);
    } catch (e) {
      console.warn("people-index: album scan failed:", albumKey, e && e.message ? e.message : e);
    }
  });

  const people = Array.from(peopleToAlbums.entries())
    .map(([name, albumMap]) => ({ name, albums: Array.from(albumMap.values()) }))
    .sort((a, b) => String(a.name).localeCompare(String(b.name)));

  return {
    generatedAt: new Date().toISOString(),
    albumsScanned: albumKeys.length,
    people
  };
}

function curatedCacheKey(albumKey) {
  // bump v1 when logic changes
  return `curated-index:v1:${albumKey}`;
}

function safeJsonParse(s) {
  try {
    return JSON.parse(s);
  } catch (_) {
    return null;
  }
}

async function ensureCuratedCacheDir() {
  try {
    await fs.promises.mkdir(CURATED_CACHE_DIR, { recursive: true });
  } catch (_) {}
}

function curatedCacheFile(albumKey) {
  // keep filename safe
  const safe = String(albumKey || "").replace(/[^a-zA-Z0-9_-]+/g, "_");
  return path.join(CURATED_CACHE_DIR, `${safe}.json`);
}

async function readCuratedDiskCache(albumKey) {
  try {
    const p = curatedCacheFile(albumKey);
    const raw = await fs.promises.readFile(p, "utf8");
    const parsed = safeJsonParse(raw);
    if (!parsed || typeof parsed !== "object") return null;
    return parsed;
  } catch (_) {
    return null;
  }
}

async function writeCuratedDiskCache(albumKey, payload) {
  try {
    await ensureCuratedCacheDir();
    const p = curatedCacheFile(albumKey);
    await fs.promises.writeFile(p, JSON.stringify(payload), "utf8");
  } catch (e) {
    // cache write failures should never break endpoint
    console.warn("curated-index cache write failed:", e && e.message ? e.message : e);
  }
}

function isCacheFresh(storedAtIso, ttlMs) {
  const t = Date.parse(String(storedAtIso || ""));
  if (!Number.isFinite(t)) return false;
  return Date.now() - t < ttlMs;
}

function extractAlbumKeywords(metaJson) {
  // Be resilient to varying SmugMug shapes.
  // Prefer a keyword array if present; fall back to comma-delimited string.
  const resp = metaJson && metaJson.Response ? metaJson.Response : metaJson;

  const maybeArray =
    resp?.KeywordArray ||
    resp?.Album?.KeywordArray ||
    resp?.Album?.Keywords?.KeywordArray ||
    resp?.Album?.Keywords ||
    resp?.Album?.Keyword;

  if (Array.isArray(maybeArray)) {
    return maybeArray
      .map(k => (typeof k === "string" ? k : k && k.Name ? String(k.Name) : ""))
      .map(s => s.trim())
      .filter(Boolean);
  }

  const maybeString =
    resp?.Keywords || resp?.Album?.Keywords || (resp?.Album && resp.Album.Keywords);

  if (typeof maybeString === "string") {
    return maybeString
      .split(/[,;]+/)
      .map(s => s.trim())
      .filter(Boolean);
  }

  return [];
}

function extractImageKeywordsFromAlbumImage(albumImage) {
  // We request _expand=Image.Keywords and _expand=KeywordArray where possible.
  // Try multiple shapes, then fall back to comma-delimited string.
  const maybeArray =
    albumImage?.KeywordArray ||
    albumImage?.Image?.KeywordArray ||
    albumImage?.Image?.Keywords?.KeywordArray ||
    albumImage?.Image?.Keywords;

  if (Array.isArray(maybeArray)) {
    return maybeArray
      .map(k => (typeof k === "string" ? k : k && k.Name ? String(k.Name) : ""))
      .map(s => s.trim())
      .filter(Boolean);
  }

  const maybeString = albumImage?.Keywords || albumImage?.Image?.Keywords;
  if (typeof maybeString === "string") {
    return maybeString
      .split(/[,;]+/)
      .map(s => s.trim())
      .filter(Boolean);
  }
  return [];
}

async function computeCuratedIndex(albumKey) {
  // 1) Fetch album keywords (curated). Some albums may not allow /album/{key} via APIKey-only;
  // we fall back to extracting Album keywords from the first !images page if possible.
  let albumKeywords = [];
  try {
    const meta = await smug(`/album/${encodeURIComponent(albumKey)}?_expand=Keywords&_expand=KeywordArray`);
    albumKeywords = extractAlbumKeywords(meta);
  } catch (e) {
    // We'll try to pull Album keywords from the first images page below.
    console.warn("curated-index: album-meta fetch failed (will fallback):", e && e.message ? e.message : e);
  }

  // 2) Fetch all album images (paged) and build keyword frequency map
  const imageKeywordCounts = Object.create(null);
  const totalImagesSeen = { n: 0 };

  // Helper: extract keywords from image detail response
  function extractKeywordsFromImageDetail(imageDetailJson) {
    const resp = imageDetailJson && imageDetailJson.Response ? imageDetailJson.Response : imageDetailJson;
    const img = resp?.Image || resp?.Response?.Image || resp;
    const maybeArray =
      img?.KeywordArray ||
      img?.Keywords?.KeywordArray ||
      img?.Keywords;
    if (Array.isArray(maybeArray)) {
      return maybeArray
        .map(k => (typeof k === "string" ? k : k && k.Name ? String(k.Name) : ""))
        .map(s => s.trim())
        .filter(Boolean);
    }
    if (typeof img?.Keywords === "string") {
      return img.Keywords.split(/[,;]+/).map(s => s.trim()).filter(Boolean);
    }
    return [];
  }

  // Concurrency-limited mapper (keeps SmugMug happy)
  async function mapLimit(items, limit, fn) {
    const out = new Array(items.length);
    let i = 0;
    async function worker() {
      while (true) {
        const idx = i++;
        if (idx >= items.length) return;
        out[idx] = await fn(items[idx], idx);
      }
    }
    const workers = Array.from({ length: Math.max(1, limit) }, () => worker());
    await Promise.all(workers);
    return out;
  }

  let start = 1;
  const count = 200;

  while (true) {
    const endpoint =
      `/album/${encodeURIComponent(albumKey)}!images?count=${count}&start=${start}` +
      `&_accept=application/json&_expand=Image`;

    const page = await smug(endpoint);
    const resp = page && page.Response ? page.Response : page;

    // Fallback album keywords from images page if album-meta didn't work
    if ((!albumKeywords || albumKeywords.length === 0) && resp?.Album) {
      albumKeywords = extractAlbumKeywords(resp);
    }

    const images = resp?.AlbumImage || [];
    if (!Array.isArray(images) || images.length === 0) break;

    totalImagesSeen.n += images.length;

    // For reliability: fetch image detail for each image to get keywords.
    // (AlbumImage payload often omits keywords when using APIKey-only.)
    const imageKeys = images
      .map(ai => ai?.Image?.ImageKey || ai?.ImageKey || "")
      .map(s => String(s).trim())
      .filter(Boolean);

    await mapLimit(imageKeys, 4, async (imageKey) => {
      const detail = await smug(
        `/image/${encodeURIComponent(imageKey)}-0?_accept=application/json&_verbosity=1&_expand=Image&_expand=Image.Keywords&_expand=KeywordArray`
      );
      const kws = extractKeywordsFromImageDetail(detail);
      for (const kw of kws) {
        const nk = normKeyword(kw);
        if (!nk) continue;
        imageKeywordCounts[nk] = (imageKeywordCounts[nk] || 0) + 1;
      }
    });

    if (images.length < count) break;
    start += count;
  }

  // 3) Verify curated keywords against image metadata
  const keywords = (albumKeywords || []).map((k) => {
    const nk = normKeyword(k);
    const c = imageKeywordCounts[nk] || 0;
    return { keyword: k, verified: c > 0, imageCount: c };
  });

  const verifiedKeywords = keywords.filter(k => k.verified).map(k => k.keyword);
  const missingKeywords = keywords.filter(k => !k.verified).map(k => k.keyword);

  return {
    albumKey,
    computedAt: new Date().toISOString(),
    ttlMs: CURATED_INDEX_TTL_MS,
    stats: {
      curatedKeywordCount: (albumKeywords || []).length,
      imagesScanned: totalImagesSeen.n
    },
    keywords,
    verifiedKeywords,
    missingKeywords,
    imageKeywordCounts
  };
}


app.get("/smug/curated-index/:albumKey", async (req, res) => {
  const albumKey = req.params.albumKey;
  const refresh = String(req.query.refresh || "") === "1";
  const debug = String(req.query.debug || "") === "1";

  allowCors(res, req);
  if (!albumKey) return res.status(400).json({ error: "missing albumKey" });

  const key = curatedCacheKey(albumKey);

  try {
    if (!refresh) {
      // 1) Memory cache
      const memHit = curatedMemCache.get(key);
      if (memHit && isCacheFresh(memHit?.computedAt, CURATED_INDEX_TTL_MS)) {
        return res.json({
          ...memHit,
          cache: { hit: true, layer: "memory", ageSec: Math.floor((Date.now() - Date.parse(memHit.computedAt)) / 1000) }
        });
      }

      // 2) Disk cache
      const diskHit = await readCuratedDiskCache(albumKey);
      if (diskHit && isCacheFresh(diskHit?.computedAt, CURATED_INDEX_TTL_MS)) {
        curatedMemCache.set(key, diskHit);
        return res.json({
          ...diskHit,
          cache: { hit: true, layer: "disk", ageSec: Math.floor((Date.now() - Date.parse(diskHit.computedAt)) / 1000) }
        });
      }
    }

    // Compute
    const computed = await computeCuratedIndex(albumKey);
    curatedMemCache.set(key, computed);
    await writeCuratedDiskCache(albumKey, computed);

    return res.json({
      ...computed,
      cache: { hit: false, layer: "computed", ageSec: 0 }
    });
  } catch (err) {
    console.error("curated-index failed:", err && err.message ? err.message : err);
    return res.status(500).json({
      error: "curated index failed",
      ...(debug ? { detail: String(err && err.message ? err.message : err) } : {})
    });
  }
});

// =========================================================
// PEOPLE INDEX (server-cached)
// =========================================================
app.get('/index/people', async (req, res) => {
  const force = String(req.query.force || '') === '1';
  allowCors(res, req);

  try {
    // 1) Memory cache
    if (!force && peopleIndexMem && isFreshGeneratedAt(peopleIndexMem.generatedAt, PEOPLE_INDEX_TTL_MS)) {
      return res.json({ ...peopleIndexMem, cache: { hit: true, layer: 'memory' } });
    }

    // 2) Disk cache (ignore cached-empty results so we don't get stuck at albumsScanned=0 forever)
    if (!force) {
      const disk = safeReadJsonFile(PEOPLE_INDEX_FILE);
      const looksEmpty = disk && Number(disk.albumsScanned || 0) === 0 && Array.isArray(disk.people) && disk.people.length === 0;
      if (!looksEmpty && disk && isFreshGeneratedAt(disk.generatedAt, PEOPLE_INDEX_TTL_MS)) {
        peopleIndexMem = disk;
        return res.json({ ...disk, cache: { hit: true, layer: 'disk' } });
      }
    }

    // 3) Compute (source of truth: recursively scan albums under PEOPLE_INDEX_BANDS_ROOT)
    // Prevent multiple concurrent rebuilds from crushing the instance or timing out.
    if (peopleIndexBuildPromise) {
      if (!force) {
        // Serve best-effort cached data while a build is running.
        if (peopleIndexMem) return res.json({ ...peopleIndexMem, cache: { hit: true, layer: 'memory', building: true } });
        const disk = safeReadJsonFile(PEOPLE_INDEX_FILE);
        if (disk) return res.json({ ...disk, cache: { hit: true, layer: 'disk', building: true } });
      }
      return res.status(202).json({ status: 'building', message: 'People index rebuild already in progress.' });
    }

    peopleIndexBuildPromise = (async () => {
      const computed = await computePeopleIndexFromBandsFolder();
      peopleIndexMem = computed;
      safeWriteJsonFile(PEOPLE_INDEX_FILE, computed);
      return computed;
    })();

    try {
      const computed = await peopleIndexBuildPromise;
      return res.json({ ...computed, cache: { hit: false, layer: 'computed' } });
    } finally {
      peopleIndexBuildPromise = null;
    }
  } catch (err) {
    console.error('people index failed:', err && err.message ? err.message : err);
    return res.status(500).json({ error: 'people index failed' });
  }
});

// =========================================================
// SHEETS → CSV
// =========================================================
app.get("/sheet/bands", async (req, res) => {
  try {
    const r = await fetch(BANDS_SHEET_URL);
    const csv = await r.text();
    allowCors(res);
    res.type("text/plain").send(csv);
  } catch (err) {
    console.error("sheet /bands fetch failed:", err);
    allowCors(res);
    res.status(500).send("sheet error");
  }
});

app.get("/sheet/shows", async (req, res) => {
  try {
    const r = await fetch(SHOWS_SHEET_URL);
    const csv = await r.text();
    allowCors(res);
    res.type("text/plain").send(csv);
  } catch (err) {
    console.error("sheet /shows fetch failed:", err);
    allowCors(res);
    res.status(500).send("shows sheet error");
  }
});

// Stats tab (Fix / Metadata)
// Aliases included to match different frontend endpoint names used over time.
async function sendStatsCsv(req, res) {
  try {
    const r = await fetch(STATS_SHEET_URL);
    const csv = await r.text();
    allowCors(res);
    res.type("text/plain").send(csv);
  } catch (err) {
    console.error("sheet /stats fetch failed:", err);
    allowCors(res);
    res.status(500).send("stats sheet error");
  }
}

app.get("/sheet/stats", sendStatsCsv);
app.get("/sheet/stats/", sendStatsCsv);
app.get("/sheet/fix_metadata", sendStatsCsv);
app.get("/sheet/fix_metadata/", sendStatsCsv);
app.get("/sheet/fix-metadata", sendStatsCsv);
app.get("/sheet/fix-metadata/", sendStatsCsv);
app.get("/sheet/fixmetadata", sendStatsCsv);
app.get("/sheet/fixmetadata/", sendStatsCsv);
app.get("/sheet/fix", sendStatsCsv);
app.get("/sheet/fix/", sendStatsCsv);

// =========================================================
// IMAGE PROXY (posters)
// =========================================================
app.get("/show-poster", async (req, res) => {
  allowCors(res);
  const remoteUrl = req.query.url;
  if (!remoteUrl) return res.status(400).send("missing url");

  try {
    const upstream = await fetch(remoteUrl);
    if (!upstream.ok) {
      console.error("upstream not ok:", upstream.status, remoteUrl);
      return res.status(502).send("bad upstream");
    }

    const contentType = upstream.headers.get("content-type") || "image/jpeg";
    res.setHeader("Content-Type", contentType);
    res.send(Buffer.from(await upstream.arrayBuffer()));
  } catch (err) {
    console.error("proxy image error:", err);
    res.status(500).send("error");
  }
});

// =========================================================
// SMART FOLDER → ALBUMS
// =========================================================
app.get("/smug/:slug", async (req, res) => {
  const slug = req.params.slug;
  const folderFromSheet = req.query.folder;
  const region = req.query.region || "Local";

  const REGION_FOLDER_BASE = {
    Local: "Local",
    Regional: "Regional",
    National: "National",
    International: "International"
  };

  const regionFolder = REGION_FOLDER_BASE[region] || REGION_FOLDER_BASE.Local;

  const base = `https://api.smugmug.com/api/v2/folder/user/vmpix/Music/Archives/Bands/${regionFolder}`;

  const candidates = [];
  if (folderFromSheet) {
    candidates.push(folderFromSheet);
    candidates.push(folderFromSheet.replace(/\s+/g, "-"));
  }

  const rawLower = slug.replace(/-/g, " ");
  const words = rawLower.split(" ").filter(Boolean);

  const SMALL = new Set(["of", "the", "and", "a", "an", "to", "for", "at", "by", "with", "in"]);

  const titleSmart = words
    .map((w, i) => {
      const lw = w.toLowerCase();
      if (i !== 0 && SMALL.has(lw)) return lw;
      return lw.charAt(0).toUpperCase() + lw.slice(1);
    })
    .join(" ");

  const titleAll = words.map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(" ");

  const noSpaces = rawLower.replace(/\s+/g, "");
  const dashedSmart = titleSmart.replace(/\s+/g, "-");

  candidates.push(titleSmart, titleAll, rawLower, dashedSmart, noSpaces);

  let successData = null;
  let usedUrl = null;

  for (const name of candidates) {
    const url = `${base}/${encodeURIComponent(name)}!albums?APIKey=${SMUG_API_KEY}`;
    console.log("Trying:", url);

    try {
      const r = await fetch(url, {
        headers: {
          Accept: "application/json",
          "User-Agent": "SmugProxy/1.0"
        }
      });

      if (r.ok) {
        const data = await r.json();
        if (data && data.Response && Array.isArray(data.Response.Album)) {
          successData = data;
          usedUrl = url;
          break;
        }
      }
    } catch (err) {
      console.log("Error fetching", url, err.message);
    }
  }

  allowCors(res);

  if (successData) {
    successData._usedUrl = usedUrl;
    return res.json(successData);
  }

  res.json({
    Response: { Album: [] },
    info: `No albums found for slug=${slug} (tried: ${candidates.join(" | ")})`
  });
});

// =========================================================
// ALBUM → IMAGES (paged)
// =========================================================
app.get("/smug/album/:albumKey", async (req, res) => {
  const albumKey = req.params.albumKey;
  const count = req.query.count || 200;
  const start = req.query.start || 1;

  const url = `https://api.smugmug.com/api/v2/album/${encodeURIComponent(
    albumKey
  )}!images?APIKey=${SMUG_API_KEY}&count=${count}&start=${start}&_accept=application/json&_expand=Image`;

  console.log("PROXY ALBUM IMAGES:", url);

  try {
    const upstream = await fetch(url, {
      headers: {
        Accept: "application/json",
        "User-Agent": "SmugProxy/1.0"
      }
    });

    if (!upstream.ok) {
      console.error("upstream album error:", upstream.status, await upstream.text());
      res.set("Access-Control-Allow-Origin", "*");
      return res.status(upstream.status).json({ error: "album images upstream error" });
    }

    const data = await upstream.json();
    res.set("Access-Control-Allow-Origin", "*");
    return res.json(data);
  } catch (err) {
    console.error("album images proxy error:", err);
    res.set("Access-Control-Allow-Origin", "*");
    return res.status(500).json({ error: "album images proxy failed" });
  }
});

// =========================================================
// ✔ NEW: ALBUM METADATA (album keywords)
// =========================================================
app.get("/smug/album-meta/:albumKey", async (req, res) => {
  const albumKey = req.params.albumKey;

  try {
    const result = await smug(
      `/album/${encodeURIComponent(albumKey)}?_expand=Keywords&_expand=KeywordArray`
    );

    allowCors(res);
    return res.json(result);
  } catch (err) {
    console.error("Error fetching album metadata:", err);
    allowCors(res);
    return res.status(500).json({ error: "Failed to fetch album metadata" });
  }
});

// =========================================================
// IMAGE DETAIL (keywords, caption, etc.)
// =========================================================
app.get("/smug/image/:imageKey", async (req, res) => {
  const imageKey = req.params.imageKey;

  const url = `https://api.smugmug.com/api/v2/image/${encodeURIComponent(
    imageKey
  )}-0?APIKey=${SMUG_API_KEY}&_accept=application/json&_verbosity=1&_expand=Image&_expand=Image.Keywords&_expand=KeywordArray`;

  console.log("FETCHING IMAGE DETAIL:", url);

  try {
    const r = await fetch(url, {
      headers: {
        Accept: "application/json",
        "User-Agent": "SmugProxy/1.0"
      }
    });

    const data = await r.json();

    allowCors(res);
    return res.json(data);
  } catch (err) {
    console.error("error fetching image detail:", err);
    allowCors(res);
    return res.status(500).json({ error: "image detail fetch failed" });
  }
});



function fetchStreamWithRedirects(inputUrl, redirectsLeft = 5) {
  return new Promise((resolve, reject) => {
    try {
      if (!inputUrl || redirectsLeft < 0) return reject(new Error("Too many redirects"));
      const u = new URL(inputUrl);
      const lib = u.protocol === "https:" ? https : http;

      const req = lib.get(
        inputUrl,
        {
          headers: {
            "User-Agent": "MusicArchiveZip/1.0",
            "Accept": "*/*",
          },
        },
        (res) => {
          const code = res.statusCode || 0;

          // Redirects
          if (code >= 300 && code < 400 && res.headers.location) {
            const next = new URL(res.headers.location, inputUrl).toString();
            res.resume();
            return resolve(fetchStreamWithRedirects(next, redirectsLeft - 1));
          }

          if (code < 200 || code >= 300) {
            res.resume();
            return reject(new Error(`HTTP ${code} for ${inputUrl}`));
          }

          return resolve(res);
        }
      );

      req.on("error", reject);
    } catch (e) {
      reject(e);
    }
  });
}


// =========================================================
// ✔ NEW: ZIP BUILDER (multi-download)
// Expects: { items: [{ url, filename }, ...] }
// Returns: application/zip stream
// =========================================================
app.options("/zip", (req, res) => {
  allowCors(res);
  return res.status(204).send("");
});

app.post("/zip", async (req, res) => {
  try {
    allowCors(res);

    const items = Array.isArray(req.body?.items) ? req.body.items : [];
    if (!items.length) return res.status(400).send("No items provided");
    if (items.length > 120) return res.status(400).send("Too many items (max 120)");

    res.setHeader("Content-Type", "application/zip");
    res.setHeader("Content-Disposition", 'attachment; filename="photos.zip"');

    const archive = archiver("zip", { zlib: { level: 9 } });

    archive.on("warning", (err) => {
      console.warn("zip warning:", err);
    });

    archive.on("error", (err) => {
      console.error("zip error:", err);
      try { if (!res.headersSent) res.status(500).send("ZIP error"); } catch (_) {}
      try { res.end(); } catch (_) {}
    });

    archive.pipe(res);

    for (let i = 0; i < items.length; i++) {
      const it = items[i] || {};
      const url = String(it.url || "").trim();
      let filename = String(it.filename || `photo-${i + 1}.jpg`).trim();

      // basic sanitization
      filename = filename.replace(/[\/\\:*?"<>|]+/g, "-").slice(0, 160) || `photo-${i + 1}.jpg`;

      if (!/^https?:\/\//i.test(url)) continue;

      // Server-to-server fetch as stream (no global fetch needed)
      let stream;
      try {
        stream = await fetchStreamWithRedirects(url);
      } catch (e) {
        console.warn("zip fetch failed:", String(e && e.message ? e.message : e), url);
        continue;
      }

      archive.append(stream, { name: filename });
    }

    await archive.finalize();
  } catch (err) {
    console.error("POST /zip failed:", err);
    try {
      allowCors(res);
      return res.status(500).send("ZIP failed");
    } catch (_) {
      try { res.end(); } catch (_) {}
    }
  }
});

// =========================================================
// ✔ NEW: ANALYTICS EVENT LOGGER (no Google Analytics)
//
// Frontend calls: POST /track (or navigator.sendBeacon to /track)
// Server forwards the payload to a Google Apps Script Web App
// that appends the row into your Google Sheet tab (e.g. "Analytics").
//
// Env vars:
//   ANALYTICS_WEBAPP_URL  (required to enable logging)
//   ANALYTICS_KEY         (optional shared secret; if you add checks in Apps Script)
// =========================================================

const ANALYTICS_WEBAPP_URL = process.env.ANALYTICS_WEBAPP_URL || "";
const ANALYTICS_KEY = process.env.ANALYTICS_KEY || "";

app.post("/track", async (req, res) => {
  allowCors(res);

  // Always respond quickly so the UI never feels slow.
  // (We still try to forward the event to Sheets in the background.)
  res.status(204).send("");

  try {
    if (!ANALYTICS_WEBAPP_URL) return; // logging disabled if not configured

    const body = req.body && typeof req.body === "object" ? req.body : {};
    const eventName = String(body.event || "").trim();
    if (!eventName) return;    // Keep payload small + predictable (matches your Sheet headers)
    const sessionid = (body.sessionid || body.sessionId || body.sessionID) ? String(body.sessionid || body.sessionId || body.sessionID) : '';

    // Extra is optional; if provided as an object, keep it.
    // We also tuck request context into extra._ctx so it never breaks column mapping.
    let extra = undefined;
    if (body.extra && typeof body.extra === 'object') {
      extra = body.extra;
    }
    if (extra && typeof extra === 'object') {
      extra._ctx = Object.assign({}, extra._ctx || {}, {
        ip: String(req.headers['x-forwarded-for'] || req.socket?.remoteAddress || '').slice(0, 64),
        ua: String(req.headers['user-agent'] || '').slice(0, 220)
      });
    }

    const payload = {
      event: eventName.slice(0, 64),
      route: body.route ? String(body.route).slice(0, 120) : '',
      view: body.view ? String(body.view).slice(0, 64) : '',
      band: body.band ? String(body.band).slice(0, 120) : '',
      show: body.show ? String(body.show).slice(0, 160) : '',
      album: body.album ? String(body.album).slice(0, 180) : '',
      photo: body.photo ? String(body.photo).slice(0, 220) : '',
      year: body.year ? String(body.year).slice(0, 16) : '',
      category: body.category ? String(body.category).slice(0, 48) : '',
      source: body.source ? String(body.source).slice(0, 48) : '',
      page: body.page ? String(body.page).slice(0, 400) : '',
      referrer: body.referrer ? String(body.referrer).slice(0, 400) : '',
      sessionid: sessionid.slice(0, 80),
      extra: extra
    };

    const headers = { "Content-Type": "application/json" };
    if (ANALYTICS_KEY) headers["X-Analytics-Key"] = ANALYTICS_KEY;

    // Forward to your Apps Script Web App endpoint
    await fetch(ANALYTICS_WEBAPP_URL, {
      method: "POST",
      headers,
      body: JSON.stringify(payload)
    });
  } catch (err) {
    // Don't throw; analytics should never break the site.
    console.warn("/track forward failed:", err && err.message ? err.message : err);
  }
});

// (People index route is defined earlier; older duplicate implementation removed.)

// =========================================================
// 404 (keep CORS headers on missing routes too)
// =========================================================
app.use((req, res) => {
  allowCors(res);
  res.status(404).json({ error: "Not found", path: req.originalUrl });
});

// =========================================================
// SERVER START
// =========================================================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log("Server listening on http://localhost:" + PORT);
});
