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

// SmugMug account + People Index configuration
// (Required by /index/people. Defaults match Chris' vmpix SmugMug structure.)
const SMUG_NICKNAME = String(process.env.SMUG_NICKNAME || "vmpix").trim();

// Root folder to recursively scan for Albums when building the People index.
// Example: https://vmpix.smugmug.com/Music/Archives/Bands/
const PEOPLE_INDEX_BANDS_ROOT = String(process.env.PEOPLE_INDEX_BANDS_ROOT || "Music/Archives/Bands").trim();

// Safety cap (0 = no cap). Default tuned to avoid free-tier timeouts.
const PEOPLE_INDEX_MAX_ALBUMS = Math.max(
  0,
  Number(process.env.PEOPLE_INDEX_MAX_ALBUMS || "800")
);

// Google Sheets (your existing CSV sources)
const BANDS_SHEET_URL =
  "https://docs.google.com/spreadsheets/d/e/2PACX-1vTdi19qTDyPeBGzq0PpkdlDS_bNg34XpdRiXy8aBa-Jlu-jg2Wzkj1SnLXtRVFU4TGOh5KHJPK8Lwhc/pub?gid=0&single=true&output=csv";

const NEW_SHEET_BANDS_URL = String(
  process.env.NEW_SHEET_BANDS_URL ||
  "https://docs.google.com/spreadsheets/d/e/2PACX-1vR9eB4uyDNgfQ7Jqydfe-nfTM0PJ4PGt85AI7BcIR7k1c708VMcVXnmxK0_0JCHI1ukAQT_B6xp1ntA/pub?gid=0&single=true&output=csv"
).trim();

const SHOWS_SHEET_URL =
  "https://docs.google.com/spreadsheets/d/e/2PACX-1vTdi19qTDyPeBGzq0PpkdlDS_bNg34XpdRiXy8aBa-Jlu-jg2Wzkj1SnLXtRVFU4TGOh5KHJPK8Lwhc/pub?gid=1306635885&single=true&output=csv";

// Stats tab (Fix / Metadata) - gid provided by Chris
// NOTE: Uses the Google Sheet "export?format=csv" URL style.
// This will work as long as the sheet (or at least this tab) is readable without auth.
const STATS_SHEET_URL =
  "https://docs.google.com/spreadsheets/d/e/2PACX-1vTdi19qTDyPeBGzq0PpkdlDS_bNg34XpdRiXy8aBa-Jlu-jg2Wzkj1SnLXtRVFU4TGOh5KHJPK8Lwhc/pub?gid=1973247444&single=true&output=csv";

// Lightweight CSV response cache (keeps Google Sheet fetches from repeating for every visitor)
const SHEET_CACHE_TTL_MS = Math.max(15_000, Number(process.env.SHEET_CACHE_TTL_MS || String(1000 * 60 * 5)));
const sheetResponseCache = new Map();
const RECENT_ACTIVITY_CACHE_TTL_MS = Math.max(60_000, Number(process.env.RECENT_ACTIVITY_CACHE_TTL_MS || String(1000 * 60 * 10)));
let recentMusicActivityCache = null;
let recentMusicActivityPromise = null;
const GEO_SUMMARY_CACHE_TTL_MS = Math.max(60_000, Number(process.env.GEO_SUMMARY_CACHE_TTL_MS || String(1000 * 60 * 60 * 6)));
const GEO_REPORT_CACHE_TTL_MS = Math.max(60_000, Number(process.env.GEO_REPORT_CACHE_TTL_MS || String(1000 * 60 * 15)));
const albumGeoSummaryCache = new Map();
const albumGeoSummaryInFlight = new Map();
let geoReportCache = null;
let geoReportPromise = null;

function isPeoplePayloadEffectivelyEmpty(payload) {
  const albumsScanned = Number(
    (payload && payload.stats && payload.stats.albumsScanned) ||
    (payload && payload.albumsScanned) ||
    0
  );
  return !!(
    payload &&
    albumsScanned === 0 &&
    Array.isArray(payload.people) &&
    payload.people.length === 0
  );
}

function isSheetCacheFresh(entry) {
  return !!(entry && Number.isFinite(Number(entry.fetchedAt)) && (Date.now() - Number(entry.fetchedAt) < SHEET_CACHE_TTL_MS));
}

async function fetchTextWithShortCache(cacheKey, url) {
  const hit = sheetResponseCache.get(cacheKey);
  if (isSheetCacheFresh(hit)) return hit.text;

  const r = await fetch(url, { headers: { Accept: 'text/plain,text/csv;q=0.9,*/*;q=0.8' } });
  if (!r.ok) {
    let body = '';
    try { body = await r.text(); } catch (_) {}
    const snippet = String(body || '').slice(0, 180).replace(/\s+/g, ' ').trim();
    throw new Error(`sheet upstream returned ${r.status}${snippet ? ': ' + snippet : ''}`);
  }

  const text = await r.text();
  sheetResponseCache.set(cacheKey, { text, fetchedAt: Date.now() });
  return text;
}

function setPublicTextCacheHeaders(res, maxAgeSec) {
  const sec = Math.max(0, Number(maxAgeSec) || 0);
  res.set('Cache-Control', `public, max-age=${sec}, s-maxage=${sec}, stale-while-revalidate=60`);
}

function isFreshRecentMusicActivityCache(entry) {
  return !!(entry && Number.isFinite(Number(entry.builtAt)) && (Date.now() - Number(entry.builtAt) < RECENT_ACTIVITY_CACHE_TTL_MS) && entry.payload);
}


// =========================================================
// FIXED: SmugMug API helper (must be ABOVE all routes)
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

function isSmugRateLimitError(err) {
  return /SmugMug upstream returned 429/i.test(String(err && err.message ? err.message : err || ""));
}

async function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function smugWithRetry(endpoint, opts) {
  const o = (opts && typeof opts === "object") ? opts : {};
  const retries = Math.max(0, Number(o.retries || 0));
  const delayMs = Math.max(0, Number(o.delayMs || 0));

  let lastErr;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await smug(endpoint);
    } catch (err) {
      lastErr = err;
      if (attempt >= retries || !isSmugRateLimitError(err)) throw err;
      await delay(delayMs * (attempt + 1));
    }
  }

  throw lastErr;
}

// =========================================================
// NEW: CURATED INDEX (album keywords verified against image metadata)
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
//   {
//     generatedAt,
//     albumsScanned,
//     people: [ { name, photoCount, albums:[{albumKey,title,url}...] } ]
//   }
//
// Notes:
// - Uses disk cache (people-index.json) + memory cache.
// - Render free-tier has no cron; rebuild can be triggered client-side once/day.
// =========================================================

const PEOPLE_INDEX_TTL_MS = Math.max(
  60_000,
  Number(process.env.PEOPLE_INDEX_TTL_MS || String(1000 * 60 * 60 * 24)) // default 24h
);
const PEOPLE_INDEX_BUILD_TIMEOUT_MS = Math.max(
  60_000,
  Number(process.env.PEOPLE_INDEX_BUILD_TIMEOUT_MS || String(1000 * 60 * 10)) // default 10m
);

const PEOPLE_INDEX_FILE = path.join(__dirname, "people-index.json");
const SHOW_INDEX_FILE = path.join(__dirname, "show-index.json");
const BAND_INDEX_FILE = path.join(__dirname, "band-index.json");
let peopleIndexMem = null; // { generatedAt, albumsScanned, people:[...] }
let peopleIndexBuildPromise = null; // prevents concurrent long rebuilds
let peopleIndexBuildStartedAt = 0;

function publicPeopleIndexPayload(payload) {
  // Do not leak private fields to the UI; keep them in cache files for incremental builds.
  if (!payload || typeof payload !== "object") return payload;
  const out = { ...payload };
  try { delete out._albumKeys; } catch (_) {}
  try { delete out._incremental; } catch (_) {}
  try { delete out._albumStateByKey; } catch (_) {}
  return out;
}

function buildUnknownPeopleFromAlbumState(payload) {
  const src = payload && typeof payload === "object" ? payload : {};
  const explicit = src.unknown && typeof src.unknown === "object" ? src.unknown : null;
  if (explicit) {
    const explicitPhotoCount = Number(explicit.photoCount || 0);
    const explicitAlbums = Array.isArray(explicit.albums) ? explicit.albums : [];
    const explicitImages = Array.isArray(explicit.images) ? explicit.images : [];
    if (explicitPhotoCount > 0 || explicitAlbums.length || explicitImages.length) {
      return {
        photoCount: explicitPhotoCount,
        albums: explicitAlbums,
        images: explicitImages
      };
    }
  }

  const albumStateByKey = src._albumStateByKey && typeof src._albumStateByKey === "object"
    ? src._albumStateByKey
    : {};

  const albums = [];
  const images = [];
  let photoCount = 0;
  Object.entries(albumStateByKey).forEach(([albumKey, state]) => {
    const item = state && typeof state === "object" ? state : {};
    const stats = item.stats && typeof item.stats === "object" ? item.stats : {};
    const perAlbumUnknownImages = Array.isArray(item.unknownImages) ? item.unknownImages : [];
    const perAlbumPhotoCount = perAlbumUnknownImages.length || Number(stats.shotsUntagged || 0);
    if (!Number.isFinite(perAlbumPhotoCount) || perAlbumPhotoCount <= 0) return;
    photoCount += perAlbumPhotoCount;
    albums.push({
      albumKey: String(albumKey || "").trim(),
      title: String(item.title || "").trim(),
      url: String(item.url || "").trim(),
      photoCount: perAlbumPhotoCount,
      lastUpdated: String(item.lastUpdated || "").trim()
    });
    perAlbumUnknownImages.forEach((image) => {
      if (image && typeof image === "object") images.push(image);
    });
  });

  return {
    photoCount: images.length || photoCount,
    albums,
    images
  };
}

function buildPeopleIndexResponse(payload, cacheInfo) {
  const safe = publicPeopleIndexPayload(payload) || {};
  const unknown = buildUnknownPeopleFromAlbumState(payload);
  const report = Object.assign(
    {},
    (safe.report && typeof safe.report === "object") ? safe.report : {},
    (safe.rebuild && typeof safe.rebuild === "object") ? { rebuild: safe.rebuild } : {}
  );
  if (cacheInfo && typeof cacheInfo === "object") {
    report.cache = cacheInfo;
  }
  const out = {
    generatedAt: safe.generatedAt || "",
    stats: safe.stats && typeof safe.stats === "object" ? safe.stats : {},
    report,
    people: Array.isArray(safe.people) ? safe.people : [],
    unknown
  };
  return out;
}

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

function withTimeout(promise, timeoutMs, label) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error(`${label} timed out after ${timeoutMs}ms`));
    }, timeoutMs);

    Promise.resolve(promise).then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (err) => {
        clearTimeout(timer);
        reject(err);
      }
    );
  });
}

function resetPeopleIndexBuildState() {
  peopleIndexBuildPromise = null;
  peopleIndexBuildStartedAt = 0;
}

function isPeopleIndexBuildTimedOut() {
  return !!(
    peopleIndexBuildPromise &&
    peopleIndexBuildStartedAt &&
    (Date.now() - peopleIndexBuildStartedAt > PEOPLE_INDEX_BUILD_TIMEOUT_MS)
  );
}

function startPeopleIndexBuild(opts) {
  const o = (opts && typeof opts === "object") ? opts : {};
  const mode = String(o.mode || "incremental").trim().toLowerCase() === "full" ? "full" : "incremental";
  if (peopleIndexBuildPromise && !isPeopleIndexBuildTimedOut()) {
    return peopleIndexBuildPromise;
  }

  if (isPeopleIndexBuildTimedOut()) {
    console.warn("people index rebuild timed out; resetting build state");
    resetPeopleIndexBuildState();
  }

  peopleIndexBuildStartedAt = Date.now();
  peopleIndexBuildPromise = (async () => {
    try {
      const previous = safeReadJsonFile(PEOPLE_INDEX_FILE);
      const computed = await withTimeout(
        computePeopleIndexFromBandsFolder({
          previous,
          incremental: mode !== "full"
        }),
        PEOPLE_INDEX_BUILD_TIMEOUT_MS,
        "people index rebuild"
      );

      if (isPeoplePayloadEffectivelyEmpty(computed)) {
        throw new Error("people index rebuild produced an empty payload; keeping existing cache");
      }

      peopleIndexMem = computed;
      safeWriteJsonFile(PEOPLE_INDEX_FILE, computed);
      return computed;
    } finally {
      resetPeopleIndexBuildState();
    }
  })();

  return peopleIndexBuildPromise;
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

function scoreBandHeaderRow(row) {
  const known = new Set([
    'band',
    'band_id',
    'smug_folder',
    'logo_url',
    'region',
    'location',
    'state',
    'country',
    'members',
    'past_members',
    'tags',
    'status',
    'notes',
    'sets_archive',
    'archived_sets',
    'total_sets'
  ]);

  return (Array.isArray(row) ? row : []).reduce((score, value) => {
    const key = String(value || '').trim().toLowerCase();
    return known.has(key) ? score + 1 : score;
  }, 0);
}

function detectBandHeader(parsed) {
  const base = parsed && typeof parsed === 'object' ? parsed : { header: [], rows: [] };
  const allRows = [base.header].concat(Array.isArray(base.rows) ? base.rows : []);
  let bestIndex = 0;
  let bestScore = scoreBandHeaderRow(allRows[0]);

  for (let i = 1; i < allRows.length; i++) {
    const score = scoreBandHeaderRow(allRows[i]);
    if (score > bestScore) {
      bestIndex = i;
      bestScore = score;
    }
  }

  if (bestScore < 4) return base;
  return {
    header: (allRows[bestIndex] || []).map((h) => String(h || '').trim()),
    rows: allRows.slice(0, bestIndex).concat(allRows.slice(bestIndex + 1)),
    headerRowNumber: bestIndex + 1
  };
}

function _roundPct(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Math.round(n * 100) / 100;
}

function _normalizeStatsHeader(value) {
  return String(value || "")
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "");
}

async function fetchMusicPeopleSheetStats(totalShotsScanned) {
  try {
    const csv = await fetchTextWithShortCache('stats', STATS_SHEET_URL);
    const { header, rows } = parseCsvSimple(csv);
    if (!header.length || !rows.length) {
      return {
        totalMusicShots: 0,
        DAIDone: 0,
        DAIDonePct: 0,
        DAIAppliedPct: 0
      };
    }

    const normalized = header.map(_normalizeStatsHeader);
    const totalIdx = normalized.indexOf('total');
    const daiAppliedIdx = normalized.indexOf('daiappliednotedited');

    const valueRow = rows.find((cols) => Array.isArray(cols) && cols.some((cell) => String(cell || '').trim()));
    if (!valueRow) {
      return {
        totalMusicShots: 0,
        DAIDone: 0,
        DAIDonePct: 0,
        DAIAppliedPct: 0
      };
    }

    const totalMusicShots = Number(totalIdx !== -1 ? String(valueRow[totalIdx] || '').trim() : 0);
    const DAIDone = Number(daiAppliedIdx !== -1 ? String(valueRow[daiAppliedIdx] || '').trim() : 0);
    const totalSafe = Number.isFinite(totalMusicShots) && totalMusicShots > 0 ? totalMusicShots : 0;
    const doneSafe = Number.isFinite(DAIDone) && DAIDone > 0 ? DAIDone : 0;
    const scannedSafe = Number.isFinite(Number(totalShotsScanned)) ? Number(totalShotsScanned) : 0;

    return {
      totalMusicShots: totalSafe,
      DAIDone: doneSafe,
      DAIDonePct: totalSafe > 0 ? _roundPct((doneSafe / totalSafe) * 100) : 0,
      DAIAppliedPct: totalSafe > 0 ? _roundPct((scannedSafe / totalSafe) * 100) : 0
    };
  } catch (err) {
    console.warn('people-index: stats sheet parse failed:', err && err.message ? err.message : err);
    return {
      totalMusicShots: 0,
      DAIDone: 0,
      DAIDonePct: 0,
      DAIAppliedPct: 0
    };
  }
}

function formatPrettyShowDate(raw) {
  const value = String(raw || '').trim();
  if (!value) return '';
  const parts = value.split('/');
  if (parts.length !== 3) return value;

  let [m, d, y] = parts.map((p) => String(p || '').trim());
  m = parseInt(m, 10);
  d = parseInt(d, 10);
  if (String(y).length === 2) y = Number('20' + y);
  else y = parseInt(y, 10);

  const monthNames = [
    '', 'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'
  ];

  function ordinal(n) {
    const s = ['th', 'st', 'nd', 'rd'];
    const v = n % 100;
    return n + (s[(v - 20) % 10] || s[v] || s[0]);
  }

  if (!monthNames[m] || !d || !y) return value;
  return `${monthNames[m]} ${ordinal(d)}, ${y}`;
}

function buildShowIndexPayload(csvText) {
  const { header, rows } = parseCsvSimple(csvText);
  const headerLower = header.map((h) => String(h || '').trim().toLowerCase());

  const nameIdx = headerLower.indexOf('show_name') !== -1
    ? headerLower.indexOf('show_name')
    : headerLower.indexOf('title');
  const urlIdx = headerLower.indexOf('show_url') !== -1
    ? headerLower.indexOf('show_url')
    : headerLower.indexOf('poster_url');
  const dateIdx = headerLower.indexOf('show_date') !== -1
    ? headerLower.indexOf('show_date')
    : headerLower.indexOf('date');
  const venueIdx = headerLower.indexOf('show_venue');
  const cityIdx = headerLower.indexOf('show_city') !== -1
    ? headerLower.indexOf('show_city')
    : headerLower.indexOf('city');
  const stateIdx = headerLower.indexOf('show_state') !== -1
    ? headerLower.indexOf('show_state')
    : headerLower.indexOf('state');

  const shows = rows.map((cols) => {
    const title = nameIdx !== -1 ? String(cols[nameIdx] || '').trim() : '';
    const posterUrl = urlIdx !== -1 ? String(cols[urlIdx] || '').trim() : '';
    const date = dateIdx !== -1 ? String(cols[dateIdx] || '').trim() : '';
    const venue = venueIdx !== -1 ? String(cols[venueIdx] || '').trim() : '';
    const city = cityIdx !== -1 ? String(cols[cityIdx] || '').trim() : '';
    const state = stateIdx !== -1 ? String(cols[stateIdx] || '').trim() : '';

    const row = {
      title,
      show_name: title,
      poster_url: posterUrl,
      show_url: posterUrl,
      date,
      show_date: date,
      pretty_date: formatPrettyShowDate(date),
      venue,
      show_venue: venue,
      city,
      show_city: city,
      state,
      show_state: state
    };

    header.forEach((colName, i) => {
      const key = String(colName || '').trim().toLowerCase();
      if (!key) return;
      const val = String(cols[i] || '').trim();
      if (typeof row[key] === 'undefined') row[key] = val;
    });

    return row;
  });

  return {
    generatedAt: new Date().toISOString(),
    count: shows.length,
    shows
  };
}
function buildBandIndexPayload(csvText, options) {
  const parsed = options && options.detectHeaderRow
    ? detectBandHeader(parseCsvSimple(csvText))
    : parseCsvSimple(csvText);
  const { header, rows } = parsed;
  const headerLower = header.map((h) => String(h || '').trim().toLowerCase());

  const bandIdx = headerLower.indexOf('band');
  const regionIdx = headerLower.indexOf('region');
  const letterIdx = headerLower.indexOf('letter');
  const smugFolderIdx = headerLower.indexOf('smug_folder');
  const logoIdx = headerLower.indexOf('logo_url');
  const totalSetsIdx = headerLower.indexOf('total_sets');
  const setsArchiveIdx = headerLower.indexOf('sets_archive');

  const bands = rows.map((cols) => {
    const name = bandIdx !== -1 ? String(cols[bandIdx] || '').trim() : '';
    const region = regionIdx !== -1 ? String(cols[regionIdx] || '').trim() : '';
    const letter = letterIdx !== -1 ? String(cols[letterIdx] || '').trim() : '';
    const smugFolder = smugFolderIdx !== -1 ? String(cols[smugFolderIdx] || '').trim() : '';
    const logoUrl = logoIdx !== -1 ? String(cols[logoIdx] || '').trim() : '';
    const totalSets = totalSetsIdx !== -1 ? String(cols[totalSetsIdx] || '').trim() : '';
    const setsArchive = setsArchiveIdx !== -1 ? String(cols[setsArchiveIdx] || '').trim() : '';

    const row = {
      name,
      band: name,
      region,
      letter,
      smug_folder: smugFolder,
      logo_url: logoUrl,
      total_sets: totalSets,
      sets_archive: setsArchive
    };

    header.forEach((colName, i) => {
      const key = String(colName || '').trim().toLowerCase();
      if (!key) return;
      row[key] = String(cols[i] || '').trim();
    });

    return row;
  }).filter((row) => String(row.band || '').trim());

  return {
    generatedAt: new Date().toISOString(),
    count: bands.length,
    bands
  };
}

function bandLetterFromBandId(bandId, name) {
  const source = String(bandId || name || '').trim();
  const match = source.match(/[A-Za-z0-9]/);
  return match ? String(match[0]).toUpperCase() : '#';
}

function buildNewSheetBandIndexPayload(csvText) {
  const flat = buildBandIndexPayload(csvText, { detectHeaderRow: true });
  const sourceBands = Array.isArray(flat && flat.bands) ? flat.bands : [];
  const sortedBands = sourceBands.slice().sort((a, b) => {
    const aId = String((a && a.band_id) || '').trim();
    const bId = String((b && b.band_id) || '').trim();
    const aLetter = bandLetterFromBandId(aId, a && (a.band || a.name));
    const bLetter = bandLetterFromBandId(bId, b && (b.band || b.name));
    return aLetter.localeCompare(bLetter, undefined, { numeric: true, sensitivity: 'base' }) ||
      aId.localeCompare(bId, undefined, { numeric: true, sensitivity: 'base' }) ||
      String((a && (a.band || a.name)) || '').localeCompare(String((b && (b.band || b.name)) || ''), undefined, { numeric: true, sensitivity: 'base' });
  });

  const grouped = {};
  sortedBands.forEach((row) => {
    const item = row && typeof row === 'object' ? row : {};
    const name = String((item.band || item.name) || '').trim();
    const bandId = String(item.band_id || '').trim();
    const letter = bandLetterFromBandId(bandId, name);

    if (!grouped[letter]) grouped[letter] = [];
    grouped[letter].push({
      general: {
        band_id: bandId,
        name,
        smug_folder: String(item.smug_folder || '').trim(),
        logo_url: String(item.logo_url || '').trim(),
        status: String(item.status || '').trim(),
        tags: String(item.tags || '').trim(),
        notes: String(item.notes || '').trim()
      },
      personnel: {
        members: String(item.members || '').trim(),
        past_members: String(item.past_members || '').trim()
      },
      stats: {
        region: String(item.region || '').trim(),
        location: String(item.location || '').trim(),
        city: String(item.city || '').trim(),
        state: String(item.state || '').trim(),
        country: String(item.country || '').trim(),
        archived_sets: String((item.archived_sets != null ? item.archived_sets : item.sets_archive) || '').trim(),
        total_sets: String(item.total_sets || '').trim()
      }
    });
  });

  return {
    generatedAt: flat.generatedAt || new Date().toISOString(),
    count: sourceBands.length,
    bands: grouped
  };
}

function extractImageKeyFromUrl(url) {
  const u = String(url || "").trim();
  if (!u) return "";
  // Common SmugMug image URL styles:
  // - /i-<ImageKey>
  // - .../<ImageKey>-X2.jpg
  // - .../1-<ImageKey>/...
  const direct = u.match(/\b\/i-([A-Za-z0-9]+)\b/i);
  if (direct && direct[1]) return String(direct[1] || "").trim();

  const fileStyle = u.match(/\/([A-Za-z0-9]+)-[A-Za-z0-9]+\.(?:jpe?g|png|gif|webp)(?:\?|#|$)/i);
  if (fileStyle && fileStyle[1]) return String(fileStyle[1] || "").trim();

  const segmentStyle = u.match(/\/\d+-([A-Za-z0-9]+)(?:\/|$)/i);
  if (segmentStyle && segmentStyle[1]) return String(segmentStyle[1] || "").trim();

  return "";
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

  // Last resort: walk the full response object and grab the first AlbumKey
  // or any URI that embeds /album/<key>. Smug image payloads vary a lot.
  try {
    const roots = [resp, img];
    const seen = new Set();
    const stack = roots.filter((node) => node && typeof node === "object");
    while (stack.length) {
      const cur = stack.pop();
      if (!cur || typeof cur !== "object") continue;
      if (seen.has(cur)) continue;
      seen.add(cur);

      const directAlbumKey = cur.AlbumKey || cur.albumKey || "";
      if (typeof directAlbumKey === "string" && directAlbumKey.trim()) {
        return directAlbumKey.trim();
      }

      const uriCandidates = [
        cur.Uri,
        cur.URI,
        cur.Url,
        cur.URL,
        cur.AlbumUri,
        cur.AlbumURL,
        cur.AlbumUrl
      ];
      for (const candidate of uriCandidates) {
        const mm = String(candidate || "").match(/\/album\/([^/?#]+)/i);
        if (mm && mm[1]) return String(mm[1] || "").trim();
      }

      for (const value of Object.values(cur)) {
        if (value && typeof value === "object") stack.push(value);
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
  let discoveryErrorCount = 0;

  while (queue.length) {
    const folderPath = queue.shift();
    const folderKey = String(folderPath || "");
    if (!folderKey || visitedFolders.has(folderKey)) continue;
    visitedFolders.add(folderKey);

    const apiFolderPath = folderPathToApiPath(folderPath);
    if (!apiFolderPath) continue;

    // Albums in this folder
    try {
      const page = await smugWithRetry(`/folder/user/${encodeURIComponent(SMUG_NICKNAME)}/${apiFolderPath}!albums?_accept=application/json`, { retries: 4, delayMs: 1500 });
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
      discoveryErrorCount++;
      console.warn("people-index: folder albums list failed:", folderPath, e && e.message ? e.message : e);
    }

    // Subfolders
    try {
      const page = await smugWithRetry(`/folder/user/${encodeURIComponent(SMUG_NICKNAME)}/${apiFolderPath}!folders?_accept=application/json`, { retries: 4, delayMs: 1500 });
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
      discoveryErrorCount++;
      console.warn("people-index: folder subfolders list failed:", folderPath, e && e.message ? e.message : e);
    }
    await delay(250);
  }

  if (albums.size === 0 && discoveryErrorCount > 0) {
    throw new Error(`people index album discovery failed after ${discoveryErrorCount} folder request errors`);
  }

  return Array.from(albums.values());
}

function _safeIsoDateValue(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  const dt = new Date(raw);
  if (Number.isNaN(dt.getTime())) return "";
  return dt.toISOString();
}

function _pickAlbumThumbUrl(album) {
  const candidates = [
    album && album.HighlightImage && album.HighlightImage.ThumbnailUrl,
    album && album.HighlightImage && album.HighlightImage.SmallUrl,
    album && album.HighlightImage && album.HighlightImage.MediumUrl,
    album && album.HighlightImage && album.HighlightImage.LargeUrl,
    album && album.HighlightImageUri,
    album && album.ThumbnailUrl,
    album && album.SmallUrl,
    album && album.MediumUrl
  ];
  for (const value of candidates) {
    const url = String(value || "").trim();
    if (url) return url;
  }
  return "";
}

function _pickAlbumImageUrl(src, keys) {
  const list = Array.isArray(keys) ? keys : [];
  for (const key of list) {
    const url = String(src && src[key] || "").trim();
    if (url) return url;
  }
  return "";
}

function _extractUnknownImageFromAlbumImage(albumImage, albumMeta, caption, reason) {
  const src = (albumImage && albumImage.Image && typeof albumImage.Image === "object") ? albumImage.Image : albumImage;
  const imageKey = String(src && src.ImageKey || albumImage && albumImage.ImageKey || "").trim();
  const thumbUrl = _pickAlbumImageUrl(src, [
    "ThumbnailUrl", "ThumbUrl", "SmallUrl", "MediumUrl", "LargeUrl", "XLargeUrl", "X3LargeUrl", "LargestUrl", "WebUri", "Url", "URL", "Uri"
  ]) || _pickAlbumImageUrl(albumImage, [
    "ThumbnailUrl", "ThumbUrl", "SmallUrl", "MediumUrl", "LargeUrl", "XLargeUrl", "X3LargeUrl", "LargestUrl", "WebUri", "Url", "URL", "Uri"
  ]);
  const imageUrl = _pickAlbumImageUrl(src, [
    "LargestUrl", "X3LargeUrl", "XLargeUrl", "LargeUrl", "MediumUrl", "WebUri", "Url", "URL", "Uri"
  ]) || _pickAlbumImageUrl(albumImage, [
    "LargestUrl", "X3LargeUrl", "XLargeUrl", "LargeUrl", "MediumUrl", "WebUri", "Url", "URL", "Uri"
  ]) || thumbUrl;
  return {
    imageKey,
    caption: String(caption || ""),
    reason: String(reason || "").trim(),
    thumbUrl,
    imageUrl,
    albumKey: String(albumMeta && albumMeta.albumKey || "").trim(),
    albumTitle: String(albumMeta && albumMeta.title || "").trim(),
    albumUrl: String(albumMeta && albumMeta.url || "").trim()
  };
}

function _maxIsoDateValue(values) {
  let best = "";
  let bestMs = -1;
  (Array.isArray(values) ? values : []).forEach((value) => {
    const iso = _safeIsoDateValue(value);
    if (!iso) return;
    const ms = Date.parse(iso);
    if (!Number.isFinite(ms)) return;
    if (ms > bestMs) {
      bestMs = ms;
      best = iso;
    }
  });
  return best;
}

function _extractRecentAlbumActivity(album) {
  const highlight = (album && album.HighlightImage && typeof album.HighlightImage === "object")
    ? album.HighlightImage
    : {};

  const lastUpdated = _maxIsoDateValue([
    album && album.LastUpdated,
    album && album.DateModified,
    album && album.DateTimeModified,
    album && album.ModifiedAt,
    highlight && highlight.LastUpdated,
    highlight && highlight.DateModified,
    highlight && highlight.DateTimeModified,
    highlight && highlight.ModifiedAt,
    highlight && highlight.DateTimeUploaded
  ]);

  const dateUploaded = _maxIsoDateValue([
    album && album.DateUploaded,
    album && album.DateTimeUploaded,
    album && album.Date,
    highlight && highlight.DateUploaded,
    highlight && highlight.DateTimeUploaded,
    highlight && highlight.Date
  ]);

  return { lastUpdated, dateUploaded };
}

function _normalizeRecentLookupText(value) {
  return String(value || "").trim().toLowerCase().replace(/[^a-z0-9]+/g, " ").replace(/\s+/g, " ").trim();
}

function _parseRecentAlbumTitleParts(title) {
  const raw = String(title || "").trim();
  if (!raw) return { showName: "", showDate: "" };
  const m = raw.match(/^(\d{1,2}\/\d{1,2}\/\d{2,4})\s*-\s*(.+)$/);
  if (!m) return { showName: raw, showDate: "" };
  return {
    showDate: String(m[1] || "").trim(),
    showName: String(m[2] || "").trim()
  };
}

function _deriveBandNameFromAlbumUrl(url) {
  const raw = String(url || "").trim();
  if (!raw) return "";
  try {
    const u = new URL(raw);
    const parts = String(u.pathname || "").split("/").filter(Boolean);
    const bandsIdx = parts.findIndex((part) => String(part || "").toLowerCase() === "bands");
    if (bandsIdx !== -1 && parts.length >= bandsIdx + 3) {
      const bandSlug = String(parts[bandsIdx + 2] || "").trim();
      if (bandSlug) return bandSlug.replace(/[-_]+/g, " ").replace(/\b\w/g, (ch) => ch.toUpperCase());
    }
  } catch (_) {}
  return "";
}

function _buildRecentShowLookup(rows) {
  const map = new Map();
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    const showName = String(row && (row.show_name || row.title) || "").trim();
    const showDate = String(row && (row.show_date || row.date) || "").trim();
    const showUrl = String(row && (row.show_url || row.poster_url) || "").trim();
    if (!showName && !showDate) return;
    const key = `${_normalizeRecentLookupText(showDate)}|${_normalizeRecentLookupText(showName)}`;
    if (!map.has(key)) {
      map.set(key, {
        showName,
        showDate,
        prettyDate: String(row && row.pretty_date || formatPrettyShowDate(showDate)).trim(),
        showUrl
      });
    }
  });
  return map;
}

async function _fetchRecentMusicAlbumEntry(seed) {
  const albumKey = String(seed && seed.albumKey || "").trim();
  if (!albumKey) return null;
  try {
    const result = await smug(`/album/${encodeURIComponent(albumKey)}?_accept=application/json&_verbosity=1&_expand=HighlightImage`);
    const album = (result && result.Response && result.Response.Album) || result.Album || result || {};
    const title = String(album.Title || seed.title || "").trim();
    const url = String(album.WebUri || album.Url || seed.url || "").trim();
    const activity = _extractRecentAlbumActivity(album);
    const lastUpdated = activity.lastUpdated;
    const dateUploaded = activity.dateUploaded;
    if (!title || (!lastUpdated && !dateUploaded)) return null;
    return {
      type: "album",
      albumKey,
      title,
      url,
      thumbUrl: _pickAlbumThumbUrl(album),
      lastUpdated,
      dateUploaded
    };
  } catch (err) {
    console.warn("recent-activity: album metadata fetch failed:", albumKey, err && err.message ? err.message : err);
    return null;
  }
}

async function _fetchAlbumFingerprint(seed) {
  const albumKey = String(seed && seed.albumKey || "").trim();
  if (!albumKey) return null;
  try {
    const result = await smug(`/album/${encodeURIComponent(albumKey)}?_accept=application/json&_verbosity=1`);
    const album = (result && result.Response && result.Response.Album) || result.Album || result || {};
    const meta = extractAlbumMeta(result);
    return {
      albumKey,
      title: String(meta.title || seed.title || "").trim(),
      url: String(meta.url || seed.url || "").trim(),
      lastUpdated: _safeIsoDateValue(album.LastUpdated)
    };
  } catch (err) {
    console.warn("people-index: album fingerprint fetch failed:", albumKey, err && err.message ? err.message : err);
    return {
      albumKey,
      title: String(seed && seed.title || "").trim(),
      url: String(seed && seed.url || "").trim(),
      lastUpdated: ""
    };
  }
}

function _roundGeoCoord(value, digits) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  const places = Math.max(0, Number.isFinite(Number(digits)) ? Number(digits) : 5);
  return Number(n.toFixed(places));
}

function _extractGeoFromAlbumImage(item) {
  const src = (item && item.Image && typeof item.Image === "object") ? item.Image : item;
  if (!src || typeof src !== "object") return null;
  const lat = Number(src.Latitude);
  const lng = Number(src.Longitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;
  return {
    lat,
    lng,
    imageKey: String(src.ImageKey || item && item.ImageKey || "").trim()
  };
}

function _buildGeoMapUrl(lat, lng) {
  const latVal = _roundGeoCoord(lat, 6);
  const lngVal = _roundGeoCoord(lng, 6);
  if (!Number.isFinite(latVal) || !Number.isFinite(lngVal)) return "";
  return `https://www.google.com/maps?q=${encodeURIComponent(`${latVal},${lngVal}`)}`;
}

async function _resolveAlbumKeyFromShowUrl(showUrl) {
  const url = String(showUrl || "").trim();
  if (!url) return "";
  const imageKey = extractImageKeyFromUrl(url);
  if (!imageKey) return "";
  try {
    const img = await smug(`/image/${encodeURIComponent(imageKey)}-0?_accept=application/json&_verbosity=1&_expand=Image`);
    return extractAlbumKeyFromImageDetail(img);
  } catch (_) {
    return "";
  }
}

function _isFreshGeoCache(entry, ttlMs) {
  return !!(entry && Number.isFinite(Number(entry.builtAt)) && (Date.now() - Number(entry.builtAt) < ttlMs) && entry.payload);
}

async function buildAlbumGeoSummary(albumKey, opts) {
  const key = String(albumKey || "").trim();
  if (!key) return null;
  const o = (opts && typeof opts === "object") ? opts : {};
  const requestedMaxImages = Number(o.maxImages);
  const maxImages = Number.isFinite(requestedMaxImages) && requestedMaxImages > 0
    ? Math.max(12, Math.floor(requestedMaxImages))
    : Number.MAX_SAFE_INTEGER;
  let start = 1;
  const count = 200;
  let totalImagesScanned = 0;
  let geoTaggedImages = 0;
  const coords = [];
  let sampleImageKey = "";

  while (totalImagesScanned < maxImages) {
    const page = await smug(`/album/${encodeURIComponent(key)}!images?count=${count}&start=${start}&_accept=application/json&_expand=Image&_verbosity=1`);
    const resp = page && page.Response ? page.Response : page;
    const items = Array.isArray(resp && resp.AlbumImage) ? resp.AlbumImage : [];
    if (!items.length) break;

    const detailKeys = [];
    for (const item of items) {
      if (totalImagesScanned >= maxImages) break;
      totalImagesScanned += 1;
      const geo = _extractGeoFromAlbumImage(item);
      if (geo) {
        geoTaggedImages += 1;
        coords.push(geo);
        if (!sampleImageKey && geo.imageKey) sampleImageKey = geo.imageKey;
        continue;
      }
      const imageKey = String(item && item.Image && item.Image.ImageKey || item && item.ImageKey || "").trim();
      if (imageKey) detailKeys.push(imageKey);
    }

    await mapLimit(detailKeys, 4, async (imageKey) => {
      try {
        const detail = await smug(`/image/${encodeURIComponent(imageKey)}-0?_accept=application/json&_verbosity=1&_expand=Image`);
        const resp2 = detail && detail.Response ? detail.Response : detail;
        const geo = _extractGeoFromAlbumImage(resp2 && (resp2.Image || resp2));
        if (!geo) return;
        geoTaggedImages += 1;
        coords.push(geo);
        if (!sampleImageKey && geo.imageKey) sampleImageKey = geo.imageKey;
      } catch (_) {}
    });

    if (items.length < count) break;
    start += count;
  }

  let center = null;
  let bounds = null;
  if (coords.length) {
    const latSum = coords.reduce((sum, item) => sum + Number(item.lat || 0), 0);
    const lngSum = coords.reduce((sum, item) => sum + Number(item.lng || 0), 0);
    const lats = coords.map((item) => Number(item.lat)).filter(Number.isFinite);
    const lngs = coords.map((item) => Number(item.lng)).filter(Number.isFinite);
    center = {
      lat: _roundGeoCoord(latSum / coords.length, 6),
      lng: _roundGeoCoord(lngSum / coords.length, 6)
    };
    bounds = {
      minLat: _roundGeoCoord(Math.min(...lats), 6),
      maxLat: _roundGeoCoord(Math.max(...lats), 6),
      minLng: _roundGeoCoord(Math.min(...lngs), 6),
      maxLng: _roundGeoCoord(Math.max(...lngs), 6)
    };
  }

  return {
    albumKey: key,
    builtAt: new Date().toISOString(),
    totalImagesScanned,
    geoTaggedImages,
    coveragePct: totalImagesScanned > 0 ? _roundPct((geoTaggedImages / totalImagesScanned) * 100) : 0,
    center,
    bounds,
    sampleImageKey,
    mapUrl: center ? _buildGeoMapUrl(center.lat, center.lng) : ""
  };
}

function _parseGeoShowDateValue(raw) {
  const value = String(raw || '').trim();
  if (!value) return 0;
  const iso = value.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (iso) {
    const ts = Date.UTC(Number(iso[1]), Number(iso[2]) - 1, Number(iso[3]));
    return Number.isFinite(ts) ? ts : 0;
  }
  const us = value.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/);
  if (us) {
    const yyyy = us[3].length === 2 ? 2000 + Number(us[3]) : Number(us[3]);
    const ts = Date.UTC(yyyy, Number(us[1]) - 1, Number(us[2]));
    return Number.isFinite(ts) ? ts : 0;
  }
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

async function getAlbumGeoSummaryCached(albumKey, opts) {
  const key = String(albumKey || "").trim();
  if (!key) return null;
  const force = !!(opts && opts.force);
  const cacheHit = albumGeoSummaryCache.get(key);
  if (!force && _isFreshGeoCache(cacheHit, GEO_SUMMARY_CACHE_TTL_MS)) {
    return cacheHit.payload;
  }
  if (!force && albumGeoSummaryInFlight.has(key)) {
    return albumGeoSummaryInFlight.get(key);
  }
  const work = buildAlbumGeoSummary(key, opts).then((payload) => {
    if (payload) albumGeoSummaryCache.set(key, { builtAt: Date.now(), payload });
    return payload;
  }).finally(() => {
    albumGeoSummaryInFlight.delete(key);
  });
  albumGeoSummaryInFlight.set(key, work);
  return work;
}

function _combineGeoSummaries(albumKeys, summaries) {
  const keys = Array.isArray(albumKeys) ? albumKeys.filter(Boolean) : [];
  const list = (Array.isArray(summaries) ? summaries : []).filter(Boolean);
  if (!list.length) {
    return {
      albumKeys: keys,
      totalImagesScanned: 0,
      geoTaggedImages: 0,
      coveragePct: 0,
      center: null,
      bounds: null,
      mapUrl: ""
    };
  }

  let totalImagesScanned = 0;
  let geoTaggedImages = 0;
  let latWeighted = 0;
  let lngWeighted = 0;
  let weightedCount = 0;
  let bounds = null;
  let sampleImageKey = "";

  list.forEach((item) => {
    const scanned = Number(item && item.totalImagesScanned || 0);
    const tagged = Number(item && item.geoTaggedImages || 0);
    const center = item && item.center ? item.center : null;
    const itemBounds = item && item.bounds ? item.bounds : null;

    totalImagesScanned += scanned;
    geoTaggedImages += tagged;

    if (center && Number.isFinite(Number(center.lat)) && Number.isFinite(Number(center.lng)) && tagged > 0) {
      latWeighted += Number(center.lat) * tagged;
      lngWeighted += Number(center.lng) * tagged;
      weightedCount += tagged;
    }

    if (itemBounds) {
      if (!bounds) {
        bounds = {
          minLat: Number(itemBounds.minLat),
          maxLat: Number(itemBounds.maxLat),
          minLng: Number(itemBounds.minLng),
          maxLng: Number(itemBounds.maxLng)
        };
      } else {
        bounds.minLat = Math.min(bounds.minLat, Number(itemBounds.minLat));
        bounds.maxLat = Math.max(bounds.maxLat, Number(itemBounds.maxLat));
        bounds.minLng = Math.min(bounds.minLng, Number(itemBounds.minLng));
        bounds.maxLng = Math.max(bounds.maxLng, Number(itemBounds.maxLng));
      }
    }

    if (!sampleImageKey && item && item.sampleImageKey) sampleImageKey = item.sampleImageKey;
  });

  const center = weightedCount > 0
    ? {
        lat: _roundGeoCoord(latWeighted / weightedCount, 6),
        lng: _roundGeoCoord(lngWeighted / weightedCount, 6)
      }
    : null;

  if (bounds) {
    bounds = {
      minLat: _roundGeoCoord(bounds.minLat, 6),
      maxLat: _roundGeoCoord(bounds.maxLat, 6),
      minLng: _roundGeoCoord(bounds.minLng, 6),
      maxLng: _roundGeoCoord(bounds.maxLng, 6)
    };
  }

  return {
    albumKeys: keys,
    albumKey: keys[0] || "",
    sampleImageKey,
    totalImagesScanned,
    geoTaggedImages,
    coveragePct: totalImagesScanned > 0 ? _roundPct((geoTaggedImages / totalImagesScanned) * 100) : 0,
    center,
    bounds,
    mapUrl: center ? _buildGeoMapUrl(center.lat, center.lng) : ""
  };
}

function _buildGeoVenueLine(row) {
  if (!row || typeof row !== "object") return "";
  const venue = String(row.show_venue || row.venue || "").trim();
  const city = String(row.show_city || row.city || "").trim();
  const state = String(row.show_state || row.state || "").trim();
  const place = (city && state) ? `${city}, ${state}` : (city || state);
  return [venue, place].filter(Boolean).join(" - ");
}

async function buildMusicGeoReportPayload(forceFresh) {
  if (!forceFresh && _isFreshGeoCache(geoReportCache, GEO_REPORT_CACHE_TTL_MS)) {
    return geoReportCache.payload;
  }
  if (!forceFresh && geoReportPromise) return geoReportPromise;

  geoReportPromise = (async () => {
    const showsCsv = await fetchTextWithShortCache('shows', SHOWS_SHEET_URL);
    const rows = buildShowIndexPayload(showsCsv).shows || [];
    const discovered = await listAlbumsAndFoldersRecursive(PEOPLE_INDEX_BANDS_ROOT);
    const seeds = Array.isArray(discovered) ? discovered.filter((item) => item && item.albumKey) : [];
    const albumEntries = [];
    let albumCursor = 0;

    async function albumWorker() {
      while (albumCursor < seeds.length) {
        const idx = albumCursor++;
        const seed = seeds[idx];
        const item = await _fetchRecentMusicAlbumEntry(seed);
        if (item && item.albumKey) albumEntries.push(item);
      }
    }

    await Promise.all(Array.from({ length: 6 }, () => albumWorker()));

    const albumLookup = new Map();
    const albumLookupByDate = new Map();
    albumEntries.forEach((item) => {
      const parsed = _parseRecentAlbumTitleParts(item.title);
      const dateKey = _normalizeRecentLookupText(parsed.showDate);
      const showKey = `${dateKey}|${_normalizeRecentLookupText(parsed.showName)}`;
      if (dateKey) {
        if (!albumLookupByDate.has(dateKey)) albumLookupByDate.set(dateKey, []);
        albumLookupByDate.get(dateKey).push(item);
      }
      if (dateKey && parsed.showName) {
        if (!albumLookup.has(showKey)) albumLookup.set(showKey, []);
        albumLookup.get(showKey).push(item);
      }
    });
    const items = [];
    let cursor = 0;

    async function worker() {
      while (cursor < rows.length) {
        const idx = cursor++;
        const row = rows[idx];
        const showUrl = String(row && row.show_url || "").trim();
        const showName = String(row && (row.show_name || row.title) || "").trim();
        const showDate = String(row && (row.show_date || row.date) || "").trim();
        const normalizedDate = _normalizeRecentLookupText(showDate);
        const normalizedKey = `${normalizedDate}|${_normalizeRecentLookupText(showName)}`;
        const matchedAlbums = albumLookup.get(normalizedKey) || albumLookupByDate.get(normalizedDate) || [];
        const albumKeys = Array.from(new Set((Array.isArray(matchedAlbums) ? matchedAlbums : []).map((item) => String(item && item.albumKey || '').trim()).filter(Boolean)));
        const geoSummaries = await Promise.all(albumKeys.map((albumKey) => getAlbumGeoSummaryCached(albumKey, { force: forceFresh }).catch(() => null)));
        const geo = _combineGeoSummaries(albumKeys, geoSummaries);
        items.push({
          showName,
          showDate,
          prettyDate: String(row && row.pretty_date || formatPrettyShowDate(row && (row.show_date || row.date) || "")).trim(),
          venueLine: _buildGeoVenueLine(row),
          posterUrl: showUrl,
          albumKey: String(geo && geo.albumKey || '').trim(),
          albumKeys,
          hasGeo: !!(geo && geo.center),
          geoTaggedImages: Number(geo && geo.geoTaggedImages || 0),
          totalImagesScanned: Number(geo && geo.totalImagesScanned || 0),
          coveragePct: Number(geo && geo.coveragePct || 0),
          center: geo && geo.center ? geo.center : null,
          bounds: geo && geo.bounds ? geo.bounds : null,
          mapUrl: String(geo && geo.mapUrl || "").trim()
        });
      }
    }

    await Promise.all(Array.from({ length: 4 }, () => worker()));
    items.sort((a, b) =>
      _parseGeoShowDateValue(b.showDate || b.prettyDate || '') -
      _parseGeoShowDateValue(a.showDate || a.prettyDate || '')
    );

    const payload = {
      generatedAt: new Date().toISOString(),
      count: items.length,
      geoTaggedCount: items.filter((item) => item.hasGeo).length,
      items
    };
    geoReportCache = { builtAt: Date.now(), payload };
    return payload;
  })().finally(() => {
    geoReportPromise = null;
  });

  return geoReportPromise;
}

async function buildRecentMusicActivityPayload(forceFresh) {
  if (!forceFresh && isFreshRecentMusicActivityCache(recentMusicActivityCache)) {
    return recentMusicActivityCache.payload;
  }
  if (recentMusicActivityPromise) return recentMusicActivityPromise;

  recentMusicActivityPromise = (async () => {
    const discovered = await listAlbumsAndFoldersRecursive(PEOPLE_INDEX_BANDS_ROOT);
    const showsCsv = await fetchTextWithShortCache('shows', SHOWS_SHEET_URL);
    const showRows = buildShowIndexPayload(showsCsv).shows || [];
    const showLookup = _buildRecentShowLookup(showRows);
    const seeds = Array.isArray(discovered) ? discovered.filter((item) => item && item.albumKey) : [];
    const concurrency = 8;
    const entries = [];
    let cursor = 0;

    async function worker() {
      while (cursor < seeds.length) {
        const idx = cursor++;
        const seed = seeds[idx];
        const item = await _fetchRecentMusicAlbumEntry(seed);
        if (item) entries.push(item);
      }
    }

    const workers = [];
    for (let i = 0; i < concurrency; i++) workers.push(worker());
    await Promise.all(workers);

    const hydrated = entries.map((item) => {
      const parsed = _parseRecentAlbumTitleParts(item.title);
      const key = `${_normalizeRecentLookupText(parsed.showDate)}|${_normalizeRecentLookupText(parsed.showName)}`;
      const showInfo = showLookup.get(key) || null;
      return Object.assign({}, item, {
        showName: String((showInfo && showInfo.showName) || parsed.showName || item.title || '').trim(),
        showDate: String((showInfo && showInfo.showDate) || parsed.showDate || '').trim(),
        prettyDate: String((showInfo && showInfo.prettyDate) || formatPrettyShowDate(parsed.showDate || '')).trim(),
        showUrl: String((showInfo && showInfo.showUrl) || '').trim(),
        band: _deriveBandNameFromAlbumUrl(item.url || '')
      });
    });

    const byLastUpdated = hydrated
      .filter((item) => item.lastUpdated)
      .sort((a, b) => String(b.lastUpdated).localeCompare(String(a.lastUpdated)))
      .slice(0, 6);

    const byDateUploaded = hydrated
      .filter((item) => item.dateUploaded)
      .sort((a, b) => String(b.dateUploaded).localeCompare(String(a.dateUploaded)))
      .slice(0, 6);

    const payload = {
      generatedAt: new Date().toISOString(),
      latestUpdated: byLastUpdated,
      latestAdded: byDateUploaded
    };
    recentMusicActivityCache = { builtAt: Date.now(), payload };
    return payload;
  })().finally(() => {
    recentMusicActivityPromise = null;
  });

  return recentMusicActivityPromise;
}

function isLikelyCompositePersonName(name) {
  const raw = String(name || "").trim().replace(/\s+/g, " ");
  if (!raw) return true;
  if (/[;\uFF1B\u037E]/.test(raw)) return true;

  const commaParts = raw.split(/\s*,\s*/g).filter(Boolean);
  if (commaParts.length >= 2 && commaParts.every((part) => /\s/.test(part))) {
    return true;
  }

  return false;
}

function parsePeopleFromCaption(caption) {
  const raw = String(caption || "").trim();
  if (!raw) return [];

  // Normalize alternate semicolon characters so caption parsing is consistent.
  const normalized = raw.replace(/[\uFF1B\u037E]/g, ";");

  // Semicolon-delimited list, dedupe case-insensitively, preserve first-seen casing.
  const parts = normalized
    .split(/\s*;\s*/g)
    .map((s) => String(s || "").trim().replace(/\s+/g, " "))
    .filter(Boolean);
  const seen = new Set();
  const out = [];
  for (const p of parts) {
    if (isLikelyCompositePersonName(p)) continue;
    const k = String(p || "").toLowerCase();
    if (!k || seen.has(k)) continue;
    seen.add(k);
    out.push(p);
  }
  return out;
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
  const peopleToPhotoCount = new Map(); // name -> number of photos they appear in
  const shotStats = {
    totalShotsScanned: 0,
    shotsTagged: 0,
    shotsUntagged: 0
  };

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
        const pageCaption =
          (ai && Object.prototype.hasOwnProperty.call(ai, "Caption")) ? ai.Caption :
          (ai && ai.Image && Object.prototype.hasOwnProperty.call(ai.Image, "Caption")) ? ai.Image.Caption :
          (ai && ai.Image && Object.prototype.hasOwnProperty.call(ai.Image, "CaptionText")) ? ai.Image.CaptionText :
          undefined;
        const cap = typeof pageCaption === "string" ? pageCaption : "";

        if (pageCaption !== undefined) {
          shotStats.totalShotsScanned += 1;
          if (cap.trim()) shotStats.shotsTagged += 1;
          else shotStats.shotsUntagged += 1;
        }

        const names = parsePeopleFromCaption(cap);
        if (names.length) {
          for (const n of names) {
            const key = String(n).trim();
            if (!key) continue;
            if (!peopleToAlbums.has(key)) peopleToAlbums.set(key, new Map());
            peopleToAlbums.get(key).set(albumKey, { albumKey, title: meta.title, url: meta.url });
            // Photo count: increment once per photo per person (names are already deduped within caption)
            peopleToPhotoCount.set(key, (peopleToPhotoCount.get(key) || 0) + 1);
          }
          continue;
        }

        const ik = (ai && ai.Image && ai.Image.ImageKey) || ai.ImageKey || "";
        if (pageCaption === undefined && ik) imageKeysNeedingDetail.push(String(ik));
        else if (pageCaption === undefined) {
          shotStats.totalShotsScanned += 1;
          shotStats.shotsUntagged += 1;
        }
      }

      // If captions weren't included, fetch per-image detail for the remaining.
      await mapLimit(imageKeysNeedingDetail, 4, async (imageKey) => {
        try {
          const detail = await smug(`/image/${encodeURIComponent(imageKey)}-0?_accept=application/json&_verbosity=1&_expand=Image`);
          const resp2 = detail && detail.Response ? detail.Response : detail;
          const img = resp2 && (resp2.Image || resp2.image || resp2);
          const cap = img && (img.Caption || img.CaptionText || "");
          shotStats.totalShotsScanned += 1;
          if (String(cap || "").trim()) shotStats.shotsTagged += 1;
          else shotStats.shotsUntagged += 1;
          const names = parsePeopleFromCaption(cap);
          if (!names.length) return;
          for (const n of names) {
            const key = String(n).trim();
            if (!key) continue;
            if (!peopleToAlbums.has(key)) peopleToAlbums.set(key, new Map());
            peopleToAlbums.get(key).set(albumKey, { albumKey, title: meta.title, url: meta.url });
            peopleToPhotoCount.set(key, (peopleToPhotoCount.get(key) || 0) + 1);
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
      return { name, photoCount: Number(peopleToPhotoCount.get(name) || 0), albums };
    })
    .filter((person) => !isLikelyCompositePersonName(person && person.name))
    .sort((a, b) => String(a.name).localeCompare(String(b.name)));

  const sheetStats = await fetchMusicPeopleSheetStats(shotStats.totalShotsScanned);
  const totalMusicShots = Number(sheetStats.totalMusicShots || 0);
  const shotsLeftToDo = Number(sheetStats.DAIDone || 0);
  const totalShotsScanned = Number(shotStats.totalShotsScanned || 0);
  const shotsTagged = Number(shotStats.shotsTagged || 0);
  const shotsUntagged = Number(shotStats.shotsUntagged || 0);
  const stats = {
    totalMusicShots,
    shotsLeftToDo,
    daiLeftPct: totalMusicShots > 0 ? _roundPct((shotsLeftToDo / totalMusicShots) * 100) : 0,
    totalShotsScanned,
    shotsPct: totalMusicShots > 0 ? _roundPct((totalShotsScanned / totalMusicShots) * 100) : 0,
    shotsTagged,
    taggedPct: totalShotsScanned > 0 ? _roundPct((shotsTagged / totalShotsScanned) * 100) : 0,
    shotsUntagged,
    untaggedPct: totalShotsScanned > 0 ? _roundPct((shotsUntagged / totalShotsScanned) * 100) : 0,
    albumsScanned: albumKeys.length
  };

  return {
    generatedAt: new Date().toISOString(),
    stats,
    people
  };
}

async function computePeopleIndexFromBandsFolder(opts) {
  const o = (opts && typeof opts === "object") ? opts : {};
  const previous = o.previous && typeof o.previous === "object" ? o.previous : null;
  const incremental = !!o.incremental;

  const discovered = await listAlbumsAndFoldersRecursive(PEOPLE_INDEX_BANDS_ROOT);
  let albumKeysAll = discovered.map((a) => a.albumKey).filter(Boolean);

  // Safety cap to avoid upstream timeouts. If PEOPLE_INDEX_MAX_ALBUMS is 0, no cap.
  if (PEOPLE_INDEX_MAX_ALBUMS > 0 && albumKeysAll.length > PEOPLE_INDEX_MAX_ALBUMS) {
    albumKeysAll = albumKeysAll.slice(0, PEOPLE_INDEX_MAX_ALBUMS);
  }

  const albumKeySetAll = new Set(albumKeysAll);
  const discoveredByKey = new Map();
  discovered.forEach((item) => {
    const key = String(item && item.albumKey || "").trim();
    if (key && albumKeySetAll.has(key)) discoveredByKey.set(key, item);
  });

  const previousStatesRaw = (incremental && previous && previous._albumStateByKey && typeof previous._albumStateByKey === "object")
    ? previous._albumStateByKey
    : {};
  const previousStates = new Map();
  Object.keys(previousStatesRaw).forEach((key) => {
    const clean = String(key || "").trim();
    const value = previousStatesRaw[key];
    if (clean && value && typeof value === "object") previousStates.set(clean, value);
  });

  const removedAlbums = incremental
    ? Array.from(previousStates.keys()).filter((key) => !albumKeySetAll.has(key))
    : [];

  const discoveredSeeds = albumKeysAll
    .map((key) => discoveredByKey.get(key))
    .filter(Boolean);

  const fingerprints = [];
  let cursor = 0;
  async function fingerprintWorker() {
    while (cursor < discoveredSeeds.length) {
      const idx = cursor++;
      const seed = discoveredSeeds[idx];
      const fp = await _fetchAlbumFingerprint(seed);
      if (fp && fp.albumKey) fingerprints.push(fp);
    }
  }
  await Promise.all(Array.from({ length: 6 }, () => fingerprintWorker()));

  const fingerprintByKey = new Map();
  fingerprints.forEach((item) => {
    if (item && item.albumKey && albumKeySetAll.has(item.albumKey)) fingerprintByKey.set(item.albumKey, item);
  });

  const albumKeysToScan = [];
  const reusableStates = new Map();
  for (const albumKey of albumKeysAll) {
    const fingerprint = fingerprintByKey.get(albumKey) || {
      albumKey,
      title: String((discoveredByKey.get(albumKey) && discoveredByKey.get(albumKey).title) || "").trim(),
      url: String((discoveredByKey.get(albumKey) && discoveredByKey.get(albumKey).url) || "").trim(),
      lastUpdated: ""
    };
    const prevState = previousStates.get(albumKey);
    const currentLastUpdated = String(fingerprint.lastUpdated || "").trim();
    const previousLastUpdated = String(prevState && prevState.lastUpdated || "").trim();
    if (
      incremental &&
      prevState &&
      currentLastUpdated &&
      previousLastUpdated &&
      currentLastUpdated === previousLastUpdated
    ) {
      reusableStates.set(albumKey, Object.assign({}, prevState, {
        albumKey,
        title: String(fingerprint.title || prevState.title || "").trim(),
        url: String(fingerprint.url || prevState.url || "").trim(),
        lastUpdated: currentLastUpdated
      }));
    } else {
      albumKeysToScan.push(albumKey);
    }
  }

  async function scanAlbum(albumKey) {
    const fingerprint = fingerprintByKey.get(albumKey) || {
      albumKey,
      title: String((discoveredByKey.get(albumKey) && discoveredByKey.get(albumKey).title) || "").trim(),
      url: String((discoveredByKey.get(albumKey) && discoveredByKey.get(albumKey).url) || "").trim(),
      lastUpdated: ""
    };
    const peopleCounts = new Map();
    const unknownImages = [];
    let totalShotsScanned = 0;
    let shotsTagged = 0;
    let shotsUntagged = 0;
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
        const pageCaption =
          (ai && Object.prototype.hasOwnProperty.call(ai, "Caption")) ? ai.Caption :
          (ai && ai.Image && Object.prototype.hasOwnProperty.call(ai.Image, "Caption")) ? ai.Image.Caption :
          (ai && ai.Image && Object.prototype.hasOwnProperty.call(ai.Image, "CaptionText")) ? ai.Image.CaptionText :
          undefined;
        const cap = typeof pageCaption === "string" ? pageCaption : "";

        if (pageCaption !== undefined) {
          totalShotsScanned += 1;
          if (cap.trim()) shotsTagged += 1;
          else shotsUntagged += 1;
        }

        const names = parsePeopleFromCaption(cap);
        if (names.length) {
          for (const n of names) {
            const key = String(n).trim();
            if (!key) continue;
            peopleCounts.set(key, Number(peopleCounts.get(key) || 0) + 1);
          }
          continue;
        }

        if (pageCaption !== undefined) {
          unknownImages.push(_extractUnknownImageFromAlbumImage(ai, fingerprint, cap, cap.trim() ? "unmatched_caption" : "missing_caption"));
        }

        const ik = (ai && ai.Image && ai.Image.ImageKey) || ai.ImageKey || "";
        if (pageCaption === undefined && ik) imageKeysNeedingDetail.push(String(ik));
        else if (pageCaption === undefined) {
          totalShotsScanned += 1;
          shotsUntagged += 1;
          unknownImages.push(_extractUnknownImageFromAlbumImage(ai, fingerprint, "", "missing_caption"));
        }
      }

      await mapLimit(imageKeysNeedingDetail, 4, async (imageKey) => {
        try {
          const detail = await smug(`/image/${encodeURIComponent(imageKey)}-0?_accept=application/json&_verbosity=1&_expand=Image`);
          const resp2 = detail && detail.Response ? detail.Response : detail;
          const img = resp2 && (resp2.Image || resp2.image || resp2);
          const cap = img && (img.Caption || img.CaptionText || "");
          totalShotsScanned += 1;
          if (String(cap || "").trim()) shotsTagged += 1;
          else shotsUntagged += 1;
          const names = parsePeopleFromCaption(cap);
          if (!names.length) {
            unknownImages.push(_extractUnknownImageFromAlbumImage(img, fingerprint, cap, String(cap || "").trim() ? "unmatched_caption" : "missing_caption"));
            return;
          }
          for (const n of names) {
            const key = String(n).trim();
            if (!key) continue;
            peopleCounts.set(key, Number(peopleCounts.get(key) || 0) + 1);
          }
        } catch (_) {
          // continue
        }
      });

      if (items.length < count) break;
      start += count;
    }
    return {
      albumKey,
      title: String(fingerprint.title || "").trim(),
      url: String(fingerprint.url || "").trim(),
      lastUpdated: String(fingerprint.lastUpdated || "").trim(),
      stats: {
        totalShotsScanned,
        shotsTagged,
        shotsUntagged
      },
      unknownImages,
      people: Object.fromEntries(Array.from(peopleCounts.entries()))
    };
  }

  const scannedStates = new Map();
  await mapLimit(albumKeysToScan, 2, async (albumKey) => {
    try {
      const state = await scanAlbum(albumKey);
      if (state && state.albumKey) scannedStates.set(albumKey, state);
    } catch (e) {
      console.warn("people-index: album scan failed:", albumKey, e && e.message ? e.message : e);
    }
  });

  const finalAlbumStates = new Map();
  for (const albumKey of albumKeysAll) {
    if (scannedStates.has(albumKey)) finalAlbumStates.set(albumKey, scannedStates.get(albumKey));
    else if (reusableStates.has(albumKey)) finalAlbumStates.set(albumKey, reusableStates.get(albumKey));
  }

  const peopleToAlbums = new Map();
  const peopleToPhotoCount = new Map();
  const shotStats = { totalShotsScanned: 0, shotsTagged: 0, shotsUntagged: 0 };
  for (const [albumKey, state] of finalAlbumStates.entries()) {
    const title = String(state && state.title || "").trim();
    const url = String(state && state.url || "").trim();
    const statsBlock = (state && state.stats && typeof state.stats === "object") ? state.stats : {};
    shotStats.totalShotsScanned += Number(statsBlock.totalShotsScanned || 0);
    shotStats.shotsTagged += Number(statsBlock.shotsTagged || 0);
    shotStats.shotsUntagged += Number(statsBlock.shotsUntagged || 0);

    const peopleBlock = (state && state.people && typeof state.people === "object") ? state.people : {};
    Object.keys(peopleBlock).forEach((name) => {
      const cleanName = String(name || "").trim();
      if (!cleanName) return;
      const count = Number(peopleBlock[name] || 0);
      if (!Number.isFinite(count) || count <= 0) return;
      if (!peopleToAlbums.has(cleanName)) peopleToAlbums.set(cleanName, new Map());
      peopleToAlbums.get(cleanName).set(albumKey, { albumKey, title, url });
      peopleToPhotoCount.set(cleanName, Number(peopleToPhotoCount.get(cleanName) || 0) + count);
    });
  }

  const people = Array.from(peopleToAlbums.entries())
    .map(([name, albumMap]) => ({
      name,
      photoCount: Number(peopleToPhotoCount.get(name) || 0),
      albums: Array.from(albumMap.values())
    }))
    .sort((a, b) => String(a.name).localeCompare(String(b.name)));

  // Store album keys privately for incremental diffs next time.
  // (Front-end safely ignores unknown fields.)
  const sheetStats = await fetchMusicPeopleSheetStats(shotStats.totalShotsScanned);
  const totalMusicShots = Number(sheetStats.totalMusicShots || 0);
  const shotsLeftToDo = Number(sheetStats.DAIDone || 0);
  const totalShotsScanned = Number(shotStats.totalShotsScanned || 0);
  const shotsTagged = Number(shotStats.shotsTagged || 0);
  const shotsUntagged = Number(shotStats.shotsUntagged || 0);
  const stats = {
    totalMusicShots,
    shotsLeftToDo,
    daiLeftPct: totalMusicShots > 0 ? _roundPct((shotsLeftToDo / totalMusicShots) * 100) : 0,
    totalShotsScanned,
    shotsPct: totalMusicShots > 0 ? _roundPct((totalShotsScanned / totalMusicShots) * 100) : 0,
    shotsTagged,
    taggedPct: totalShotsScanned > 0 ? _roundPct((shotsTagged / totalShotsScanned) * 100) : 0,
    shotsUntagged,
    untaggedPct: totalShotsScanned > 0 ? _roundPct((shotsUntagged / totalShotsScanned) * 100) : 0,
    albumsScanned: albumKeysAll.length
  };

  const buildSummary = {
    mode: incremental ? "incremental" : "full",
    albumsDiscovered: albumKeysAll.length,
    albumsReused: reusableStates.size,
    albumsRescanned: scannedStates.size,
    albumsRemoved: removedAlbums.length
  };

  const albumStateByKey = {};
  finalAlbumStates.forEach((state, key) => {
    albumStateByKey[key] = state;
  });

  const unknownAlbums = [];
  const unknownImages = [];
  finalAlbumStates.forEach((state, key) => {
    const item = state && typeof state === "object" ? state : {};
    const statsBlock = item.stats && typeof item.stats === "object" ? item.stats : {};
    const perAlbumUnknownImages = Array.isArray(item.unknownImages) ? item.unknownImages : [];
    const shots = perAlbumUnknownImages.length || Number(statsBlock.shotsUntagged || 0);
    if (!Number.isFinite(shots) || shots <= 0) return;
    unknownAlbums.push({
      albumKey: String(key || "").trim(),
      title: String(item.title || "").trim(),
      url: String(item.url || "").trim(),
      photoCount: shots,
      lastUpdated: String(item.lastUpdated || "").trim()
    });
    perAlbumUnknownImages.forEach((image) => {
      if (image && typeof image === "object") unknownImages.push(image);
    });
  });

  return {
    generatedAt: new Date().toISOString(),
    stats,
    report: {
      rebuild: buildSummary
    },
    people,
    unknown: {
      photoCount: unknownImages.length || shotsUntagged,
      albums: unknownAlbums,
      images: unknownImages
    },
    _albumKeys: albumKeysAll,
    _incremental: buildSummary,
    _albumStateByKey: albumStateByKey
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
// LIGHTWEIGHT WAKE ROUTES
// =========================================================
app.get('/health', (req, res) => {
  allowCors(res, req);
  setPublicTextCacheHeaders(res, 15);
  return res.json({ ok: true, service: 'music-archive', ts: new Date().toISOString() });
});

app.get('/ping', (req, res) => {
  allowCors(res, req);
  setPublicTextCacheHeaders(res, 15);
  return res.type('text/plain').send('ok');
});

// =========================================================
// PEOPLE INDEX (server-cached)
// =========================================================
app.get('/index/people', async (req, res) => {
  const force = String(req.query.force || '') === '1';
  const full = String(req.query.full || req.query.mode || '').trim().toLowerCase();
  const buildMode = full === '1' || full === 'true' || full === 'full' ? 'full' : 'incremental';
  allowCors(res, req);

  try {
    // 1) Memory cache
    if (!force && peopleIndexMem) {
      const looksEmpty = isPeoplePayloadEffectivelyEmpty(peopleIndexMem);
      const isFresh = isFreshGeneratedAt(peopleIndexMem.generatedAt, PEOPLE_INDEX_TTL_MS);
        if (looksEmpty || !isFresh) {
          peopleIndexMem = null;
        } else {
          return res.json(buildPeopleIndexResponse(peopleIndexMem, { hit: true, layer: 'memory' }));
        }
      }

    // 2) Disk cache (ignore cached-empty or stale results so we don't get stuck on old snapshots forever)
    if (!force) {
      const disk = safeReadJsonFile(PEOPLE_INDEX_FILE);
      const looksEmpty = isPeoplePayloadEffectivelyEmpty(disk);
        const isFresh = disk && isFreshGeneratedAt(disk.generatedAt, PEOPLE_INDEX_TTL_MS);
        if (!looksEmpty && isFresh) {
          peopleIndexMem = disk;
          return res.json(buildPeopleIndexResponse(disk, { hit: true, layer: 'disk' }));
        }
      }

    // 3) Non-force requests must stay on cache layers only.
    if (!force) {
      return res.status(503).json({
        error: 'people index cache unavailable',
        message: 'No cached People index is available yet. Use force=1 to rebuild.',
        cache: { hit: false, layer: 'none' }
      });
    }

    // 4) Compute (source of truth: recursively scan albums under PEOPLE_INDEX_BANDS_ROOT)
    // Force requests bypass stale cache and either start or attach to the current rebuild.
    if (isPeopleIndexBuildTimedOut()) {
      console.warn('people index rebuild exceeded timeout; resetting build state');
      resetPeopleIndexBuildState();
    }

        const attachedToBuild = !!peopleIndexBuildPromise;
        const computed = await startPeopleIndexBuild({ mode: buildMode });
        return res.json(buildPeopleIndexResponse(computed, {
          hit: false,
          layer: 'computed',
          building: attachedToBuild,
          mode: buildMode === 'full' ? 'full-rebuild' : 'incremental-rebuild'
        }));
    } catch (err) {
    const detail = String(err && err.message ? err.message : err || 'unknown error');
    console.error('people index failed:', detail);
    if (force) {
      return res.status(503).json({
        error: 'people index rebuild failed',
        message: 'Full People rebuild failed. Existing People cache and snapshot were preserved.',
        detail
      });
    }
    return res.status(500).json({ error: 'people index failed' });
  }
});

// =========================================================
// SHEETS -> CSV
// =========================================================
app.get("/sheet/bands", async (req, res) => {
  try {
    const csv = await fetchTextWithShortCache('bands', BANDS_SHEET_URL);
    allowCors(res, req);
    setPublicTextCacheHeaders(res, 300);
    res.type("text/plain").send(csv);
  } catch (err) {
    console.error("sheet /bands fetch failed:", err);
    allowCors(res, req);
    res.status(500).send("sheet error");
  }
});

app.get("/sheet/shows", async (req, res) => {
  try {
    const csv = await fetchTextWithShortCache('shows', SHOWS_SHEET_URL);
    allowCors(res, req);
    setPublicTextCacheHeaders(res, 300);
    res.type("text/plain").send(csv);
  } catch (err) {
    console.error("sheet /shows fetch failed:", err);
    allowCors(res, req);
    res.status(500).send("shows sheet error");
  }
});

app.get('/index/bands', async (req, res) => {
  try {
    const force = String(req.query.force || '') === '1';
    if (force) {
      let csv = '';
      const upstream = await fetch(BANDS_SHEET_URL, {
        headers: { Accept: 'text/plain,text/csv;q=0.9,*/*;q=0.8', 'Cache-Control': 'no-cache' }
      });
      if (!upstream.ok) {
        let body = '';
        try { body = await upstream.text(); } catch (_) {}
        const snippet = String(body || '').slice(0, 180).replace(/\s+/g, ' ').trim();
        throw new Error('sheet upstream returned ' + upstream.status + (snippet ? ': ' + snippet : ''));
      }
      csv = await upstream.text();
      sheetResponseCache.set('bands', { text: csv, fetchedAt: Date.now() });
      const payload = buildBandIndexPayload(csv);
      allowCors(res, req);
      setPublicTextCacheHeaders(res, 15);
      return res.json(payload);
    }

    const snapshot = safeReadJsonFile(BAND_INDEX_FILE);
    allowCors(res, req);
    if (snapshot && typeof snapshot === 'object') {
      setPublicTextCacheHeaders(res, 300);
      return res.json(snapshot);
    }

    return res.status(503).json({
      error: 'band index unavailable',
      message: 'No band snapshot is available yet. Use force=1 to generate a fresh export.'
    });
  } catch (err) {
    console.error('band-index fetch failed:', err);
    allowCors(res, req);
    res.status(500).json({ error: 'band index error' });
  }
});

app.get('/index/new-sheet/bands', async (req, res) => {
  try {
    const force = String(req.query.force || '') === '1';
    let csv = '';

    if (force) {
      const upstream = await fetch(NEW_SHEET_BANDS_URL, {
        headers: { Accept: 'text/plain,text/csv;q=0.9,*/*;q=0.8', 'Cache-Control': 'no-cache' }
      });
      if (!upstream.ok) {
        let body = '';
        try { body = await upstream.text(); } catch (_) {}
        const snippet = String(body || '').slice(0, 180).replace(/\s+/g, ' ').trim();
        throw new Error('new sheet upstream returned ' + upstream.status + (snippet ? ': ' + snippet : ''));
      }
      csv = await upstream.text();
      sheetResponseCache.set('new-sheet-bands', { text: csv, fetchedAt: Date.now() });
    } else {
      csv = await fetchTextWithShortCache('new-sheet-bands', NEW_SHEET_BANDS_URL);
    }

    const payload = buildNewSheetBandIndexPayload(csv);
    allowCors(res, req);
    setPublicTextCacheHeaders(res, force ? 15 : 300);
    return res.json(payload);
  } catch (err) {
    console.error('new sheet band-index fetch failed:', err);
    allowCors(res, req);
    res.status(500).json({ error: 'new sheet band index error' });
  }
});

app.get('/index/shows', async (req, res) => {
  try {
    const force = String(req.query.force || '') === '1';
    if (force) {
      let csv = '';
      const upstream = await fetch(SHOWS_SHEET_URL, {
        headers: { Accept: 'text/plain,text/csv;q=0.9,*/*;q=0.8', 'Cache-Control': 'no-cache' }
      });
      if (!upstream.ok) {
        let body = '';
        try { body = await upstream.text(); } catch (_) {}
        const snippet = String(body || '').slice(0, 180).replace(/\s+/g, ' ').trim();
        throw new Error('sheet upstream returned ' + upstream.status + (snippet ? ': ' + snippet : ''));
      }
      csv = await upstream.text();
      sheetResponseCache.set('shows', { text: csv, fetchedAt: Date.now() });
      const payload = buildShowIndexPayload(csv);
      allowCors(res, req);
      setPublicTextCacheHeaders(res, 15);
      return res.json(payload);
    }

    const snapshot = safeReadJsonFile(SHOW_INDEX_FILE);
    allowCors(res, req);
    if (snapshot && typeof snapshot === 'object') {
      setPublicTextCacheHeaders(res, 300);
      return res.json(snapshot);
    }

    return res.status(503).json({
      error: 'show index unavailable',
      message: 'No show snapshot is available yet. Use force=1 to generate a fresh export.'
    });
  } catch (err) {
    console.error('show-index fetch failed:', err);
    allowCors(res, req);
    res.status(500).json({ error: 'show index error' });
  }
});

// Stats tab (Fix / Metadata)
// Aliases included to match different frontend endpoint names used over time.
async function sendStatsCsv(req, res) {
  try {
    const csv = await fetchTextWithShortCache('stats', STATS_SHEET_URL);
    allowCors(res, req);
    setPublicTextCacheHeaders(res, 300);
    res.type("text/plain").send(csv);
  } catch (err) {
    console.error("sheet /stats fetch failed:", err);
    allowCors(res, req);
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

app.get('/recent-activity', async (req, res) => {
  allowCors(res, req);
  const forceFresh = String(req.query.force || '').trim() === '1';
  try {
    const payload = await buildRecentMusicActivityPayload(forceFresh);
    setPublicTextCacheHeaders(res, forceFresh ? 30 : 300);
    return res.json(payload);
  } catch (err) {
    console.error('/recent-activity failed:', err);
    return res.status(500).json({ error: 'recent activity error' });
  }
});

app.get('/geo/album-summary', async (req, res) => {
  allowCors(res, req);
  const forceFresh = String(req.query.force || '').trim() === '1';
  try {
    let albumKey = String(req.query.albumKey || '').trim();
    if (!albumKey) {
      albumKey = await _resolveAlbumKeyFromShowUrl(String(req.query.url || '').trim());
    }
    if (!albumKey) {
      return res.status(400).json({ error: 'missing albumKey or resolvable url' });
    }
    const payload = await getAlbumGeoSummaryCached(albumKey, { force: forceFresh });
    if (!payload) {
      return res.status(404).json({ error: 'geo summary unavailable', albumKey });
    }
    setPublicTextCacheHeaders(res, forceFresh ? 30 : 300);
    return res.json(payload);
  } catch (err) {
    console.error('/geo/album-summary failed:', err);
    return res.status(500).json({ error: 'geo album summary error' });
  }
});

app.get('/geo/report', async (req, res) => {
  allowCors(res, req);
  const forceFresh = String(req.query.force || '').trim() === '1';
  try {
    const payload = await buildMusicGeoReportPayload(forceFresh);
    setPublicTextCacheHeaders(res, forceFresh ? 30 : 300);
    return res.json(payload);
  } catch (err) {
    console.error('/geo/report failed:', err);
    return res.status(500).json({ error: 'geo report error' });
  }
});

app.get('/geo/footprint', async (req, res) => {
  allowCors(res, req);
  const forceFresh = String(req.query.force || '').trim() === '1';
  try {
    const report = await buildMusicGeoReportPayload(forceFresh);
    const items = (Array.isArray(report && report.items) ? report.items : [])
      .filter((item) => item && item.hasGeo && item.center)
      .map((item) => ({
        showName: item.showName,
        showDate: item.showDate,
        prettyDate: item.prettyDate,
        venueLine: item.venueLine,
        albumKey: item.albumKey,
        lat: Number(item.center && item.center.lat || 0),
        lng: Number(item.center && item.center.lng || 0),
        geoTaggedImages: Number(item.geoTaggedImages || 0),
        coveragePct: Number(item.coveragePct || 0),
        mapUrl: item.mapUrl || ""
      }));
    setPublicTextCacheHeaders(res, forceFresh ? 30 : 300);
    return res.json({
      generatedAt: new Date().toISOString(),
      count: items.length,
      items
    });
  } catch (err) {
    console.error('/geo/footprint failed:', err);
    return res.status(500).json({ error: 'geo footprint error' });
  }
});

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
// SMART FOLDER -> ALBUMS
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

const ALBUM_PAGE_CACHE_TTL_MS = 15000;
const albumPageMemCache = new Map();
const albumPageInFlight = new Map();

function albumPageCacheKey(albumKey, count, start) {
  return `${String(albumKey || "").trim()}::${String(count || "").trim()}::${String(start || "").trim()}`;
}

function getFreshAlbumPageCache(key) {
  const hit = albumPageMemCache.get(key);
  if (!hit) return null;
  if (!hit.fetchedAt || (Date.now() - hit.fetchedAt) > ALBUM_PAGE_CACHE_TTL_MS) {
    albumPageMemCache.delete(key);
    return null;
  }
  return hit.data || null;
}

// =========================================================
// ALBUM -> IMAGES (paged)
// =========================================================
app.get("/smug/album/:albumKey", async (req, res) => {
  const albumKey = req.params.albumKey;
  const count = req.query.count || 200;
  const start = req.query.start || 1;
  const cacheKey = albumPageCacheKey(albumKey, count, start);

  allowCors(res, req);

  const cached = getFreshAlbumPageCache(cacheKey);
  if (cached) {
    return res.json(cached);
  }

  if (albumPageInFlight.has(cacheKey)) {
    try {
      const shared = await albumPageInFlight.get(cacheKey);
      return res.json(shared);
    } catch (err) {
      console.error("album images shared proxy error:", err);
      return res.status(500).json({ error: "album images proxy failed" });
    }
  }

  const url = `https://api.smugmug.com/api/v2/album/${encodeURIComponent(
    albumKey
  )}!images?APIKey=${SMUG_API_KEY}&count=${count}&start=${start}&_accept=application/json&_expand=Image`;

  console.log("PROXY ALBUM IMAGES:", url);

  const run = (async () => {
    const upstream = await fetch(url, {
      headers: {
        Accept: "application/json",
        "User-Agent": "SmugProxy/1.0"
      }
    });

    if (!upstream.ok) {
      const body = await upstream.text();
      console.error("upstream album error:", upstream.status, body);
      const err = new Error("album images upstream error");
      err.status = upstream.status;
      throw err;
    }

    const data = await upstream.json();
    albumPageMemCache.set(cacheKey, {
      fetchedAt: Date.now(),
      data
    });
    return data;
  })();

  albumPageInFlight.set(cacheKey, run);

  try {
    const data = await run;
    return res.json(data);
  } catch (err) {
    console.error("album images proxy error:", err);
    return res.status(err.status || 500).json({
      error: err.status ? "album images upstream error" : "album images proxy failed"
    });
  } finally {
    albumPageInFlight.delete(cacheKey);
  }
});

// =========================================================
// NEW: ALBUM METADATA (album keywords)
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
// NEW: ZIP BUILDER (multi-download)
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
// NEW: ANALYTICS EVENT LOGGER (no Google Analytics)
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







