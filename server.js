console.log(">>> SERVER FILE VERSION: PATCHED-FULL-1 <<<");

const express = require("express");
const archiver = require("archiver");
const http = require("http");
const https = require("https");
const { URL } = require("url");
const app = express();

// =========================================================
// UNIVERSAL CORS
// =========================================================
function allowCors(res) {
  // Allow embedding/requests from anywhere (frontend is hosted separately)
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  // Include common headers used by fetch() and browsers
  res.set("Access-Control-Allow-Headers", "Content-Type, Accept, Authorization, X-Requested-With");
  // Optional: make it easier to debug in DevTools
  res.set("Access-Control-Expose-Headers", "Content-Type, Content-Length");
}

// Always attach CORS headers (including for static files) and handle OPTIONS fast
app.use((req, res, next) => {
  allowCors(res);
  if (req.method === "OPTIONS") return res.status(204).send("");
  next();
});


app.use(express.static(__dirname));
app.use(express.json({ limit: "2mb" }));


// SmugMug API Key
const SMUG_API_KEY = "SQLhhqgXZJd7MzqgVX563bkbjdCfXt9T";

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
  const url = `https://api.smugmug.com/api/v2${endpoint}&APIKey=${SMUG_API_KEY}`;

  const r = await fetch(url, {
    headers: {
      Accept: "application/json",
      "User-Agent": "SmugProxy/1.0"
    }
  });

  if (!r.ok) {
    throw new Error(`SmugMug upstream returned ${r.status}`);
  }

  return r.json();
}

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
    if (!eventName) return;

    // Keep payload small + predictable
    const payload = {
      event: eventName.slice(0, 64),
      band: body.band ? String(body.band).slice(0, 120) : "",
      show: body.show ? String(body.show).slice(0, 160) : "",
      year: body.year ? String(body.year).slice(0, 16) : "",
      category: body.category ? String(body.category).slice(0, 48) : "",
      page: body.page ? String(body.page).slice(0, 400) : "",
      referrer: body.referrer ? String(body.referrer).slice(0, 400) : "",
      sessionId: body.sessionId ? String(body.sessionId).slice(0, 80) : "",
      // Optional extra object; will be JSON-stringified by Apps Script
      extra: body.extra && typeof body.extra === "object" ? body.extra : undefined,

      // Lightweight request context (useful later)
      ip: String(req.headers["x-forwarded-for"] || req.socket?.remoteAddress || "").slice(0, 64),
      ua: String(req.headers["user-agent"] || "").slice(0, 220)
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
