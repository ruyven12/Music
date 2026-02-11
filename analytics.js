// Lightweight client analytics (no Google Analytics).
//
// Usage:
//   <script src="https://music-archive-3lfa.onrender.com/analytics.js"></script>
//   <script>
//     trackEvent('band_click', { band: 'Echo Ritual', year: '2024', category: 'Local' });
//   </script>
//
// Notes:
// - Uses navigator.sendBeacon when available (best for click/navigation events).
// - Falls back to fetch({ keepalive:true }).
// - Never throws; it should not break your UI.

(function () {
  const API_BASE = "https://music-archive-3lfa.onrender.com";
  const ENDPOINT = API_BASE + "/track";

  function getSessionId() {
    try {
      const key = "vmpix_session_id";
      let id = sessionStorage.getItem(key);
      if (!id) {
        id = Math.random().toString(36).slice(2) + Date.now().toString(36);
        sessionStorage.setItem(key, id);
      }
      return id;
    } catch (_) {
      return "";
    }
  }

  function trackEvent(event, fields) {
    try {
      if (!event) return;
      const payload = Object.assign(
        {
          event: String(event),
          page: window.location.href,
          referrer: document.referrer || "",
          sessionId: getSessionId()
        },
        fields || {}
      );

      const body = JSON.stringify(payload);

      if (navigator.sendBeacon) {
        const blob = new Blob([body], { type: "application/json" });
        navigator.sendBeacon(ENDPOINT, blob);
        return;
      }

      fetch(ENDPOINT, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body,
        keepalive: true
      }).catch(function () {});
    } catch (_) {
      // swallow
    }
  }

  // Expose globally
  window.trackEvent = trackEvent;

  // Baseline event to confirm wiring
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", function () {
      trackEvent("page_load", {
        path: window.location.pathname || "",
        hash: window.location.hash || ""
      });
    });
  } else {
    trackEvent("page_load", {
      path: window.location.pathname || "",
      hash: window.location.hash || ""
    });
  }
})();
