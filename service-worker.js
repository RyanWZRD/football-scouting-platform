// Service worker for the Cross-League Scouting Index PWA.
// Two caches, two different purposes:
//  - Shell cache: the static app itself (this file, index.html, manifest, icons)
//  - API cache: GET responses from the backend, used ONLY as a genuine
//    offline fallback — never preferred over a live network response.
// The frontend separately checks navigator.onLine to show an honest
// "you're offline, viewing last-seen data" indicator whenever cached
// API data is what's actually being shown, so nothing here is silently
// presented as if it were live.

const SHELL_CACHE = "scout-index-shell-v1";
const API_CACHE = "scout-index-api-v1";
const API_ORIGIN = "https://football-scouting-api-so8h.onrender.com";
const SHELL_FILES = [
  "./index.html",
  "./manifest.json",
  "./icon-192.png",
  "./icon-512.png",
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(SHELL_CACHE).then((cache) => cache.addAll(SHELL_FILES))
  );
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== SHELL_CACHE && k !== API_CACHE).map((k) => caches.delete(k)))
    )
  );
  self.clients.claim();
});

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);

  // API GET requests: network-first, falling back to a cached response
  // only when genuinely offline — a real fallback, not a replacement
  // for live data.
  if (url.origin === API_ORIGIN && event.request.method === "GET") {
    event.respondWith(
      fetch(event.request)
        .then((response) => {
          const copy = response.clone();
          caches.open(API_CACHE).then((cache) => cache.put(event.request, copy));
          return response;
        })
        .catch(() => caches.match(event.request))
    );
    return;
  }

  // Anything else cross-origin (non-GET API calls, other domains) —
  // always goes straight to the network, untouched.
  if (url.origin !== self.location.origin) return;

  // Network-first for the shell itself: always try to get the freshest
  // version when online, only falling back to cache if genuinely offline.
  event.respondWith(
    fetch(event.request)
      .then((response) => {
        const copy = response.clone();
        caches.open(SHELL_CACHE).then((cache) => cache.put(event.request, copy));
        return response;
      })
      .catch(() => caches.match(event.request))
  );
});
