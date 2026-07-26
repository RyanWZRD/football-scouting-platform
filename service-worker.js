// Service worker for the Cross-League Scouting Index PWA.
// Deliberately caches ONLY the static app shell (this file, index.html,
// manifest, icons) — never API responses. This is a live, data-driven
// app; showing stale scouting data offline would be actively misleading.
// The shell just lets the app open instantly and offer a graceful
// "you're offline" experience rather than a blank white screen.

const CACHE_NAME = "scout-index-shell-v1";
const SHELL_FILES = [
  "./index.html",
  "./manifest.json",
  "./icon-192.png",
  "./icon-512.png",
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(SHELL_FILES))
  );
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k)))
    )
  );
  self.clients.claim();
});

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);

  // Never intercept API calls (a different origin, the Render backend) —
  // those must always go to the network for genuinely live data.
  if (url.origin !== self.location.origin) return;

  // Network-first for the shell itself: always try to get the freshest
  // version when online, only falling back to cache if genuinely offline.
  event.respondWith(
    fetch(event.request)
      .then((response) => {
        const copy = response.clone();
        caches.open(CACHE_NAME).then((cache) => cache.put(event.request, copy));
        return response;
      })
      .catch(() => caches.match(event.request))
  );
});
