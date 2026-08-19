const CACHE_NAME = "crew-briefing-shell-v3";
const SHELL_PATHS = [
  "/APG/dcs/crew-briefing",
  "/APG/static/New_Gantt/live_gantt.css?v=crew-briefing-10",
  "/APG/static/New_Gantt/live_gantt.js?v=crew-briefing-10",
  "/APG/static/New_Gantt/crew_briefing_icon.svg",
];

self.addEventListener("install", (event) => {
  event.waitUntil(caches.open(CACHE_NAME).then((cache) => cache.addAll(SHELL_PATHS)).then(() => self.skipWaiting()));
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key))))
      .then(() => self.clients.claim()),
  );
});

self.addEventListener("fetch", (event) => {
  const request = event.request;
  if (request.method !== "GET") return;
  const url = new URL(request.url);
  if (url.origin !== self.location.origin || url.pathname.includes("/api/")) return;

  if (request.mode === "navigate") {
    event.respondWith(
      fetch(request)
        .then((response) => {
          const copy = response.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put("/APG/dcs/crew-briefing", copy));
          return response;
        })
        .catch(() => caches.match("/APG/dcs/crew-briefing")),
    );
    return;
  }

  event.respondWith(caches.match(request).then((cached) => cached || fetch(request)));
});
