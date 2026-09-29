const CACHE_NAME = "crew-briefing-shell-v15";
const SHELL_PATHS = [
  "/APG/dcs/crew-briefing",
  "/APG/static/New_Gantt/live_gantt.css?v=crew-apg-6",
  "/APG/static/New_Gantt/live_gantt.js?v=crew-apg-4",
  "/APG/static/images/ac-crew-brief-icon.png",
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
  // This worker is installed from Crew Briefing but has an APG-wide scope.
  // Never intercept planner, admin or other operational pages.
  if (!url.pathname.startsWith("/APG/dcs/crew-briefing")) return;

  if (request.mode === "navigate") {
    event.respondWith(
      fetch(request)
        .then((response) => {
          const copy = response.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put("/APG/dcs/crew-briefing", copy));
          return response;
        })
        .catch(() => caches.match("/APG/dcs/crew-briefing").then((cached) => cached || new Response("Offline", { status: 503 }))),
    );
    return;
  }

  event.respondWith(caches.match(request).then((cached) => cached || fetch(request)));
});
