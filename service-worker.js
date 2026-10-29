const CACHE_NAME = "muvixo-pwa-v3"

self.addEventListener("install", (event) => {
  event.waitUntil(
    (async () => {
      const cache = await caches.open(CACHE_NAME)
      await cache.addAll([
        "/",
        "/manifest.webmanifest",
        "/icon.png",
        "/icon-apk.png",
        "/logo.svg",
        "/favicon.ico",
      ])
      await self.skipWaiting()
    })()
  )
})

self.addEventListener("activate", (event) => {
  event.waitUntil(
    (async () => {
      const keep = new Set([CACHE_NAME])
      const keys = await caches.keys()
      await Promise.all(keys.filter((k) => !keep.has(k)).map((k) => caches.delete(k)))
      await self.clients.claim()
    })()
  )
})

async function cachedOrFetch(request) {
  const cache = await caches.open(CACHE_NAME)
  const cached = await cache.match(request)
  const fetchPromise = fetch(request)
    .then((resp) => {
      if (resp && resp.ok) cache.put(request, resp.clone()).catch(() => {})
      return resp
    })
    .catch(() => null)
  return cached || (await fetchPromise) || new Response("", { status: 504 })
}

self.addEventListener("fetch", (event) => {
  const req = event.request
  if (req.method !== "GET") return

  const url = new URL(req.url)
  if (url.origin !== self.location.origin) return

  // Nunca cachear endpoints de API ni descargas/previsualizaciones dinámicas
  if (url.pathname.startsWith("/files/") || url.pathname.startsWith("/api/")) return

  const accept = req.headers.get("accept") || ""
  const isHtml = req.mode === "navigate" || accept.includes("text/html")
  if (isHtml) {
    event.respondWith(
      (async () => {
        try {
          const resp = await fetch(req)
          if (resp && resp.ok) {
            const cache = await caches.open(CACHE_NAME)
            cache.put("/", resp.clone()).catch(() => {})
          }
          return resp
        } catch {
          const cached = await caches.match("/", { ignoreSearch: true })
          return cached || new Response("", { status: 504 })
        }
      })()
    )
    return
  }

  const isAsset =
    req.destination === "style" ||
    req.destination === "script" ||
    req.destination === "font" ||
    url.pathname === "/manifest.webmanifest" ||
    url.pathname.startsWith("/static/") ||
    url.pathname.startsWith("/assets/")

  if (isAsset) {
    event.respondWith(cachedOrFetch(req))
  }
})
