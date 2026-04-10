const CACHE = 'sv-v1';
const PRECACHE = ['/', '/static/styles.css'];

self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(PRECACHE)));
  self.skipWaiting();
});

self.addEventListener('activate', e => {
  e.waitUntil(caches.keys().then(keys =>
    Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
  ));
  self.clients.claim();
});

self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);
  // API 和 WebSocket 不缓存
  if (url.pathname.startsWith('/api/') || url.pathname === '/ws') return;
  // 静态资源：cache-first
  if (url.pathname.startsWith('/static/')) {
    e.respondWith(caches.match(e.request).then(r => r || fetch(e.request).then(resp => {
      const clone = resp.clone();
      caches.open(CACHE).then(c => c.put(e.request, clone));
      return resp;
    })));
    return;
  }
  // 页面：network-first
  e.respondWith(fetch(e.request).catch(() => caches.match(e.request)));
});
