import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.jsx'

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <App />
  </StrictMode>,
)

// Limpiar cachés antiguas y actualizar Service Worker
if ('caches' in window) {
  caches.delete('muxivo-pwa-v1').catch(() => {});
  caches.delete('muxivo-pwa-v2').catch(() => {});
  caches.delete('muxivo-pwa-v3').catch(() => {});
}
if ('serviceWorker' in navigator) {
  window.addEventListener('load', () => {
    navigator.serviceWorker
      .register('/service-worker.js')
      .then((reg) => {
        reg.update().catch(() => {});
      })
      .catch((err) => {
        console.warn('Service worker registration failed:', err);
      });
  });
}
