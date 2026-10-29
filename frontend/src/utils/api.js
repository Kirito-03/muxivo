// API client for Muxivo Flask backend

const BASE = '';

/**
 * Detect media type and platform from a URL.
 * GET /api/detect?raw_input=URL
 * Returns: { platform, detected_kind, allowed_kinds, type, is_gallery, items, summary, message, error }
 */
export async function detectMedia(url) {
  const params = new URLSearchParams({ raw_input: url });
  const res = await fetch(`${BASE}/api/detect?${params}`);
  if (!res.ok) throw new Error(`Error ${res.status}: ${res.statusText}`);
  return res.json();
}

/**
 * Get download format/quality options for a URL.
 * GET /api/options?raw_input=URL&kind=video|audio|image
 */
export async function getOptions(url, kind = 'video') {
  const params = new URLSearchParams({ raw_input: url, kind });
  const res = await fetch(`${BASE}/api/options?${params}`);
  if (!res.ok) throw new Error(`Error ${res.status}: ${res.statusText}`);
  return res.json();
}

/**
 * Start a download.
 * POST /api/download
 * Body: { raw_input, kind, format, detail, session_id, image_urls? }
 * Returns: { ok, tone, message, total, success, failed, label, files, zip, failures }
 */
export async function startDownload({
  rawInput,
  kind,
  format,
  detail,
  sessionId,
  imageUrls,
  title,
  thumbnail,
  creator,
  platform,
  formatLabel,
}) {
  const body = {
    raw_input: rawInput,
    kind,
    format: format || '',
    detail: detail || '',
    session_id: sessionId || '',
    title: title || '',
    thumbnail: thumbnail || '',
    creator: creator || '',
    platform: platform || '',
    format_label: formatLabel || '',
  };
  if (imageUrls) body.image_urls = imageUrls;

  const res = await fetch(`${BASE}/api/download`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });

  const data = await res.json();
  if (!res.ok) {
    throw new Error(data.message || `Error ${res.status}`);
  }
  return data;
}

/**
 * Get download history for the current session.
 * GET /api/history?session_id=X
 * Returns: { items: [...] }
 */
export async function getHistory(sessionId) {
  if (!sessionId) return { items: [] };
  const params = new URLSearchParams({ session_id: sessionId });
  const res = await fetch(`${BASE}/api/history?${params}`);
  if (!res.ok) return { items: [] };
  return res.json();
}

/**
 * Get or create a persistent session ID stored in localStorage.
 */
export function getSessionId() {
  try {
    let id = localStorage.getItem('muxivo_session_id');
    if (!id) {
      id = 'sess_' + Date.now() + '_' + Math.random().toString(36).slice(2, 10);
      localStorage.setItem('muxivo_session_id', id);
    }
    return id;
  } catch {
    return 'sess_default';
  }
}
