const el = (id) => document.getElementById(id)

const urlInput    = el("urlInput")
const urlLabel    = el("urlLabel")
const pasteBtn    = el("pasteBtn")
const clearBtn    = el("clearBtn")
const formatSelect  = el("formatSelect")
const qualitySelect = el("qualitySelect")
const downloadBtn   = el("downloadBtn")
const downloadLabel = el("downloadLabel")
const btnIconNormal  = el("btnIconNormal")
const btnIconLoading = el("btnIconLoading")
const btnIconSuccess = el("btnIconSuccess")
const statusEl      = el("status")
const archiveList   = el("archiveList")
const archiveEmpty  = el("archiveEmpty")
const zipRow        = el("zipRow")
const failuresEl    = el("failures")
const playbackEl    = el("playback")
const playbackSelect   = el("playbackSelect")
const playbackDownload = el("playbackDownload")
const playbackControls = el("playbackControls")
const archiveAcc  = el("archiveAcc")
const playbackAcc = el("playbackAcc")
const imagePicker = el("imagePicker")
const imageCandidateList = el("imageCandidateList")
const pickAllBtn = el("pickAllBtn")
const pickNoneBtn = el("pickNoneBtn")
let detectArea = el("detectArea")
let detectLoader = el("detectLoader")
let detectDots = el("detectDots")
let detectInfo = el("detectInfo")
let detectIcon = el("detectIcon")
let detectPlatform = el("detectPlatform")
let detectType = el("detectType")
let detectError = el("detectError")

let kind = "audio"
let lastFiles = []
let historyItems = []
let currentDownload = null
let selectedImageUrls = []
let currentImageCandidates = []
let lastDetectFiles = []
let currentDetectRef = ""
let currentPreviewUrl = ""
let currentDetectPlatformKey = "other"
let downloadBlocked = false
let downloadBlockReason = ""
let isLoading = false
let detectTimer = null
let detectSeq = 0
let dotsTimer = null
let dotsStep = 0

// ── Session ID for per-device archive ──
function getSessionId() {
  let sid = localStorage.getItem("muxivo_session_id")
  if (!sid) {
    sid = crypto.randomUUID()
    localStorage.setItem("muxivo_session_id", sid)
  }
  return sid
}
const SESSION_ID = getSessionId()

const DEFAULTS = {
  audio: {
    format_choices: ["mp3", "m4a", "opus", "ogg", "wav", "flac"],
    detail_choices: ["320", "256", "192", "160", "128"],
    format_value: "mp3",
    detail_value: "192",
  },
  video: {
    format_choices: ["mp4", "webm", "mkv"],
    detail_choices: ["1080", "720", "480", "360"],
    format_value: "mp4",
    detail_value: "720",
  },
  image: {
    format_choices: ["auto"],
    detail_choices: ["original"],
    format_value: "auto",
    detail_value: "original",
  },
}

// Sync label visibility + clear button based on textarea content
function syncUrlUI() {
  const hasValue = (urlInput.value || "").trim().length > 0
  // Hide label when there's content
  if (urlLabel) urlLabel.classList.toggle("is-hidden", hasValue)
  // Show/hide clear button
  if (clearBtn) clearBtn.hidden = !hasValue
}

function setStatus(message, tone) {
  statusEl.textContent = message || ""
  statusEl.classList.remove("is-success", "is-warning", "is-error")
  if (tone === "success") statusEl.classList.add("is-success")
  if (tone === "warning") statusEl.classList.add("is-warning")
  if (tone === "error")   statusEl.classList.add("is-error")
}

function setLoading(loading) {
  isLoading = Boolean(loading)
  downloadBtn.disabled = isLoading || downloadBlocked
  downloadBtn.classList.toggle("is-loading", isLoading)
  downloadBtn.setAttribute("aria-busy", isLoading ? "true" : "false")
  if (downloadLabel) downloadLabel.textContent = "DESCARGAR"
}

// ── Button state controller ──
let _btnResetTimer = null
function setButtonState(state) {
  // Limpiar timer previo de auto-reset
  if (_btnResetTimer) { clearTimeout(_btnResetTimer); _btnResetTimer = null }

  // Resetear todos los íconos al estado base
  if (btnIconNormal)  { btnIconNormal.classList.remove("d-none", "spin-anim", "pop-anim") }
  if (btnIconLoading) { btnIconLoading.classList.remove("spin-anim"); btnIconLoading.classList.add("d-none") }
  if (btnIconSuccess) { btnIconSuccess.classList.remove("pop-anim"); btnIconSuccess.classList.add("d-none") }

  if (state === "loading") {
    if (btnIconNormal)  btnIconNormal.classList.add("d-none")
    if (btnIconLoading) { btnIconLoading.classList.remove("d-none"); btnIconLoading.classList.add("spin-anim") }
    downloadBtn.disabled = true
    downloadBtn.setAttribute("aria-busy", "true")
    if (downloadLabel) downloadLabel.textContent = "DESCARGANDO..."

  } else if (state === "success") {
    if (btnIconNormal)  btnIconNormal.classList.add("d-none")
    if (btnIconSuccess) { btnIconSuccess.classList.remove("d-none"); btnIconSuccess.classList.add("pop-anim") }
    downloadBtn.disabled = true
    downloadBtn.setAttribute("aria-busy", "false")
    if (downloadLabel) downloadLabel.textContent = "COMPLETADO"
    // Volver a normal tras 3s
    _btnResetTimer = setTimeout(() => setButtonState("normal"), 3000)

  } else {
    // "normal"
    downloadBtn.disabled = isLoading || downloadBlocked
    downloadBtn.setAttribute("aria-busy", "false")
    if (downloadLabel) downloadLabel.textContent = "DESCARGAR"
  }
}

function setDownloadBlocked(blocked, reason) {
  downloadBlocked = Boolean(blocked)
  downloadBlockReason = String(reason || "").trim()
  if (!isLoading) downloadBtn.disabled = downloadBlocked
}

function ensureDetectDom() {
  if (detectArea && detectLoader && detectInfo && detectError) return true
  const box = document.querySelector(".url-box")
  if (!box) return false

  const actions = box.querySelector(".url-actions")

  let area = box.querySelector("#detectArea")
  if (!area) {
    area = document.createElement("div")
    area.id = "detectArea"
    area.className = "url-detect-area"
    area.hidden = true
  }

  const loader = document.createElement("div")
  loader.id = "detectLoader"
  loader.className = "url-detect-line"
  loader.hidden = true
  const loaderText = document.createElement("span")
  loaderText.textContent = "ANALIZANDO ENLACE"
  const dots = document.createElement("span")
  dots.id = "detectDots"
  dots.className = "url-dots"
  loader.appendChild(loaderText)
  loader.appendChild(dots)

  const info = document.createElement("div")
  info.id = "detectInfo"
  info.className = "url-detect-line"
  info.hidden = true
  const icon = document.createElement("span")
  icon.id = "detectIcon"
  icon.className = "url-detect-icon"
  icon.setAttribute("aria-hidden", "true")
  const plat = document.createElement("span")
  plat.id = "detectPlatform"
  plat.className = "url-detect-platform"
  const sep = document.createElement("span")
  sep.className = "url-detect-sep"
  sep.textContent = "•"
  const typ = document.createElement("span")
  typ.id = "detectType"
  typ.className = "url-detect-type"
  info.appendChild(icon)
  info.appendChild(plat)
  info.appendChild(sep)
  info.appendChild(typ)

  const err = document.createElement("div")
  err.id = "detectError"
  err.className = "url-detect-line url-detect-error"
  err.hidden = true

  area.innerHTML = ""
  area.appendChild(loader)
  area.appendChild(info)
  area.appendChild(err)

  if (!box.querySelector("#detectArea")) {
    if (actions) box.insertBefore(area, actions)
    else box.appendChild(area)
  }

  detectArea = el("detectArea")
  detectLoader = el("detectLoader")
  detectDots = el("detectDots")
  detectInfo = el("detectInfo")
  detectIcon = el("detectIcon")
  detectPlatform = el("detectPlatform")
  detectType = el("detectType")
  detectError = el("detectError")
  return Boolean(detectArea && detectLoader && detectInfo && detectError)
}

function stopDots() {
  if (dotsTimer) clearInterval(dotsTimer)
  dotsTimer = null
  dotsStep = 0
  if (detectDots) detectDots.textContent = ""
}

function startDots() {
  stopDots()
  if (!detectDots) return
  dotsTimer = setInterval(() => {
    dotsStep = (dotsStep + 1) % 4
    detectDots.textContent = ".".repeat(dotsStep)
  }, 420)
}

function setDetectState(state, payload) {
  if (!ensureDetectDom()) return
  if (state === "idle") {
    detectArea.hidden = true
    detectLoader.hidden = true
    detectInfo.hidden = true
    detectError.hidden = true
    stopDots()
    return
  }

  detectArea.hidden = false
  detectLoader.hidden = state !== "loading"
  detectInfo.hidden = state !== "ok"
  detectError.hidden = state !== "error"

  if (state === "loading") {
    startDots()
    return
  }

  stopDots()

  if (state === "error") {
    detectError.textContent = String(payload && payload.message ? payload.message : "No se pudo analizar el enlace.")
    return
  }

  const platform = String(payload && payload.platform ? payload.platform : "")
  const kind = String(payload && payload.kind ? payload.kind : "")
  if (detectPlatform) detectPlatform.textContent = platform
  if (detectType) detectType.textContent = `TIPO: ${kind.toUpperCase()}`
  if (detectIcon) detectIcon.innerHTML = payload && payload.icon ? payload.icon : ""
}

function platformMeta(platformKey) {
  const key = String(platformKey || "").toLowerCase()
  const ICONS = {
    tiktok:
      '<svg viewBox="0 0 24 24" aria-hidden="true"><path fill="currentColor" d="M14 3c.6 3.6 2.7 5.7 6 6v3c-2.1 0-4-.7-6-2v6.8c0 3.4-2.5 6.2-6 6.2-3.3 0-6-2.7-6-6.1 0-3.5 2.7-6.2 6.2-6.2.4 0 .8 0 1.2.1v3.2c-.3-.1-.6-.1-.9-.1-1.6 0-2.9 1.3-2.9 2.9 0 1.5 1.3 2.9 2.9 2.9 1.8 0 2.9-1.2 2.9-3.4V3h2.6z"/></svg>',
    youtube:
      '<svg viewBox="0 0 24 24" aria-hidden="true"><path fill="currentColor" d="M21 7.9a3 3 0 0 0-2.1-2.1C17 5.3 12 5.3 12 5.3s-5 0-6.9.5A3 3 0 0 0 3 7.9 31.4 31.4 0 0 0 2.6 12c0 1.4.1 2.7.4 4.1a3 3 0 0 0 2.1 2.1c1.9.5 6.9.5 6.9.5s5 0 6.9-.5a3 3 0 0 0 2.1-2.1c.3-1.4.4-2.7.4-4.1 0-1.4-.1-2.7-.4-4.1zM10.2 15.4V8.6L15.8 12l-5.6 3.4z"/></svg>',
    instagram:
      '<svg viewBox="0 0 24 24" aria-hidden="true"><path fill="currentColor" d="M7.5 2h9A5.5 5.5 0 0 1 22 7.5v9A5.5 5.5 0 0 1 16.5 22h-9A5.5 5.5 0 0 1 2 16.5v-9A5.5 5.5 0 0 1 7.5 2zm0 2A3.5 3.5 0 0 0 4 7.5v9A3.5 3.5 0 0 0 7.5 20h9A3.5 3.5 0 0 0 20 16.5v-9A3.5 3.5 0 0 0 16.5 4h-9z"/><path fill="currentColor" d="M12 7a5 5 0 1 1 0 10 5 5 0 0 1 0-10zm0 2.1A2.9 2.9 0 1 0 12 15a2.9 2.9 0 0 0 0-5.9z"/><path fill="currentColor" d="M17.6 6.3a1.1 1.1 0 1 1-2.2 0 1.1 1.1 0 0 1 2.2 0z"/></svg>',
    direct:
      '<svg viewBox="0 0 24 24" aria-hidden="true"><path fill="currentColor" d="M4 6.5A4.5 4.5 0 0 1 8.5 2h7A4.5 4.5 0 0 1 20 6.5v11A4.5 4.5 0 0 1 15.5 22h-7A4.5 4.5 0 0 1 4 17.5v-11zm4.5-2.5A2.5 2.5 0 0 0 6 6.5v11A2.5 2.5 0 0 0 8.5 20h7A2.5 2.5 0 0 0 18 17.5v-11A2.5 2.5 0 0 0 15.5 4h-7z"/></svg>',
    other:
      '<svg viewBox="0 0 24 24" aria-hidden="true"><path fill="currentColor" d="M12 2a10 10 0 1 1 0 20 10 10 0 0 1 0-20zm0 2a8 8 0 1 0 0 16 8 8 0 0 0 0-16z"/><path fill="currentColor" d="M11 10h2v7h-2v-7zm0-3h2v2h-2V7z"/></svg>',
  }

  const LABELS = {
    tiktok: "TikTok",
    youtube: "YouTube",
    instagram: "Instagram",
    direct: "Imagen",
    other: "Enlace",
  }

  return {
    platform: LABELS[key] || "Enlace",
    icon: ICONS[key] || ICONS.other,
  }
}

function setKind(nextKind) {
  kind = nextKind
  document.querySelectorAll(".seg-btn").forEach((btn) => {
    const active = btn.dataset.kind === kind
    btn.classList.toggle("is-active", active)
    btn.setAttribute("aria-selected", active ? "true" : "false")
  })
  applyOptions(DEFAULTS[kind])
  if (kind === "audio" || kind === "video") {
    fetchOptions().catch(() => {})
  }
}

function fillSelect(select, choices, value) {
  select.innerHTML = ""
  for (const c of choices || []) {
    const opt = document.createElement("option")
    opt.value = String(c)
    if (select === qualitySelect && kind === "video" && /^\d+$/.test(String(c))) {
      opt.textContent = `${String(c).toUpperCase()}P`
    } else if (select === qualitySelect && kind === "audio" && /^\d+$/.test(String(c))) {
      opt.textContent = `${String(c).toUpperCase()}KBPS`
    } else {
      opt.textContent = String(c).toUpperCase()
    }
    select.appendChild(opt)
  }
  if (value != null) select.value = String(value)
  if (!select.value && select.options.length > 0) select.selectedIndex = 0
}

function applyOptions(data) {
  const opts = data && (data.format_choices || data.detail_choices) ? data : DEFAULTS[kind]
  fillSelect(formatSelect, opts.format_choices, opts.format_value)
  fillSelect(qualitySelect, opts.detail_choices, opts.detail_value)
  qualitySelect.disabled = kind === "image"
}

async function fetchOptions() {
  if (
    downloadBlocked &&
    kind === "video" &&
    currentDetectPlatformKey === "instagram" &&
    String(currentDetectRef || "").includes("/p/")
  ) {
    applyOptions(DEFAULTS[kind])
    return
  }
  const raw = (urlInput.value || "").trim()
  if (!raw) { applyOptions(DEFAULTS[kind]); return }
  const qs = new URLSearchParams({ raw_input: raw, kind })
  const res = await fetch(`/api/options?${qs.toString()}`, { headers: { Accept: "application/json" } })
  if (!res.ok) throw new Error("options_failed")
  const data = await res.json()
  if (data && data.detected_from_source) {
    applyOptions(data)
  } else {
    applyOptions(data)
  }
}

function validUrlLines(text) {
  const re = /https?:\/\/\S+/i
  return (text || "")
    .split(/\r?\n/)
    .map((s) => {
      const t = String(s || "").trim()
      const m = t.match(re)
      return m && m[0] ? String(m[0]).replace(/^[`'"<\[{(]+|[`'">)\]}.,;]+$/g, "") : ""
    })
    .filter((s) => /^https?:\/\/\S+/i.test(s))
}

function setTabsAllowed(allowedKinds) {
  const allowed = Array.isArray(allowedKinds) && allowedKinds.length ? new Set(allowedKinds) : null
  document.querySelectorAll(".seg-btn").forEach((btn) => {
    const k = btn.dataset.kind
    const ok = !allowed || allowed.has(k)
    btn.disabled = !ok
    btn.classList.toggle("is-disabled", !ok)
  })
  if (allowed && !allowed.has(kind)) {
    const next = allowed.has("video") ? "video" : allowed.has("audio") ? "audio" : allowed.has("image") ? "image" : "video"
    setKind(next)
  }
}

function renderImagePicker(candidates) {
  if (!imagePicker || !imageCandidateList) return
  imageCandidateList.innerHTML = ""
  selectedImageUrls = []

  const list = Array.isArray(candidates) ? candidates : []
  currentImageCandidates = list
  if (list.length === 0) {
    imagePicker.hidden = true
    return
  }

  const proxied = (u, index) => {
    const s = String(u || "").trim()
    if (!s) return ""
    if (s.startsWith("/files/")) return s
    const qs = new URLSearchParams({ url: s })
    if (currentDetectRef) qs.set("ref", currentDetectRef)
    if (typeof index === "number" && Number.isFinite(index)) {
      qs.set("idx", String(index))
      qs.set("ts", String(Date.now()))
    }
    return `/api/thumb?${qs.toString()}`
  }

  console.log("gallery render", list.map((x, i) => ({ i, url: x && x.url, thumb: x && x.thumb })))

  const nameFor = (label, url) => {
    const rawLabel = String(label || "image").trim() || "image"
    const idx = (() => {
      const m = rawLabel.match(/(\d+)/)
      return m && m[1] ? m[1] : ""
    })()
    let base = ""
    try {
      const u = new URL(String(currentDetectRef || ""))
      const p = (u.pathname || "").toLowerCase()
      const plat = String(currentDetectPlatformKey || "media").toLowerCase()
      if (plat === "instagram" && p.includes("/p/")) {
        const m = p.match(/\/p\/([^/]+)/)
        base = m && m[1] ? `instagram_${m[1]}` : "instagram_post"
      } else if (plat === "tiktok" && p.includes("/photo/")) {
        const m = p.match(/\/photo\/(\d+)/)
        base = m && m[1] ? `tiktok_${m[1]}` : "tiktok_photo"
      } else {
        base = plat || "image"
      }
    } catch {
      base = String(currentDetectPlatformKey || "image").toLowerCase() || "image"
    }
    const safe = (idx ? `${base}_${idx}` : base).replace(/[^\w.-]+/g, "_").replace(/^_+|_+$/g, "") || "image"
    let ext = "jpg"
    try {
      const p = new URL(String(url || ""))
      const m = (p.pathname || "").match(/\.(jpe?g|png|webp)(?:$|\?)/i)
      if (m && m[1]) ext = m[1].toLowerCase().replace("jpeg", "jpg")
    } catch {
    }
    return `${safe}.${ext}`
  }

  const setPreview = (u, label) => {
    const url = String(u || "").trim()
    if (!url) return
    currentPreviewUrl = url
    renderPlayback([{ name: nameFor(label, url), url: proxied(url, 0), kind: "image" }])
    if (playbackAcc) playbackAcc.open = true
  }

  const firstSelected = () => {
    const u = Array.isArray(selectedImageUrls) && selectedImageUrls.length ? selectedImageUrls[0] : ""
    if (!u) return
    const item = (currentImageCandidates || []).find((x) => x && String(x.url) === String(u))
    setPreview(u, item && item.label ? item.label : "IMAGE")
  }

  if (list.length === 1) {
    const onlyUrl = list[0] && list[0].url ? String(list[0].url) : ""
    const onlyLabel = list[0] && list[0].label ? String(list[0].label) : "IMAGE"
    selectedImageUrls = onlyUrl ? [onlyUrl] : []
    imagePicker.hidden = true
    if (onlyUrl) setPreview(onlyUrl, onlyLabel)
    return
  }

  let rowIndex = 0
  for (const c of list) {
    const url = c && c.url ? String(c.url) : ""
    if (!url) continue
    const label = c && c.label ? String(c.label) : "IMAGE"
    const thumbUrl = c && c.thumb ? String(c.thumb) : url
    rowIndex += 1

    const li = document.createElement("li")
    li.className = "pick-item"
    li.dataset.index = String(rowIndex)

    const cb = document.createElement("input")
    cb.type = "checkbox"
    cb.checked = true
    cb.dataset.url = url
    cb.dataset.index = String(rowIndex)

    const thumb = document.createElement("img")
    thumb.className = "pick-thumb"
    thumb.alt = label
    thumb.loading = "lazy"
    thumb.src = proxied(thumbUrl, rowIndex)
    thumb.dataset.index = String(rowIndex)

    const name = document.createElement("span")
    name.className = "pick-name"
    name.textContent = label

    li.appendChild(cb)
    li.appendChild(thumb)
    li.appendChild(name)
    imageCandidateList.appendChild(li)
    selectedImageUrls.push(url)

    li.addEventListener("click", (e) => {
      if (e.target && e.target.tagName === "INPUT") return
      cb.checked = true
      syncSelection()
      setPreview(url, label)
    })

    cb.addEventListener("change", () => {
      syncSelection()
      if (cb.checked) setPreview(url, label)
      else if (currentPreviewUrl === url) firstSelected()
    })
  }

  imagePicker.hidden = imageCandidateList.childElementCount === 0

  const syncSelection = () => {
    selectedImageUrls = Array.from(imageCandidateList.querySelectorAll("input[type=checkbox]:checked"))
      .map((x) => x.dataset.url)
      .filter(Boolean)
  }

  const setAll = (checked) => {
    imageCandidateList.querySelectorAll("input[type=checkbox]").forEach((cb) => { cb.checked = Boolean(checked) })
    syncSelection()
    if (checked) firstSelected()
    else {
      currentPreviewUrl = ""
      renderPlayback([])
    }
  }

  imageCandidateList.onchange = syncSelection
  if (pickAllBtn)  pickAllBtn.onclick  = () => setAll(true)
  if (pickNoneBtn) pickNoneBtn.onclick = () => setAll(false)

  firstSelected()
}

async function detectFromInput() {
  const raw = (urlInput.value || "").trim()
  const urls = validUrlLines(raw)
  if (!raw || urls.length !== 1) {
    setDownloadBlocked(false, "")
    setTabsAllowed(null)
    if (imagePicker) imagePicker.hidden = true
    selectedImageUrls = []
    currentImageCandidates = []
    lastDetectFiles = []
    setDetectState("idle")
    fetchOptions().catch(() => {})
    return
  }

  const prevKind = kind
  const seq = ++detectSeq
  setDetectState("loading")
  setDownloadBlocked(false, "")
  const qs = new URLSearchParams({ raw_input: urls[0] })
  let data = {}
  try {
    const res = await fetch(`/api/detect?${qs.toString()}`, { headers: { Accept: "application/json" } })
    if (!res.ok) throw new Error("detect_failed")
    data = await res.json().catch(() => ({}))
  } catch {
    if (seq === detectSeq) setDetectState("error", { message: "No se pudo analizar el enlace." })
    return
  }
  if (seq !== detectSeq) return

  if (data && data.error) {
    setTabsAllowed(null)
    if (imagePicker) imagePicker.hidden = true
    selectedImageUrls = []
    setDownloadBlocked(true, String(data.error))
    setDetectState("error", { message: String(data.error) })
    return
  }

  const type = typeof data.type === "string" ? String(data.type).toUpperCase() : null
  const detected =
    type === "IMAGE" || type === "GALLERY" ? "image" :
    type === "VIDEO" ? "video" :
    type === "AUDIO" ? "audio" :
    (typeof data.detected_kind === "string" ? data.detected_kind : null)

  const allowed = Array.isArray(data.allowed_kinds) ? data.allowed_kinds : (
    Array.isArray(data.disable_modes)
      ? ["audio", "video", "image"].filter((k) => !new Set(data.disable_modes.map((x) => String(x))).has(k))
      : null
  )

  if (detected === "image") {
    setTabsAllowed(["image"])
    if (kind !== "image") setKind("image")
  } else if (detected === "audio" || detected === "video") {
    setTabsAllowed(["audio", "video"])
    if (kind !== detected) setKind(detected)
  } else {
    setTabsAllowed(allowed)
  }

  const items = Array.isArray(data.items) ? data.items : data.image_candidates
  currentDetectRef = String(data.resolved_url || urls[0] || "").trim()
  currentDetectPlatformKey = String(data.platform || "other").toLowerCase()
  renderImagePicker(items)

  const dfiles = Array.isArray(data.files) ? data.files : []
  lastDetectFiles = dfiles
  if (dfiles.length) {
    renderPlayback(dfiles)
    if (playbackAcc) playbackAcc.open = true
  }

  const meta = platformMeta(data.platform || "other")
  const showKind = type || detected || ""
  if (meta.platform && showKind) {
    setDetectState("ok", { platform: meta.platform, kind: showKind, icon: meta.icon })
  } else {
    setDetectState("idle")
  }

  const isIgPost =
    currentDetectPlatformKey === "instagram" &&
    String(currentDetectRef || "").includes("/p/") &&
    !String(currentDetectRef || "").includes("/reel/")

  const hasAllowed = Array.isArray(data.allowed_kinds) ? data.allowed_kinds : null
  const allowedHasImage = Array.isArray(hasAllowed) && hasAllowed.includes("image")
  const allowedHasVideo = Array.isArray(hasAllowed) && hasAllowed.includes("video")
  const reliableDetected = detected === "image" || detected === "video" || detected === "audio"

  if (isIgPost && (!reliableDetected || (allowedHasImage && allowedHasVideo))) {
    const msg =
      (data && data.message ? String(data.message) : "") ||
      "No se pudo determinar el tipo real del post de Instagram en este entorno."
    setDownloadBlocked(true, msg)
    setStatus(msg, "warning")
  } else if (data && data.message) {
    setStatus(String(data.message), "warning")
  }

  if (!downloadBlocked && kind === prevKind && (kind === "audio" || kind === "video")) {
    fetchOptions().catch(() => {})
  }
}

function scheduleDetect() {
  if (detectTimer) clearTimeout(detectTimer)
  detectTimer = setTimeout(() => {
    detectFromInput().catch(() => {})
  }, 220)
}

function renderArchive(data) {
  // data puede ser la respuesta directa de /api/download o un item de historial
  currentDownload = {
    label:    data.label    || "",
    zip:      data.zip      || null,
    files:    data.files    || [],
    failures: data.failures || [],
    total:    data.total    != null ? data.total    : (data.files || []).length + (data.failures || []).length,
    success:  data.success  != null ? data.success  : (data.files || []).length,
    failed:   data.failed   != null ? data.failed   : (data.failures || []).length,
  }
  renderArchiveView()
}

// ── KIND icons para el Archive ──
const KIND_ICON = {
  audio: `<svg viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true" width="13" height="13"><path d="M9 2v9.354A2.5 2.5 0 1 1 7.5 9.1V4.5L3 5.5V12a2.5 2.5 0 1 1-1.5-2.3V5l7.5-2V2H9z" fill="currentColor"/></svg>`,
  video: `<svg viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true" width="13" height="13"><path d="M2 4a1 1 0 0 0-1 1v6a1 1 0 0 0 1 1h8a1 1 0 0 0 1-1V5a1 1 0 0 0-1-1H2zm9 1.5 3-1.5v8l-3-1.5V5.5z" fill="currentColor"/></svg>`,
  image: `<svg viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true" width="13" height="13"><rect x="1" y="2" width="14" height="12" rx="1" stroke="currentColor" stroke-width="1.5" fill="none"/><circle cx="5.5" cy="6.5" r="1.5" fill="currentColor"/><path d="M1 11l3.5-4 3 3 2-2 4.5 5" stroke="currentColor" stroke-width="1.2" fill="none"/></svg>`,
  file:  `<svg viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true" width="13" height="13"><path d="M3 2h7l3 3v9H3V2z" stroke="currentColor" stroke-width="1.5" fill="none"/><path d="M10 2v3h3" stroke="currentColor" stroke-width="1.2" fill="none"/></svg>`,
  zip:   `<svg viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true" width="13" height="13"><path d="M3 2h7l3 3v9H3V2z" stroke="currentColor" stroke-width="1.5" fill="none"/><path d="M10 2v3h3" stroke="currentColor" stroke-width="1.2" fill="none"/><path d="M7 6v4M6 9h2M6 7h2M7 11v1" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/></svg>`,
}

function _kindIcon(kind) {
  return KIND_ICON[kind] || KIND_ICON.file
}

function _ts(ts) {
  if (!ts) return ""
  try {
    const d = new Date(Number(ts) * 1000)
    const pad = (n) => String(n).padStart(2, "0")
    return `${pad(d.getHours())}:${pad(d.getMinutes())}`
  } catch {
    return ""
  }
}

function renderArchiveView() {
  // Limpiar todo
  zipRow.innerHTML = ""
  archiveList.innerHTML = ""
  failuresEl.innerHTML = ""

  let hasAnyContent = false

  // ── Renderizar descarga actual (si existe) ──
  if (currentDownload && (currentDownload.files.length > 0 || currentDownload.failures.length > 0)) {
    hasAnyContent = true
    const section = _buildArchiveSection(currentDownload, /* isCurrent= */ true)
    archiveList.appendChild(section)
  }

  // ── Renderizar historial de sesión ──
  for (const h of historyItems || []) {
    if (!h) continue
    const hFiles   = h.files    || []
    const hFails   = h.failures || []
    const hZip     = h.zip      || null
    if (hFiles.length === 0 && hFails.length === 0 && !hZip) continue

    const hData = {
      label:    h.label   || "",
      ts:       h.ts      || null,
      zip:      hZip,
      files:    hFiles,
      failures: hFails,
      total:    h.total   != null ? h.total   : hFiles.length + hFails.length,
      success:  h.ok      != null ? h.ok      : hFiles.length,
      failed:   h.fail    != null ? h.fail    : hFails.length,
    }
    hasAnyContent = true
    const section = _buildArchiveSection(hData, /* isCurrent= */ false)
    archiveList.appendChild(section)
  }

  if (archiveEmpty) archiveEmpty.classList.toggle("is-hidden", hasAnyContent)
  window.__archiveByUrl = _buildArchiveUrlMap()
}

function _buildArchiveUrlMap() {
  const map = new Map()
  // Mapear desde currentDownload
  if (currentDownload) {
    for (const f of currentDownload.files || []) {
      if (f && f.url) map.set(f.url, f)
    }
  }
  // Mapear desde historial
  for (const h of historyItems || []) {
    for (const f of h.files || []) {
      if (f && f.url) map.set(f.url, f)
    }
  }
  return map
}

function _buildArchiveSection(data, isCurrent) {
  const section = document.createElement("div")
  section.className = "archive-section" + (isCurrent ? " archive-section--current" : "")

  // ── Header del grupo ──
  const header = document.createElement("div")
  header.className = "archive-section-header"

  const labelEl = document.createElement("span")
  labelEl.className = "archive-section-label"
  labelEl.textContent = data.label || "Descarga"

  const metaEl = document.createElement("span")
  metaEl.className = "archive-section-meta"

  const parts = []
  if (data.success != null && data.success > 0) {
    parts.push(`<span class="archive-meta-ok">${data.success} ok</span>`)
  }
  if (data.failed != null && data.failed > 0) {
    parts.push(`<span class="archive-meta-fail">${data.failed} fallo(s)</span>`)
  }
  if (data.ts) {
    parts.push(`<span class="archive-meta-time">${_ts(data.ts)}</span>`)
  }
  metaEl.innerHTML = parts.join(" · ")

  header.appendChild(labelEl)
  header.appendChild(metaEl)
  section.appendChild(header)

  // ── Lista de archivos exitosos ──
  if ((data.files || []).length > 0) {
    const ul = document.createElement("ul")
    ul.className = "archive-file-list"

    for (const f of data.files) {
      if (!f || !f.url) continue
      const li = document.createElement("li")
      li.className = "archive-file-item archive-file-item--done"

      const iconSpan = document.createElement("span")
      iconSpan.className = "archive-file-icon"
      iconSpan.innerHTML = _kindIcon(f.kind)

      const nameSpan = document.createElement("span")
      nameSpan.className = "archive-file-name"
      nameSpan.textContent = f.name || f.url
      nameSpan.title = f.name || f.url

      const dlBtn = document.createElement("a")
      dlBtn.href = f.url + "?download=1"
      dlBtn.download = f.name || ""
      dlBtn.className = "archive-file-dl"
      dlBtn.title = "Descargar"
      dlBtn.innerHTML = `<svg viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true" width="14" height="14"><path d="M8 2v8M5 7l3 3 3-3M2 13h12" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>`

      // Click en el nombre → playback
      const playLink = document.createElement("a")
      playLink.href = f.url
      playLink.className = "archive-file-play-link"
      playLink.dataset.play = "1"
      playLink.appendChild(iconSpan)
      playLink.appendChild(nameSpan)

      li.appendChild(playLink)
      li.appendChild(dlBtn)
      ul.appendChild(li)
    }
    section.appendChild(ul)
  }

  // ── Fallidos ──
  if ((data.failures || []).length > 0) {
    const failList = document.createElement("ul")
    failList.className = "archive-fail-list"

    for (const fail of data.failures) {
      if (!fail) continue
      const li = document.createElement("li")
      li.className = "archive-file-item archive-file-item--failed"

      const iconSpan = document.createElement("span")
      iconSpan.className = "archive-file-icon archive-file-icon--fail"
      iconSpan.innerHTML = `<svg viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true" width="13" height="13"><circle cx="8" cy="8" r="7" stroke="currentColor" stroke-width="1.5"/><path d="M8 5v3.5M8 11h.01" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>`

      const textDiv = document.createElement("div")
      textDiv.className = "archive-fail-text"

      const labelEl2 = document.createElement("span")
      labelEl2.className = "archive-fail-label"
      labelEl2.textContent = fail.label || "archivo"

      const reasonEl = document.createElement("span")
      reasonEl.className = "archive-fail-reason"
      reasonEl.textContent = fail.reason || "Error desconocido"

      textDiv.appendChild(labelEl2)
      textDiv.appendChild(reasonEl)
      li.appendChild(iconSpan)
      li.appendChild(textDiv)
      failList.appendChild(li)
    }
    section.appendChild(failList)
  }

  // ── Botón ZIP ──
  if (data.zip && data.zip.url) {
    const zipWrap = document.createElement("div")
    zipWrap.className = "archive-zip-row"

    const za = document.createElement("a")
    za.href = data.zip.url
    za.className = "archive-zip-btn"
    za.download = data.zip.name || "download.zip"
    za.innerHTML = `${_kindIcon("zip")}<span>DESCARGAR ZIP</span><span class="archive-zip-name">${data.zip.name || ""}</span>`

    zipWrap.appendChild(za)
    section.appendChild(zipWrap)
  }

  return section
}

async function loadHistory() {
  try {
    const qs = new URLSearchParams({ session_id: SESSION_ID })
    const res = await fetch(`/api/history?${qs.toString()}`, { headers: { Accept: "application/json" } })
    if (!res.ok) return
    const data = await res.json().catch(() => ({}))
    historyItems = Array.isArray(data.items) ? data.items : []
    renderArchiveView()
  } catch {
  }
}

function renderPlayback(files) {
  lastFiles = files || []
  playbackSelect.innerHTML = ""
  if (!lastFiles.length) {
    playbackSelect.disabled = true
    playbackDownload.hidden = true
    if (playbackControls) playbackControls.hidden = true
    playbackEl.innerHTML = `<svg class="play-icon-default" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><polygon points="6,3 20,12 6,21" fill="rgba(255,255,255,0.18)"/></svg>`
    return
  }

  if (playbackControls) playbackControls.hidden = false
  for (const f of lastFiles) {
    const opt = document.createElement("option")
    opt.value = f.url
    opt.textContent = f.name
    playbackSelect.appendChild(opt)
  }
  playbackSelect.disabled = false
  playbackSelect.selectedIndex = 0
  renderPlaybackItem(lastFiles[0])
}

function renderPlaybackItem(item) {
  if (!item) return
  playbackDownload.hidden = false
  playbackDownload.href = item.url
  playbackDownload.setAttribute("download", item.name || "")

  const src = item.url
  if (item.kind === "audio") {
    playbackEl.innerHTML = `<audio controls src="${src}"></audio>`
    return
  }
  if (item.kind === "video") {
    playbackEl.innerHTML = `<video controls playsinline src="${src}"></video>`
    return
  }
  if (item.kind === "image") {
    playbackEl.innerHTML = `<img alt="${item.name || "preview"}" src="${src}" />`
    return
  }
  playbackEl.innerHTML = ""
}

async function pasteFromClipboard() {
  try {
    const text = await navigator.clipboard.readText()
    if (!text) return
    const current = (urlInput.value || "").trim()
    urlInput.value = current ? `${current}\n${text.trim()}` : text.trim()
    syncUrlUI()
    scheduleDetect()
  } catch {
    setStatus("No se pudo leer el portapapeles.", "warning")
  }
}

async function startDownload() {
  const raw = (urlInput.value || "").trim()
  if (!raw) { setStatus("Pega al menos una URL.", "warning"); return }
  if (downloadBlocked) {
    setStatus(downloadBlockReason || "No se pudo determinar el tipo real del enlace.", "warning")
    return
  }

  if (kind === "image" && Array.isArray(currentImageCandidates) && currentImageCandidates.length > 0) {
    const count = Array.isArray(selectedImageUrls) ? selectedImageUrls.length : 0
    if (count <= 0) {
      setStatus("Selecciona al menos una imagen.", "warning")
      return
    }
  }

  isLoading = true
  setButtonState("loading")
  setStatus("Descargando...", undefined)

  try {
    const payload = { raw_input: raw, kind, format: formatSelect.value, detail: qualitySelect.value, session_id: SESSION_ID }
    if (kind === "image" && Array.isArray(currentImageCandidates) && currentImageCandidates.length > 0 && Array.isArray(selectedImageUrls)) {
      payload.image_urls = selectedImageUrls.slice(0, 200)
    }

    // Ejecutar el fetch y un delay mínimo en paralelo para que el loader siempre sea visible
    const minDelay = new Promise((r) => setTimeout(r, 500))
    const fetchPromise = fetch("/api/download", {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify(payload),
    })

    const [res] = await Promise.all([fetchPromise, minDelay])
    const data = await res.json().catch(() => ({}))

    if (!res.ok) {
      setStatus(data.message || "Error al descargar.", "error")
      setButtonState("normal")
      return
    }
    setStatus(data.message || "Listo.", data.tone)
    setButtonState("success")
    // Pasar el objeto completo al archive (nueva estructura)
    renderArchive(data)
    renderPlayback(data.files)
    if (archiveAcc) archiveAcc.open = false
    if (playbackAcc) playbackAcc.open = false
    loadHistory().catch(() => {})
  } catch {
    setStatus("Error de red o servidor.", "error")
    setButtonState("normal")
  } finally {
    isLoading = false
  }
}

// ── Event listeners ──
document.querySelectorAll(".seg-btn").forEach((btn) => {
  btn.addEventListener("click", () => setKind(btn.dataset.kind))
})

pasteBtn.addEventListener("click", pasteFromClipboard)
downloadBtn.addEventListener("click", startDownload)

clearBtn.addEventListener("click", () => {
  urlInput.value = ""
  syncUrlUI()
  scheduleDetect()
  setStatus("", undefined)
})

urlInput.addEventListener("input", () => {
  setStatus("", undefined)
  syncUrlUI()
  scheduleDetect()
})

archiveList.addEventListener("click", (e) => {
  const a = e.target.closest("a[data-play]")
  if (!a) return
  if (e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return
  const url = a.getAttribute("href")
  if (!url) return
  const map = window.__archiveByUrl
  const item = map && map.get ? map.get(url) : null
  if (!item) return
  e.preventDefault()
  renderPlayback([item])
  if (playbackAcc) playbackAcc.open = false
})

playbackSelect.addEventListener("change", () => {
  const url  = playbackSelect.value
  const item = lastFiles.find((f) => f.url === url)
  renderPlaybackItem(item)
})

// ── Init ──
if (archiveAcc)  { archiveAcc.open  = false; archiveAcc.removeAttribute("open") }
if (playbackAcc) { playbackAcc.open = false; playbackAcc.removeAttribute("open") }
applyOptions(DEFAULTS[kind])
syncUrlUI()
loadHistory().catch(() => {})
scheduleDetect()

// ── Global loader fade-out + Service Worker ──
window.addEventListener("load", () => {
  const loader = document.getElementById("global-loader")
  if (loader) loader.classList.add("loader-hidden")

  if ("serviceWorker" in navigator) {
    navigator.serviceWorker.register("/service-worker.js").catch(() => {})
  }
})
