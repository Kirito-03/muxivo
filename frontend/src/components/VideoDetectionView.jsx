import React, { useState, useEffect } from 'react';
import { getOptions } from '../utils/api';

// Default quality options if API fails or is slow
const DEFAULT_VIDEO_OPTIONS = [
  { id: '1080', label: '1080p (Full HD)', size: '~18 MB' },
  { id: '720', label: '720p (HD)', size: '~11 MB' },
  { id: '480', label: '480p (SD)', size: '~6 MB' },
];
const DEFAULT_AUDIO_OPTIONS = [
  { id: 'mp3', label: 'MP3', quality: '320 kbps', size: '~8 MB' },
  { id: 'm4a', label: 'M4A', quality: '256 kbps', size: '~6 MB' },
];

function buildVideoOptions(opts) {
  if (!opts) return DEFAULT_VIDEO_OPTIONS;
  const details = opts.detail_choices || ['1080', '720', '480'];
  return details.map((d) => ({
    id: String(d),
    label: `${d}p${d === '1080' ? ' (Full HD)' : d === '720' ? ' (HD)' : d === '480' ? ' (SD)' : ''}`,
    size: '~? MB',
  }));
}

export default function VideoDetectionView({
  mediaData,
  onChangeUrl,
  onStartDownload,
  onUpdateMediaData,
  recentDownloads = [],
  onRedownload,
  sessionId,
}) {
  const [videoOptions, setVideoOptions] = useState(DEFAULT_VIDEO_OPTIONS);
  const [audioOptions, setAudioOptions] = useState(DEFAULT_AUDIO_OPTIONS);
  const [selectedQuality, setSelectedQuality] = useState(DEFAULT_VIDEO_OPTIONS[0]);
  const [loadingOpts, setLoadingOpts] = useState(true);
  const [copiedUrl, setCopiedUrl] = useState(false);
  const [imgError, setImgError] = useState(false);

  // Fetch real format/quality options from /api/options
  useEffect(() => {
    let cancelled = false;
    setLoadingOpts(true);

    getOptions(mediaData.url, 'video')
      .then((opts) => {
        if (cancelled) return;
        const built = buildVideoOptions(opts);
        setVideoOptions(built);
        setSelectedQuality(built[0]);
        setLoadingOpts(false);

        if (opts && onUpdateMediaData) {
          const updates = {};
          if (opts.title && (!mediaData.title || mediaData.title.toLowerCase().includes('detectado'))) {
            updates.title = opts.title;
          }
          if (opts.thumbnail && !mediaData.thumbnail) {
            updates.thumbnail = opts.thumbnail;
          }
          if ((opts.creator || opts.uploader) && !mediaData.creator) {
            updates.creator = opts.creator || opts.uploader;
          }
          if (Object.keys(updates).length > 0) {
            onUpdateMediaData(updates);
          }
        }
      })
      .catch(() => {
        if (cancelled) return;
        setVideoOptions(DEFAULT_VIDEO_OPTIONS);
        setSelectedQuality(DEFAULT_VIDEO_OPTIONS[0]);
        setLoadingOpts(false);
      });

    return () => { cancelled = true; };
  }, [mediaData.url]);

  const displayTitle =
    mediaData?.title && !mediaData.title.toLowerCase().startsWith('video detectado')
      ? mediaData.title
      : (mediaData?.summary && !mediaData.summary.toLowerCase().startsWith('video detectado')
          ? mediaData.summary
          : `${mediaData?.platform || 'Video'} detectado`);

  const rawThumbnail = mediaData?.thumbnail || null;
  const currentThumbnail = rawThumbnail
    ? (rawThumbnail.startsWith('/api/thumb')
        ? rawThumbnail.replace('/api/thumb', '/files/thumb')
        : (rawThumbnail.startsWith('/') ? rawThumbnail : `/files/thumb?url=${encodeURIComponent(rawThumbnail)}`))
    : null;

  const handleCopy = () => {
    if (navigator.clipboard && mediaData?.url) {
      navigator.clipboard.writeText(mediaData.url);
      setCopiedUrl(true);
      setTimeout(() => setCopiedUrl(false), 1500);
    }
  };

  const handleDownloadVideo = () => {
    onStartDownload({
      url: mediaData.url,
      kind: 'video',
      format_id: 'mp4',
      detail: selectedQuality.id,
      sessionId,
      title: displayTitle,
      platform: mediaData.platform,
      type: 'video',
      thumbnail: currentThumbnail,
      format: `Video MP4 • ${selectedQuality.label}`,
    });
  };

  const handleDownloadAudioOnly = () => {
    onStartDownload({
      url: mediaData.url,
      kind: 'audio',
      format_id: 'mp3',
      detail: '320',
      sessionId,
      title: displayTitle,
      platform: mediaData.platform,
      type: 'audio',
      thumbnail: currentThumbnail,
      format: 'Audio MP3 • 320 kbps',
    });
  };

  return (
    <div className="max-w-[1180px] w-full mx-auto pb-24 md:pb-12">
      {/* 2-Column Responsive Layout */}
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-8 items-start">
        {/* Left / Main Column */}
        <section className="lg:col-span-7 flex flex-col gap-6">
          {/* 1. URL Analysis Bar */}
          <div className="bg-[#1b2024] border border-[#1E3640] rounded-2xl p-4 shadow-xl flex items-center justify-between gap-3">
            <div className="flex items-center gap-3 min-w-0 flex-1">
              <div className="w-9 h-9 rounded-xl bg-[#252b2e] border border-[#3d4948]/60 flex items-center justify-center shrink-0 text-[#7dece4] shadow-[0_0_10px_rgba(95,208,200,0.2)]">
                <span className="material-symbols-outlined text-[20px]" style={{ fontVariationSettings: "'FILL' 1" }}>
                  check_circle
                </span>
              </div>
              <div className="min-w-0 flex-1">
                <div className="flex items-center gap-2">
                  <span className="text-sm font-semibold text-[#7dece4]">Enlace analizado</span>
                  <span className="text-[#bcc9c7] text-xs font-mono truncate max-w-sm" title={displayTitle}>
                    • {mediaData.platform} — {displayTitle}
                  </span>
                </div>
                <p className="font-mono text-xs text-[#dee3e7] truncate select-all" title={mediaData?.url}>
                  {mediaData?.url}
                </p>
              </div>
            </div>

            <div className="flex items-center gap-1.5 shrink-0">
              <button
                onClick={handleCopy}
                className="p-2 rounded-xl bg-[#252b2e] hover:bg-[#303639] text-[#bcc9c7] hover:text-[#dee3e7] transition-colors"
                title="Copiar enlace"
                type="button"
              >
                <span className="material-symbols-outlined text-[18px]">
                  {copiedUrl ? 'check' : 'content_copy'}
                </span>
              </button>
              <button
                onClick={onChangeUrl}
                className="px-3 py-2 rounded-xl bg-[#252b2e] hover:bg-[#303639] border border-[#3d4948]/60 text-[#dee3e7] text-xs font-medium transition-colors flex items-center gap-1.5"
                type="button"
              >
                <span className="material-symbols-outlined text-[16px]">edit</span>
                <span>Cambiar</span>
              </button>
            </div>
          </div>

          {/* 2. Content Detection Card */}
          <div className="bg-[#1b2024] border border-[#1E3640] rounded-2xl p-4 sm:p-6 shadow-xl flex flex-col gap-5">
            {/* Thumbnail Display */}
            <div className="relative w-full aspect-video rounded-xl overflow-hidden bg-[#090f12] shadow-2xl group border border-[#1E3640]/50 flex items-center justify-center">
              {currentThumbnail && !imgError ? (
                <>
                  <img
                    src={currentThumbnail}
                    alt={displayTitle}
                    onError={() => setImgError(true)}
                    className="w-full h-full object-cover object-center group-hover:scale-105 transition-transform duration-500 ease-out"
                    loading="eager"
                  />
                  <div className="absolute inset-0 bg-gradient-to-t from-[#090f12]/90 via-[#090f12]/20 to-transparent pointer-events-none"></div>
                </>
              ) : (
                <div className="w-full h-full flex flex-col items-center justify-center bg-gradient-to-br from-[#131b20] to-[#090f12] text-[#5fd0c8]/30">
                  <span className="material-symbols-outlined text-[64px]">smart_display</span>
                  <span className="font-mono text-xs mt-2 text-[#5fd0c8]/40">Vista previa de video</span>
                </div>
              )}

              {/* Media Badge Overlays */}
              <div className="absolute top-3 left-3 sm:top-4 sm:left-4 flex items-center gap-2">
                <span className="px-3 py-1.5 rounded-full bg-[#1b2024]/90 backdrop-blur-md text-[#6FB8E8] font-mono text-xs font-semibold flex items-center gap-1.5 shadow-md border border-[#6FB8E8]/30">
                  <span className="material-symbols-outlined text-[16px]">smart_display</span>
                  <span>Video</span>
                </span>
                <span className="px-3 py-1.5 rounded-full bg-[#1b2024]/90 backdrop-blur-md text-[#dee3e7] font-mono text-xs font-medium border border-[#1E3640]">
                  {mediaData?.platform}
                </span>
              </div>
            </div>

            {/* Video Metadata */}
            <div className="flex flex-col gap-1.5">
              <div className="flex items-center gap-2 text-[#bcc9c7] text-xs font-mono">
                <span className="text-[#7dece4] font-medium">{mediaData?.platform}</span>
                {mediaData?.creator && (
                  <>
                    <span>•</span>
                    <span className="text-[#dee3e7] truncate">{mediaData.creator}</span>
                  </>
                )}
              </div>
              <h1 className="text-xl sm:text-2xl text-[#dee3e7] font-bold leading-snug line-clamp-2" title={displayTitle}>
                {displayTitle}
              </h1>
              {mediaData.message && (
                <div className="mt-2 p-3 rounded-xl bg-[#252b2e] border border-[#f0c060]/40 text-[#f0c060] text-xs font-mono">
                  <span className="material-symbols-outlined text-[14px] mr-1">info</span>
                  {mediaData.message}
                </div>
              )}
            </div>

            {/* 3. Quality Selection Section */}
            <div className="flex flex-col gap-3 pt-2">
              <div className="flex items-center gap-2">
                <span className="text-sm font-semibold text-[#dee3e7]">Calidad de video:</span>
                {loadingOpts && (
                  <span className="material-symbols-outlined text-[16px] text-[#5fd0c8] animate-spin">refresh</span>
                )}
              </div>
              <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
                {videoOptions.map((opt) => {
                  const isSelected = selectedQuality.id === opt.id;
                  return (
                    <button
                      key={opt.id}
                      type="button"
                      onClick={() => setSelectedQuality(opt)}
                      className={`p-3.5 rounded-xl flex items-center justify-between text-left transition-all ${
                        isSelected
                          ? 'bg-[#7dece4]/15 border-2 border-[#5fd0c8] shadow-[0_0_16px_rgba(95,208,200,0.2)]'
                          : 'bg-[#252b2e] border border-[#3d4948]/60 hover:bg-[#303639] hover:border-[#1E3640]'
                      }`}
                    >
                      <div className="flex items-center gap-2.5">
                        <span
                          className={`w-2.5 h-2.5 rounded-full ${
                            isSelected ? 'bg-[#7dece4] shadow-[0_0_8px_#5fd0c8]' : 'bg-[#303639]'
                          }`}
                        ></span>
                        <div className="flex flex-col">
                          <span className={`text-sm font-bold ${isSelected ? 'text-[#7dece4]' : 'text-[#dee3e7]'}`}>
                            {opt.label}
                          </span>
                          <span className="text-xs font-mono text-[#bcc9c7]">{opt.size}</span>
                        </div>
                      </div>
                      {isSelected && (
                        <span
                          className="material-symbols-outlined text-[#7dece4] text-[18px]"
                          style={{ fontVariationSettings: "'FILL' 1" }}
                        >
                          check
                        </span>
                      )}
                    </button>
                  );
                })}
              </div>
            </div>

            {/* 4. Action Buttons */}
            <div className="flex flex-col gap-3 pt-2">
              <button
                onClick={handleDownloadVideo}
                disabled={loadingOpts}
                className="w-full h-14 rounded-2xl bg-[#5fd0c8] hover:bg-[#7FE0D8] active:bg-[#46B8B0] text-[#041014] font-bold text-base flex items-center justify-center gap-2.5 shadow-[0_0_20px_rgba(95,208,200,0.35)] transition-all cursor-pointer active:scale-[0.99] disabled:opacity-60 disabled:cursor-not-allowed"
                type="button"
              >
                <span className="material-symbols-outlined text-[24px]">download</span>
                <span>Descargar video ({selectedQuality.label})</span>
              </button>

              <button
                onClick={handleDownloadAudioOnly}
                className="w-full h-12 rounded-2xl bg-[#252b2e] hover:bg-[#303639] border border-[#3d4948]/60 text-[#dee3e7] text-sm font-semibold flex items-center justify-center gap-2 transition-all cursor-pointer"
                type="button"
              >
                <span className="material-symbols-outlined text-[20px] text-[#7dece4]">audiotrack</span>
                <span>Descargar solo audio (MP3)</span>
              </button>
            </div>

            {/* Legal Notice */}
            <div className="flex items-center justify-center gap-2 pt-1 text-[#869391] text-xs font-mono text-center">
              <span className="material-symbols-outlined text-[16px]">info</span>
              <span>Descarga solo contenido que tengas derecho a usar.</span>
            </div>
          </div>
        </section>

        {/* Right Column / Desktop Aside */}
        <aside className="lg:col-span-5 flex flex-col gap-6">
          {/* Recent Downloads Card */}
          {recentDownloads.length > 0 && (
            <div className="bg-[#1b2024] border border-[#1E3640] rounded-2xl p-5 sm:p-6 shadow-xl flex flex-col gap-5">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <span className="material-symbols-outlined text-[#7dece4] text-[22px]">history</span>
                  <h2 className="text-base font-bold text-[#dee3e7]">Descargas recientes</h2>
                </div>
                <span className="text-xs font-mono text-[#5fd0c8]">Historial</span>
              </div>

              <div className="flex flex-col gap-3">
                {recentDownloads.slice(0, 3).map((item, i) => (
                  <div
                    key={item.ts || i}
                    className="bg-[#252b2e] border border-[#3d4948]/40 rounded-xl p-3 flex items-center gap-3.5 hover:bg-[#303639] transition-colors group"
                  >
                    <div className="w-10 h-10 rounded-lg bg-[#1b2024] flex items-center justify-center shrink-0 text-[#5fd0c8]">
                      <span className="material-symbols-outlined text-[20px]">
                        {item.kind === 'audio' ? 'audiotrack' : item.kind === 'image' ? 'image' : 'smart_display'}
                      </span>
                    </div>
                    <div className="flex-1 min-w-0">
                      <h3 className="text-xs font-bold text-[#dee3e7] truncate group-hover:text-[#5fd0c8] transition-colors">
                        {item.label || '(sin título)'}
                      </h3>
                      <p className="text-[11px] font-mono text-[#bcc9c7] truncate">
                        {item.kind?.toUpperCase()} • {item.ok} archivo{item.ok !== 1 ? 's' : ''}
                      </p>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Quick Platform Info */}
          <div className="bg-[#1b2024] border border-[#1E3640] rounded-2xl p-5 shadow-xl flex flex-col gap-3">
            <h3 className="text-sm font-bold text-[#dee3e7]">Compatibilidad Muvixo</h3>
            <p className="text-xs text-[#bcc9c7] leading-relaxed">
              Soporta video, audio y colecciones completas desde las principales plataformas con conversión instantánea y cero compresión innecesaria.
            </p>
            <div className="grid grid-cols-2 gap-2.5 pt-1">
              <div className="p-2.5 rounded-xl bg-[#252b2e] border border-[#3d4948]/50 flex items-center gap-2">
                <span className="material-symbols-outlined text-[#7dece4] text-[18px]">hd</span>
                <span className="text-xs font-mono text-[#dee3e7]">Hasta 4K UHD</span>
              </div>
              <div className="p-2.5 rounded-xl bg-[#252b2e] border border-[#3d4948]/50 flex items-center gap-2">
                <span className="material-symbols-outlined text-[#7dece4] text-[18px]">music_note</span>
                <span className="text-xs font-mono text-[#dee3e7]">320kbps MP3</span>
              </div>
            </div>
          </div>
        </aside>
      </div>
    </div>
  );
}
