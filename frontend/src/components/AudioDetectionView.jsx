import React, { useState, useEffect } from 'react';
import { getOptions } from '../utils/api';

const DEFAULT_AUDIO_OPTIONS = [
  { id: 'mp3', label: 'MP3', quality: '320 kbps', size: '~8 MB' },
  { id: 'm4a', label: 'M4A', quality: '256 kbps', size: '~6 MB' },
];

export default function AudioDetectionView({
  mediaData,
  onChangeUrl,
  onStartDownload,
  onUpdateMediaData,
  sessionId,
}) {
  const [audioOptions, setAudioOptions] = useState(DEFAULT_AUDIO_OPTIONS);
  const [selectedFormat, setSelectedFormat] = useState(DEFAULT_AUDIO_OPTIONS[0]);
  const [copiedUrl, setCopiedUrl] = useState(false);
  const [imgError, setImgError] = useState(false);

  useEffect(() => {
    let cancelled = false;
    getOptions(mediaData.url, 'audio')
      .then((opts) => {
        if (cancelled) return;
        const fmts = opts.format_choices || ['mp3', 'm4a'];
        const built = fmts.map((f) => ({
          id: f,
          label: f.toUpperCase(),
          quality: f === 'mp3' ? '320 kbps' : f === 'flac' ? 'Lossless' : '256 kbps',
          size: '~? MB',
        }));
        setAudioOptions(built);
        setSelectedFormat(built[0]);

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
      .catch(() => {});
    return () => { cancelled = true; };
  }, [mediaData.url]);

  const displayTitle =
    mediaData?.title && !mediaData.title.toLowerCase().startsWith('audio detectado')
      ? mediaData.title
      : (mediaData?.summary && !mediaData.summary.toLowerCase().startsWith('audio detectado')
          ? mediaData.summary
          : `${mediaData?.platform || 'Audio'} detectado`);

  const currentThumbnail = mediaData?.thumbnail || null;

  const handleCopy = () => {
    if (navigator.clipboard && mediaData?.url) {
      navigator.clipboard.writeText(mediaData.url);
      setCopiedUrl(true);
      setTimeout(() => setCopiedUrl(false), 1500);
    }
  };

  const handleDownload = () => {
    onStartDownload({
      url: mediaData.url,
      kind: 'audio',
      format_id: selectedFormat.id,
      detail: selectedFormat.id === 'mp3' ? '320' : '256',
      sessionId,
      title: displayTitle,
      platform: mediaData.platform,
      type: 'audio',
      thumbnail: currentThumbnail,
      format: `Audio ${selectedFormat.label} • ${selectedFormat.quality || '320 kbps'}`,
    });
  };

  return (
    <div className="max-w-[560px] w-full mx-auto pb-24 md:pb-12 flex flex-col gap-5">
      {/* 1. Analyzed URL Bar */}
      <div className="flex items-center justify-between w-full bg-[#1b2024] rounded-xl px-4 py-3 shadow-md border border-[#1E3640]">
        <div className="flex items-center gap-2.5 min-w-0 flex-1">
          <span className="material-symbols-outlined text-[#7dece4] text-[20px] shrink-0">link</span>
          <span className="w-full bg-transparent text-[#D5EEEC] font-mono text-xs sm:text-sm tracking-tight truncate select-all">
            {mediaData?.url}
          </span>
        </div>
        <div className="flex items-center gap-1.5 shrink-0 ml-2">
          <button
            aria-label="Copiar enlace"
            className="p-1.5 rounded-lg bg-[#252b2e] hover:bg-[#303639] text-[#bcc9c7] hover:text-[#7dece4] transition-colors"
            onClick={handleCopy}
            type="button"
          >
            <span className="material-symbols-outlined text-[18px]">
              {copiedUrl ? 'check' : 'content_copy'}
            </span>
          </button>
          <button
            onClick={onChangeUrl}
            className="px-2.5 py-1 rounded-lg bg-[#252b2e] hover:bg-[#303639] text-[#dee3e7] text-xs font-medium transition-colors"
            type="button"
          >
            Cambiar
          </button>
        </div>
      </div>

      {/* 2. Audio Content Card */}
      <div className="relative w-full bg-[#1b2024] rounded-2xl p-5 sm:p-6 shadow-xl border border-[#1E3640] flex flex-col gap-4">
        {/* Album Art / Thumbnail 16:9 */}
        <div className="relative w-full aspect-video rounded-xl overflow-hidden bg-[#090f12] shadow-inner border border-[#1E3640]/50 group flex items-center justify-center">
          {currentThumbnail && !imgError ? (
            <>
              <img
                src={currentThumbnail}
                alt={displayTitle}
                onError={() => setImgError(true)}
                className="w-full h-full object-cover group-hover:scale-105 transition-transform duration-500"
                loading="eager"
              />
              <div className="absolute inset-0 bg-gradient-to-t from-[#090f12]/80 via-transparent to-transparent"></div>
            </>
          ) : (
            <div className="w-full h-full flex flex-col items-center justify-center bg-gradient-to-br from-[#131b20] to-[#090f12] text-[#5FD99A]/30">
              <span className="material-symbols-outlined text-[64px]">audiotrack</span>
              <span className="font-mono text-xs mt-2 text-[#5FD99A]/40">Pista de audio</span>
            </div>
          )}

          {/* Floating Audio Play Icon */}
          <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
            <div className="w-14 h-14 rounded-full bg-[#13232A]/80 backdrop-blur-md border border-[#5FD99A]/40 flex items-center justify-center text-[#5FD99A] shadow-[0_0_20px_rgba(95,217,154,0.3)]">
              <span className="material-symbols-outlined text-[30px]" style={{ fontVariationSettings: "'FILL' 1" }}>
                audiotrack
              </span>
            </div>
          </div>

          {/* Audio Badge */}
          <div className="absolute top-3 left-3 flex items-center gap-2">
            <div className="flex items-center gap-1.5 px-3 py-1 rounded-full bg-[#090f12]/80 backdrop-blur-md text-[#5FD99A] border border-[#5FD99A]/30">
              <span className="material-symbols-outlined text-[16px]">music_note</span>
              <span className="font-mono text-xs font-semibold tracking-wide">Audio</span>
            </div>
            <span className="px-3 py-1 rounded-full bg-[#090f12]/80 backdrop-blur-md text-[#dee3e7] font-mono text-xs font-medium border border-[#1E3640]">
              {mediaData?.platform}
            </span>
          </div>
        </div>

        {/* Title & Artist */}
        <div>
          <h2 className="text-xl sm:text-2xl text-[#dee3e7] font-bold leading-snug line-clamp-2" title={displayTitle}>
            {displayTitle}
          </h2>
          <p className="text-sm font-mono text-[#bcc9c7] mt-1">
            {mediaData?.creator || mediaData?.platform} • Audio estéreo
          </p>
        </div>

        {/* Format Selector Chips */}
        <div className="flex flex-col gap-2.5 pt-2 border-t border-[#1E3640]">
          <label className="text-sm text-[#bcc9c7] font-medium">
            Formato de salida:
          </label>
          <div className="grid grid-cols-2 gap-3">
            {audioOptions.map((opt) => {
              const isSelected = selectedFormat.id === opt.id;
              return (
                <button
                  key={opt.id}
                  onClick={() => setSelectedFormat(opt)}
                  type="button"
                  className={`flex flex-col items-start p-3.5 rounded-xl text-left transition-all ${
                    isSelected
                      ? 'bg-[#252b2e] border-2 border-[#5fd0c8] shadow-[0_0_16px_rgba(95,208,200,0.2)]'
                      : 'bg-[#171c20] border border-[#1E3640] hover:bg-[#252b2e]'
                  }`}
                >
                  <div className="flex items-center justify-between w-full mb-1">
                    <span className={`text-base font-bold ${isSelected ? 'text-[#7dece4]' : 'text-[#dee3e7]'}`}>
                      {opt.label}
                    </span>
                    <span
                      className={`w-4 h-4 rounded-full flex items-center justify-center ${
                        isSelected ? 'bg-[#5fd0c8]' : 'bg-[#303639]'
                      }`}
                    >
                      <span className={`w-1.5 h-1.5 rounded-full ${isSelected ? 'bg-[#041014]' : 'bg-transparent'}`}></span>
                    </span>
                  </div>
                  <span className="text-xs font-mono text-[#bcc9c7]">
                    {opt.size} • {opt.quality || '320 kbps'}
                  </span>
                </button>
              );
            })}
          </div>
        </div>

        {/* Primary CTA Button */}
        <div className="pt-2 flex flex-col gap-3">
          <button
            onClick={handleDownload}
            className="w-full h-14 rounded-2xl bg-[#5fd0c8] hover:bg-[#7FE0D8] active:bg-[#46B8B0] text-[#041014] font-bold text-base flex items-center justify-center gap-2.5 shadow-[0_0_20px_rgba(95,208,200,0.35)] active:scale-[0.99] transition-all cursor-pointer"
            type="button"
          >
            <span className="material-symbols-outlined text-[24px]">download</span>
            <span>Descargar Audio ({selectedFormat.label})</span>
          </button>

          <p className="text-center text-xs font-mono text-[#869391] leading-relaxed">
            Descarga solo contenido que tengas derecho a usar.
          </p>
        </div>
      </div>
    </div>
  );
}
