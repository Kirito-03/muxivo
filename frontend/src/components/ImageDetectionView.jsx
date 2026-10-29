import React, { useState } from 'react';

export default function ImageDetectionView({
  mediaData,
  onChangeUrl,
  onStartDownload,
  sessionId,
}) {
  const [photoIndex, setPhotoIndex] = useState(0);
  const [scope, setScope] = useState('all'); // 'all' | 'single'
  const [format, setFormat] = useState('JPG'); // 'JPG' | 'PNG'
  const [copiedUrl, setCopiedUrl] = useState(false);

  // Real image candidates from the server
  const candidates = mediaData?.imageCandidates || [];
  const rawPhotos = candidates.length > 0
    ? candidates.map((c) => c.url || c.thumb).filter(Boolean)
    : [mediaData?.thumbnail].filter(Boolean);
  const totalPhotos = rawPhotos.length;
  const isMultiPhoto = totalPhotos > 1;

  const toPreviewUrl = (u) => {
    if (!u) return '';
    if (u.startsWith('/')) return u;
    return `/api/thumb?url=${encodeURIComponent(u)}`;
  };

  const handlePrev = () => {
    if (!isMultiPhoto) return;
    setPhotoIndex((prev) => (prev - 1 + totalPhotos) % totalPhotos);
  };

  const handleNext = () => {
    if (!isMultiPhoto) return;
    setPhotoIndex((prev) => (prev + 1) % totalPhotos);
  };

  const handleCopy = () => {
    if (navigator.clipboard && mediaData?.url) {
      navigator.clipboard.writeText(mediaData.url);
      setCopiedUrl(true);
      setTimeout(() => setCopiedUrl(false), 1500);
    }
  };

  const buttonLabel = !isMultiPhoto
    ? `Descargar Imagen (${format})`
    : scope === 'all'
      ? `Descargar Galería (${totalPhotos} fotos • ${format})`
      : `Descargar Foto ${photoIndex + 1} (${format})`;

  const handleDownload = () => {
    // Collect image URLs to download
    let imageUrls;
    if (!isMultiPhoto || scope === 'all') {
      imageUrls = candidates.length > 0
        ? candidates.map((c) => c.url).filter(Boolean)
        : rawPhotos.filter((u) => u && u.startsWith('http'));
    } else {
      const candidate = candidates[photoIndex];
      imageUrls = candidate ? [candidate.url] : undefined;
    }

    onStartDownload({
      url: mediaData.url,
      kind: 'image',
      format_id: format.toLowerCase(),
      detail: '',
      sessionId,
      imageUrls: imageUrls && imageUrls.length > 0 ? imageUrls : undefined,
      title: !isMultiPhoto || scope === 'all'
        ? (mediaData.title || mediaData.summary || mediaData.platform)
        : `${mediaData.title || mediaData.summary || mediaData.platform} (Foto ${photoIndex + 1})`,
      platform: mediaData.platform,
      type: 'image',
      thumbnail: toPreviewUrl(rawPhotos[photoIndex]) || null,
      format: `${isMultiPhoto && scope === 'all' ? 'Galería' : 'Imagen'} ${format} • ${isMultiPhoto && scope === 'all' ? `${totalPhotos} fotos` : '1 foto'}`,
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

      {/* 2. Image Content Card with Carousel */}
      <div className="relative w-full bg-[#1b2024] rounded-2xl p-5 sm:p-6 shadow-xl border border-[#1E3640] flex flex-col gap-4">
        {/* Preview with Navigation */}
        <div className="relative w-full min-h-[260px] max-h-[520px] rounded-xl overflow-hidden bg-[#090f12] shadow-inner border border-[#1E3640]/50 group flex items-center justify-center">
          {rawPhotos[photoIndex] ? (
            <img
              src={toPreviewUrl(rawPhotos[photoIndex])}
              alt={`Foto ${photoIndex + 1}`}
              className="w-full max-h-[520px] object-contain bg-[#090f12] transition-all duration-300"
            />
          ) : (
            <div className="w-full h-64 flex items-center justify-center text-[#5fd0c8]/40">
              <span className="material-symbols-outlined text-[48px]">image</span>
            </div>
          )}

          {/* Photo Counter Pill */}
          <div className="absolute top-3 left-3 flex items-center gap-1.5 px-3 py-1 rounded-full bg-[#090f12]/85 backdrop-blur-md border border-[#1E3640] shadow-md">
            <span className="material-symbols-outlined text-[16px] text-[#7dece4]">
              {isMultiPhoto ? 'photo_library' : 'image'}
            </span>
            <span className="font-mono text-xs text-[#dee3e7]">
              {isMultiPhoto ? `${photoIndex + 1} de ${totalPhotos} fotos` : 'Imagen única'}
            </span>
          </div>

          {/* Carousel Arrows (only when gallery has >1 photo) */}
          {isMultiPhoto && (
            <div className="absolute inset-x-3 top-1/2 -translate-y-1/2 flex items-center justify-between pointer-events-auto">
              <button
                onClick={handlePrev}
                className="w-9 h-9 rounded-full bg-[#090f12]/80 hover:bg-[#5fd0c8] text-[#dee3e7] hover:text-[#041014] flex items-center justify-center transition-all shadow-md active:scale-95"
                title="Foto anterior"
                type="button"
              >
                <span className="material-symbols-outlined text-[20px]">chevron_left</span>
              </button>
              <button
                onClick={handleNext}
                className="w-9 h-9 rounded-full bg-[#090f12]/80 hover:bg-[#5fd0c8] text-[#dee3e7] hover:text-[#041014] flex items-center justify-center transition-all shadow-md active:scale-95"
                title="Foto siguiente"
                type="button"
              >
                <span className="material-symbols-outlined text-[20px]">chevron_right</span>
              </button>
            </div>
          )}
        </div>

        {/* Thumbnail Strip for Multi-Photo Galleries */}
        {isMultiPhoto && (
          <div className="flex items-center gap-2 overflow-x-auto pb-1 pt-0.5 scrollbar-thin">
            {rawPhotos.map((u, idx) => {
              const active = idx === photoIndex;
              return (
                <button
                  key={`${idx}-${u}`}
                  type="button"
                  onClick={() => setPhotoIndex(idx)}
                  className={`relative w-16 h-20 rounded-lg overflow-hidden shrink-0 border-2 transition-all ${
                    active
                      ? 'border-[#5fd0c8] shadow-[0_0_10px_rgba(95,208,200,0.4)] scale-[1.03]'
                      : 'border-[#1E3640] opacity-65 hover:opacity-100'
                  }`}
                  title={`Ver foto ${idx + 1}`}
                >
                  <img
                    src={toPreviewUrl(u)}
                    alt={`Miniatura ${idx + 1}`}
                    className="w-full h-full object-cover"
                    loading="lazy"
                  />
                  <span className="absolute bottom-1 right-1 px-1.5 py-0.2 rounded bg-[#090f12]/85 text-[10px] font-mono text-[#dee3e7]">
                    {idx + 1}
                  </span>
                </button>
              );
            })}
          </div>
        )}

        {/* Info & Badges */}
        <div className="flex flex-col gap-1.5">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2 text-xs font-mono text-[#bcc9c7]">
              <span className="text-[#7dece4] font-semibold">{mediaData?.platform}</span>
              {mediaData?.creator && (
                <>
                  <span>•</span>
                  <span>{mediaData.creator}</span>
                </>
              )}
            </div>
            <span className="px-2.5 py-0.5 rounded-full bg-amber-500/15 text-amber-300 font-mono text-xs font-semibold border border-amber-500/30">
              {isMultiPhoto ? `Galería (${totalPhotos})` : 'Imagen'}
            </span>
          </div>
          <h2 className="text-xl sm:text-2xl text-[#dee3e7] font-bold leading-snug">
            {mediaData?.title || mediaData?.summary || mediaData?.platform || 'Imagen detectada'}
          </h2>
          {mediaData.message && (
            <div className="mt-1 p-3 rounded-xl bg-[#252b2e] border border-[#f0c060]/40 text-[#f0c060] text-xs font-mono">
              <span className="material-symbols-outlined text-[14px] mr-1">info</span>
              {mediaData.message}
            </div>
          )}
        </div>

        {/* 3. Scope & Format Selectors */}
        <div className="flex flex-col gap-3 pt-2 border-t border-[#1E3640]">
          {/* Scope (Todas vs Solo esta) - only shown when there are multiple photos */}
          {isMultiPhoto && (
            <div className="flex flex-col gap-1.5">
              <span className="text-xs font-mono text-[#bcc9c7] uppercase tracking-wider">
                Ámbito de descarga:
              </span>
              <div className="grid grid-cols-2 gap-2 bg-[#090f12] p-1 rounded-xl border border-[#1E3640]">
                <button
                  type="button"
                  onClick={() => setScope('all')}
                  className={`flex items-center justify-center gap-2 py-2 px-3 rounded-lg text-xs font-semibold transition-all ${
                    scope === 'all'
                      ? 'bg-[#252b2e] border border-[#5fd0c8] text-[#7dece4] shadow-sm'
                      : 'text-[#bcc9c7] hover:text-[#dee3e7]'
                  }`}
                >
                  <span className="material-symbols-outlined text-[16px]">collections</span>
                  <span>Todas ({totalPhotos})</span>
                </button>
                <button
                  type="button"
                  onClick={() => setScope('single')}
                  className={`flex items-center justify-center gap-2 py-2 px-3 rounded-lg text-xs font-semibold transition-all ${
                    scope === 'single'
                      ? 'bg-[#252b2e] border border-[#5fd0c8] text-[#7dece4] shadow-sm'
                      : 'text-[#bcc9c7] hover:text-[#dee3e7]'
                  }`}
                >
                  <span className="material-symbols-outlined text-[16px]">crop_original</span>
                  <span>Solo esta (#{photoIndex + 1})</span>
                </button>
              </div>
            </div>
          )}

          {/* Format (JPG vs PNG) */}
          <div className="flex flex-col gap-1.5">
            <span className="text-xs font-mono text-[#bcc9c7] uppercase tracking-wider">
              Formato:
            </span>
            <div className="grid grid-cols-2 gap-2">
              <button
                type="button"
                onClick={() => setFormat('JPG')}
                className={`flex items-center justify-between p-3 rounded-xl text-left transition-all ${
                  format === 'JPG'
                    ? 'bg-[#252b2e] border-2 border-[#5fd0c8] shadow-[0_0_12px_rgba(95,208,200,0.2)]'
                    : 'bg-[#171c20] border border-[#1E3640] hover:bg-[#252b2e]'
                }`}
              >
                <div className="flex items-center gap-2">
                  <span className={`text-sm font-bold ${format === 'JPG' ? 'text-[#7dece4]' : 'text-[#dee3e7]'}`}>
                    JPG
                  </span>
                  <span className="text-xs font-mono text-[#bcc9c7]">(3.2 MB)</span>
                </div>
                {format === 'JPG' && (
                  <span
                    className="material-symbols-outlined text-[#7dece4] text-[18px]"
                    style={{ fontVariationSettings: "'FILL' 1" }}
                  >
                    check_circle
                  </span>
                )}
              </button>

              <button
                type="button"
                onClick={() => setFormat('PNG')}
                className={`flex items-center justify-between p-3 rounded-xl text-left transition-all ${
                  format === 'PNG'
                    ? 'bg-[#252b2e] border-2 border-[#5fd0c8] shadow-[0_0_12px_rgba(95,208,200,0.2)]'
                    : 'bg-[#171c20] border border-[#1E3640] hover:bg-[#252b2e]'
                }`}
              >
                <div className="flex items-center gap-2">
                  <span className={`text-sm font-bold ${format === 'PNG' ? 'text-[#7dece4]' : 'text-[#dee3e7]'}`}>
                    PNG
                  </span>
                  <span className="text-xs font-mono text-[#bcc9c7]">(9.8 MB)</span>
                </div>
                {format === 'PNG' && (
                  <span
                    className="material-symbols-outlined text-[#7dece4] text-[18px]"
                    style={{ fontVariationSettings: "'FILL' 1" }}
                  >
                    check_circle
                  </span>
                )}
              </button>
            </div>
          </div>
        </div>

        {/* Primary CTA */}
        <div className="pt-2 flex flex-col gap-3">
          <button
            onClick={handleDownload}
            className="w-full h-14 rounded-2xl bg-[#5fd0c8] hover:bg-[#7FE0D8] active:bg-[#46B8B0] text-[#041014] font-bold text-base flex items-center justify-center gap-2.5 shadow-[0_0_20px_rgba(95,208,200,0.35)] active:scale-[0.99] transition-all cursor-pointer"
            type="button"
          >
            <span className="material-symbols-outlined text-[24px]">download</span>
            <span>{buttonLabel}</span>
          </button>

          <p className="text-center text-xs font-mono text-[#869391] leading-relaxed">
            Descarga solo contenido que tengas derecho a usar.
          </p>
        </div>
      </div>
    </div>
  );
}
