import React, { useState } from 'react';
import { InstallBanner } from './InstallPrompt';

export default function HomeView({
  urlInput,
  setUrlInput,
  onAnalyzeUrl,
  recentDownloads = [],
  onRedownload,
  showInstallBanner = false,
  onInstall,
  onDismissInstall,
}) {
  const [pasteNotice, setPasteNotice] = useState('');

  const handlePaste = async () => {
    try {
      if (navigator.clipboard && navigator.clipboard.readText) {
        const text = await navigator.clipboard.readText();
        if (text) {
          const clean = text.trim();
          setUrlInput(clean);
          setPasteNotice('¡Enlace pegado!');
          setTimeout(() => setPasteNotice(''), 2000);
          return;
        }
      }
    } catch {
      // Permission denied or unhandled
    }
    setPasteNotice('Usa Ctrl+V o mantén presionado para pegar');
    setTimeout(() => setPasteNotice(''), 2500);
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    if (urlInput.trim()) {
      onAnalyzeUrl(urlInput.trim());
    }
  };

  return (
    <div className="w-full flex flex-col items-center justify-center py-6 sm:py-12">
      <div className="w-full max-w-[680px] flex flex-col items-center text-center">
        {/* Banner de instalación en teléfono */}
        {showInstallBanner && (
          <InstallBanner onInstall={onInstall} onDismiss={onDismissInstall} />
        )}

        {/* Glow effect backdrop */}
        <div className="relative flex flex-col items-center mb-6 w-full">
          <div className="absolute -top-10 w-72 h-72 rounded-full bg-[#5fd0c8]/10 blur-3xl pointer-events-none -z-10"></div>

          {/* Tag retro terminal */}
          <div className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full bg-[#1b2024] border border-[#1E3640] text-[#5fd0c8] font-mono text-xs mb-3 shadow-[0_0_12px_rgba(95,208,200,0.15)]">
            <span className="w-1.5 h-1.5 rounded-full bg-[#5fd0c8] animate-pulse"></span>
            <span>MOTOR DE DETECCIÓN INTELIGENTE V1.2</span>
          </div>

          <h1 className="text-[#dee3e7] text-3xl sm:text-4xl lg:text-[40px] font-bold tracking-tight mb-3 text-balance leading-tight">
            Pega un enlace para descargar
          </h1>
          <p className="text-[#b8c9d2] text-base sm:text-lg max-w-lg leading-relaxed">
            Descarga videos, fotos y audios en alta calidad directamente a tu equipo.
          </p>
        </div>

        {/* Input Container */}
        <form onSubmit={handleSubmit} className="w-full flex flex-col gap-3 mb-6">
          <div className="relative w-full h-14 bg-[#171c20] border border-[#1E3640] rounded-2xl flex items-center px-4 shadow-[0_4px_24px_rgba(0,0,0,0.45)] transition-all duration-300 focus-within:border-[#5fd0c8] focus-within:shadow-[0_0_24px_rgba(95,208,200,0.25)]">
            <div className="flex items-center justify-center text-[#5fd0c8] mr-3 shrink-0">
              <span className="material-symbols-outlined text-[24px]">link</span>
            </div>

            <input
              aria-label="Enlace de descarga"
              className="w-full bg-transparent border-0 outline-none text-[#dee3e7] placeholder:text-[#869391] font-mono text-sm sm:text-base min-w-0 pr-2"
              id="url-input"
              value={urlInput}
              onChange={(e) => setUrlInput(e.target.value)}
              placeholder="Pega tu enlace aquí (TikTok, Instagram, YouTube, SoundCloud...)"
              type="text"
            />

            {urlInput && (
              <button
                type="button"
                onClick={() => setUrlInput('')}
                className="w-8 h-8 rounded-lg flex items-center justify-center text-[#869391] hover:text-[#dee3e7] mr-1 shrink-0 transition-colors"
                title="Limpiar campo"
              >
                <span className="material-symbols-outlined text-[18px]">close</span>
              </button>
            )}

            <button
              onClick={handlePaste}
              className="h-10 px-3.5 rounded-xl bg-[#1b2024] border border-[#3d4948]/60 text-[#dee3e7] hover:text-[#5fd0c8] hover:border-[#5fd0c8] hover:bg-[#252b2e] transition-all flex items-center gap-1.5 shrink-0 shadow-sm text-sm font-medium"
              id="paste-btn"
              type="button"
            >
              <span className="material-symbols-outlined text-[18px]">content_paste</span>
              <span>Pegar</span>
            </button>
          </div>

          {/* Feedback badge for paste */}
          {pasteNotice && (
            <div className="text-xs font-mono text-[#5fd0c8] flex items-center justify-center gap-1 animate-fade-in">
              <span className="material-symbols-outlined text-[14px]">check</span>
              <span>{pasteNotice}</span>
            </div>
          )}

          {/* Primary CTA Descargar */}
          <button
            className="w-full h-[52px] rounded-xl bg-[#5fd0c8] hover:bg-[#7FE0D8] active:bg-[#46B8B0] text-[#041014] font-semibold text-base flex items-center justify-center gap-2 shadow-[0_0_18px_rgba(95,208,200,0.3)] hover:shadow-[0_0_26px_rgba(95,208,200,0.5)] active:scale-[0.99] transition-all duration-200 cursor-pointer"
            id="download-btn"
            type="submit"
          >
            <span className="material-symbols-outlined text-[22px]">download</span>
            <span>Descargar</span>
          </button>
        </form>



        {/* Supported Platforms */}
        <div className="w-full flex flex-col items-center gap-3 mb-8">
          <span className="text-[#869391] text-xs font-mono uppercase tracking-wider">
            Plataformas compatibles:
          </span>
          <div className="flex flex-wrap items-center justify-center gap-2 max-w-xl">
            {['TikTok', 'Instagram', 'YouTube', 'SoundCloud', 'X / Twitter', 'Facebook', 'Threads'].map(
              (p) => (
                <div
                  key={p}
                  className="px-3 py-1.5 rounded-xl bg-[#1b2024] border border-[#1E3640]/70 text-[#dee3e7] text-xs font-mono flex items-center gap-1.5 shadow-sm hover:border-[#5fd0c8]/50 transition-colors"
                >
                  <span className="w-1.5 h-1.5 rounded-full bg-[#5fd0c8]"></span>
                  <span>{p}</span>
                </div>
              )
            )}
          </div>
        </div>

        {/* Quick History Strip (if any) */}
        {recentDownloads.length > 0 && (
          <div className="w-full flex flex-col gap-3 text-left bg-[#0D171B] border border-[#1E3640] rounded-2xl p-4 sm:p-5 mb-6">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2 text-sm font-semibold text-[#dee3e7]">
                <span className="material-symbols-outlined text-[#5fd0c8] text-[20px]">history</span>
                <span>Últimas descargas en este navegador</span>
              </div>
              <span className="text-xs font-mono text-[#7FA3A6]">
                {recentDownloads.length} {recentDownloads.length === 1 ? 'archivo' : 'archivos'}
              </span>
            </div>

            <div className="grid grid-cols-1 sm:grid-cols-2 gap-2.5 pt-1">
              {recentDownloads.slice(0, 4).map((item, idx) => {
                const itemTitle = item.title || item.label || item.name || "Archivo multimedia";
                const itemFormat = item.format || (item.kind ? item.kind.toUpperCase() : "ARCHIVO");
                const itemSize = item.size || (item.ok ? `${item.ok} archivo(s)` : "");
                const itemSubtitle = [itemFormat, itemSize].filter(Boolean).join(" • ");
                const directFile = item.files && item.files[0];
                const kindIcon = item.kind === "audio" ? "audiotrack" : item.kind === "image" ? "image" : "smart_display";

                return (
                  <div
                    key={item.id || item.ts || idx}
                    className="p-2.5 rounded-xl bg-[#171c20] hover:bg-[#1b2024] border border-[#1E3640]/70 flex items-center justify-between gap-3 transition-colors group"
                  >
                    <div className="flex items-center gap-2.5 min-w-0 flex-1">
                      <div className="w-10 h-10 rounded-lg overflow-hidden bg-[#090f12] border border-[#1E3640]/60 shrink-0 flex items-center justify-center relative">
                        {item.thumbnail ? (
                          <img
                            src={item.thumbnail}
                            alt={itemTitle}
                            className="w-full h-full object-cover"
                            onError={(e) => {
                              e.currentTarget.style.display = "none";
                              const fb = e.currentTarget.parentElement && e.currentTarget.parentElement.querySelector(".thumb-fallback");
                              if (fb) fb.style.display = "flex";
                            }}
                          />
                        ) : null}
                        <div
                          className={`thumb-fallback w-full h-full items-center justify-center text-[#5fd0c8] bg-[#252b2e] ${
                            item.thumbnail ? "hidden" : "flex"
                          }`}
                        >
                          <span className="material-symbols-outlined text-[20px]">{kindIcon}</span>
                        </div>
                      </div>

                      <div className="min-w-0 flex flex-col flex-1">
                        <span
                          className="text-xs font-semibold text-[#dee3e7] truncate group-hover:text-[#5fd0c8] transition-colors"
                          title={itemTitle}
                        >
                          {itemTitle}
                        </span>
                        <span className="text-[11px] font-mono text-[#7FA3A6] truncate">
                          {itemSubtitle}
                        </span>
                      </div>
                    </div>

                    {directFile && directFile.url ? (
                      <a
                        href={`${directFile.url}${directFile.url.includes('?') ? '&' : '?'}download=1&name=${encodeURIComponent(directFile.name || itemTitle)}`}
                        download={directFile.name || itemTitle}
                        className="w-8 h-8 rounded-lg bg-[#252b2e] hover:bg-[#5fd0c8] hover:text-[#041014] text-[#dee3e7] flex items-center justify-center shrink-0 transition-colors"
                        title={`Descargar ${directFile.name || itemTitle}`}
                        onClick={(e) => e.stopPropagation()}
                      >
                        <span className="material-symbols-outlined text-[16px]">download</span>
                      </a>
                    ) : (
                      <button
                        onClick={() => onRedownload(item)}
                        className="w-8 h-8 rounded-lg bg-[#252b2e] hover:bg-[#5fd0c8] hover:text-[#041014] text-[#dee3e7] flex items-center justify-center shrink-0 transition-colors"
                        title="Volver a procesar"
                        type="button"
                      >
                        <span className="material-symbols-outlined text-[16px]">download</span>
                      </button>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {/* Legal Disclaimer Footer */}
        <div className="pt-2 w-full text-center">
          <p className="text-[#869391] text-xs font-mono max-w-lg mx-auto opacity-80 leading-relaxed">
            Descarga solo contenido que tengas derecho a usar. Muxivo no almacena archivos en servidores.
          </p>
        </div>
      </div>
    </div>
  );
}
