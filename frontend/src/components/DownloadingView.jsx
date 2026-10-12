import React, { useState, useEffect, useRef } from 'react';
import { startDownload } from '../utils/api';

export default function DownloadingView({ downloadItem, onCancel, onComplete }) {
  const [progress, setProgress] = useState(0);
  const [statusText, setStatusText] = useState('Iniciando descarga...');
  const [isCancelling, setIsCancelling] = useState(false);
  const [error, setError] = useState(null);
  const didStart = useRef(false);

  useEffect(() => {
    if (didStart.current) return;
    didStart.current = true;

    let cancelled = false;

    // Animate progress while waiting for the real download
    const fakeProgressInterval = setInterval(() => {
      setProgress((prev) => {
        if (prev >= 85) return prev; // Stop at 85% — wait for real response
        const inc = Math.floor(Math.random() * 6) + 3;
        return Math.min(prev + inc, 85);
      });
    }, 400);

    const updateStatus = (pct) => {
      if (pct < 20) return 'Conectando con el servidor...';
      if (pct < 50) return 'Extrayendo flujo de datos...';
      if (pct < 80) return 'Descargando y convirtiendo...';
      return 'Finalizando y empaquetando...';
    };

    const statusInterval = setInterval(() => {
      setProgress((prev) => {
        setStatusText(updateStatus(prev));
        return prev;
      });
    }, 600);

    // Real download call
    startDownload({
      rawInput: downloadItem.url,
      kind: downloadItem.kind || downloadItem.type || 'video',
      format: downloadItem.format_id || '',
      detail: downloadItem.detail || '',
      sessionId: downloadItem.sessionId || '',
      imageUrls: downloadItem.imageUrls || undefined,
    })
      .then((result) => {
        if (cancelled) return;
        clearInterval(fakeProgressInterval);
        clearInterval(statusInterval);

        // Animate to 100%
        setProgress(100);
        setStatusText('¡Descarga completada!');

        setTimeout(() => {
          if (!cancelled) {
            onComplete({
              ...downloadItem,
              ...result,
            });
          }
        }, 600);
      })
      .catch((err) => {
        if (cancelled) return;
        clearInterval(fakeProgressInterval);
        clearInterval(statusInterval);
        setError(err.message || 'Error en la descarga.');
        setProgress(0);
        setStatusText('Error en la descarga');
      });

    return () => {
      cancelled = true;
      clearInterval(fakeProgressInterval);
      clearInterval(statusInterval);
    };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const handleCancel = () => {
    setIsCancelling(true);
    setTimeout(() => {
      onCancel();
    }, 800);
  };

  return (
    <div className="max-w-[480px] w-full mx-auto pb-24 md:pb-12 flex flex-col gap-4 animate-fade-in">
      {/* 1. Top active stream status card */}
      <div className="bg-[#171c20] border border-[#1E3640] rounded-xl p-3.5 shadow-md flex items-center justify-between gap-3">
        <div className="flex items-center gap-2.5 min-w-0">
          <div className={`w-2.5 h-2.5 rounded-full shrink-0 ${error ? 'bg-[#ff808b]' : 'bg-[#5fd0c8] shadow-[0_0_8px_rgba(95,208,200,0.8)] animate-pulse'}`}></div>
          <div className="flex flex-col min-w-0">
            <span className="font-mono text-xs text-[#7dece4] tracking-wider uppercase font-semibold">
              {error ? 'Error de descarga' : 'Extrayendo flujo de datos'}
            </span>
            <span className="font-mono text-[11px] text-[#bcc9c7] truncate">
              {downloadItem?.url || 'muxivo.network/buffer'}
            </span>
          </div>
        </div>
        <span className={`material-symbols-outlined text-[20px] shrink-0 ${error ? 'text-[#ff808b]' : 'text-[#5fd0c8] animate-pulse'}`}>
          {error ? 'error' : 'sensors'}
        </span>
      </div>

      {/* 2. Media Metadata Card */}
      <div className="bg-[#1b2024] border border-[#1E3640] rounded-xl p-4 shadow-md flex items-center gap-3.5">
        <div className="relative w-16 h-16 rounded-lg overflow-hidden shrink-0 bg-[#090f12] shadow-inner border border-[#1E3640]/50">
          {downloadItem?.thumbnail ? (
            <img src={downloadItem.thumbnail} alt={downloadItem.title} className="w-full h-full object-cover" />
          ) : (
            <div className="w-full h-full flex items-center justify-center">
              <span className="material-symbols-outlined text-[28px] text-[#5fd0c8]/40">
                {downloadItem?.type === 'audio' ? 'music_note' : downloadItem?.type === 'image' ? 'image' : 'smart_display'}
              </span>
            </div>
          )}
          <div className="absolute inset-0 bg-gradient-to-t from-[#090f12]/80 via-transparent to-transparent"></div>
          <div className="absolute bottom-1 right-1 w-5 h-5 rounded-full bg-[#090f12]/90 flex items-center justify-center text-[#5fd0c8]">
            <span className="material-symbols-outlined text-[13px]">
              {downloadItem?.type === 'audio' ? 'music_note' : downloadItem?.type === 'image' ? 'image' : 'smart_display'}
            </span>
          </div>
        </div>

        <div className="flex flex-col min-w-0 justify-center flex-1">
          <div className="flex items-center gap-2 mb-0.5">
            <span className="font-mono text-[11px] px-2 py-0.5 rounded-full bg-[#5fd0c8]/15 text-[#5fd0c8] font-semibold">
              {(downloadItem?.type || 'video').toUpperCase()}
            </span>
            <span className="font-mono text-[11px] text-[#869391] truncate">
              {downloadItem?.platform}
            </span>
          </div>
          <h2 className="text-sm font-bold text-[#dee3e7] truncate">
            {downloadItem?.title || downloadItem?.platform || 'Descargando...'}
          </h2>
          <p className="text-xs font-mono text-[#bcc9c7] truncate">
            {downloadItem?.format || downloadItem?.kind || ''}
          </p>
        </div>
      </div>

      {/* Error State */}
      {error && (
        <div className="bg-[#1b2024] border border-[#ff808b]/40 rounded-2xl p-6 shadow-xl flex flex-col items-center text-center gap-4">
          <div className="w-16 h-16 rounded-full bg-[#252b2e] border border-[#ff808b]/40 flex items-center justify-center shadow-[0_0_20px_rgba(255,128,139,0.2)]">
            <span className="material-symbols-outlined text-[32px] text-[#ff808b]">error</span>
          </div>
          <div>
            <p className="text-[#ff808b] font-semibold text-base mb-1">Error en la descarga</p>
            <p className="text-[#bcc9c7] text-sm font-mono leading-relaxed">{error}</p>
          </div>
          <button
            onClick={handleCancel}
            className="w-full h-12 rounded-xl bg-[#252b2e] hover:bg-[#303639] border border-[#1E3640] text-[#dee3e7] text-sm font-semibold flex items-center justify-center gap-2 transition-all cursor-pointer"
            type="button"
          >
            <span className="material-symbols-outlined text-[18px]">arrow_back</span>
            <span>Volver a intentar con otras opciones</span>
          </button>
        </div>
      )}

      {/* Progress Display (only when not error) */}
      {!error && (
        <>
          {/* 3. Progress Display Box with Phosphor Glow */}
          <div className="bg-[#1b2024] border border-[#1E3640] rounded-2xl p-6 shadow-xl flex flex-col items-center text-center relative overflow-hidden">
            {/* Glow halo */}
            <div className="absolute -top-12 left-1/2 -translate-x-1/2 w-48 h-24 bg-[#5fd0c8]/15 blur-2xl rounded-full pointer-events-none"></div>

            <div className="flex items-baseline justify-center gap-1.5 mb-1 relative">
              <span className="text-4xl sm:text-5xl font-mono text-[#dee3e7] font-bold tracking-tight drop-shadow-[0_0_12px_rgba(95,208,200,0.45)]">
                {progress}%
              </span>
              <span className="font-mono text-sm text-[#5fd0c8] font-semibold">
                {progress === 100 ? 'completado' : 'procesando'}
              </span>
            </div>

            <p className="text-xs text-[#bcc9c7] mb-5">{statusText}</p>

            {/* Custom Progress Bar */}
            <div className="w-full bg-[#090f12] h-2.5 rounded-full overflow-hidden relative shadow-inner border border-[#1E3640]/60">
              <div
                className="bg-[#5fd0c8] h-full rounded-full shadow-[0_0_14px_rgba(95,208,200,0.8)] transition-all duration-300 ease-out"
                style={{ width: `${progress}%` }}
              ></div>
            </div>

            {/* Live Status Info */}
            <div className="flex items-center justify-between w-full mt-4 px-1 text-xs font-mono text-[#bcc9c7]">
              <span className="flex items-center gap-1.5 text-[#dee3e7]">
                <span className="material-symbols-outlined text-[16px] text-[#5fd0c8]">data_saver_on</span>
                <span>{statusText}</span>
              </span>
              <span className="flex items-center gap-1 text-[#7dece4] font-medium">
                <span className="material-symbols-outlined text-[16px]">speed</span>
                <span>En progreso</span>
              </span>
            </div>
          </div>

          {/* 4. Conversion Highlights */}
          <div className="grid grid-cols-2 gap-2.5">
            <div className="bg-[#171c20] border border-[#1E3640] rounded-xl p-3 flex items-center gap-2.5">
              <div className="w-8 h-8 rounded-lg bg-[#252b2e] flex items-center justify-center text-[#5fd0c8] shrink-0">
                <span className="material-symbols-outlined text-[18px]">equalizer</span>
              </div>
              <div className="flex flex-col min-w-0">
                <span className="font-mono text-[10px] text-[#869391] uppercase">CALIDAD</span>
                <span className="text-xs text-[#dee3e7] font-semibold truncate">Original Máxima</span>
              </div>
            </div>
            <div className="bg-[#171c20] border border-[#1E3640] rounded-xl p-3 flex items-center gap-2.5">
              <div className="w-8 h-8 rounded-lg bg-[#252b2e] flex items-center justify-center text-[#5fd0c8] shrink-0">
                <span className="material-symbols-outlined text-[18px]">timer</span>
              </div>
              <div className="flex flex-col min-w-0">
                <span className="font-mono text-[10px] text-[#869391] uppercase">ESTADO</span>
                <span className="text-xs text-[#dee3e7] font-semibold truncate">
                  {progress < 85 ? 'Procesando...' : 'Casi listo...'}
                </span>
              </div>
            </div>
          </div>

          {/* 5. Cancel Action Button */}
          <div className="pt-2 flex flex-col gap-3">
            <button
              onClick={handleCancel}
              disabled={isCancelling}
              className="w-full h-13 py-3 rounded-xl bg-[#252b2e] hover:bg-[#303639] border border-[#1E3640] text-[#ff808b] active:text-[#ffb4ab] text-sm font-semibold shadow-md transition-all flex items-center justify-center gap-2 cursor-pointer active:scale-[0.99] disabled:opacity-50"
              type="button"
            >
              {isCancelling ? (
                <>
                  <span className="material-symbols-outlined text-[18px] animate-spin">refresh</span>
                  <span>Cancelando descarga...</span>
                </>
              ) : (
                <>
                  <span className="material-symbols-outlined text-[18px]">close</span>
                  <span>Cancelar descarga</span>
                </>
              )}
            </button>
            <p className="text-xs font-mono text-[#869391] text-center leading-relaxed">
              Descarga solo contenido que tengas derecho a usar.
            </p>
          </div>
        </>
      )}
    </div>
  );
}
