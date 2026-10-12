import React, { useState } from 'react';

export default function ErrorView({ errorData, onRetry, onPasteAnother, onClear }) {
  const [retrying, setRetrying] = useState(false);
  const [toast, setToast] = useState('');

  const triggerToast = (msg) => {
    setToast(msg);
    setTimeout(() => setToast(''), 2600);
  };

  const handleRetry = () => {
    setRetrying(true);
    triggerToast('Reconectando al servidor...');
    setTimeout(() => {
      setRetrying(false);
      triggerToast('Enlace aún no disponible. Prueba con otro enlace público.');
    }, 1400);
  };

  return (
    <div className="max-w-[480px] w-full mx-auto pb-24 md:pb-12 flex flex-col gap-4 animate-fade-in">
      {/* 1. Status Terminal Pill */}
      <div className="flex items-center justify-between px-3.5 py-2.5 bg-[#171c20] border border-[#ffb4ab]/30 rounded-xl shadow-sm">
        <div className="flex items-center gap-2">
          <span className="inline-block w-2 h-2 rounded-full bg-[#ffb4ab] animate-pulse shadow-[0_0_8px_rgba(255,180,171,0.8)]"></span>
          <span className="font-mono text-xs text-[#ffb4ab] uppercase tracking-wider font-semibold">
            ERROR // {errorData?.errorCode || '0x404_INVALID_NODE'}
          </span>
        </div>
        <span className="font-mono text-xs text-[#869391]">PARSER: OFF</span>
      </div>

      {/* 2. Faulty Input Field Display */}
      <div className="relative w-full">
        <div className="bg-[#090f12] border border-[#93000a] rounded-xl p-3.5 shadow-[0_0_18px_rgba(147,0,10,0.35)] flex items-center justify-between gap-2.5">
          <div className="flex items-center gap-2.5 min-w-0 flex-1">
            <span className="material-symbols-outlined text-[#ffb4ab] text-[20px] shrink-0">link_off</span>
            <div className="min-w-0 flex-1">
              <p className="font-mono text-[10px] text-[#ffb4ab]/90 uppercase tracking-widest leading-none mb-1">
                Entrada detectada
              </p>
              <p className="font-mono text-xs text-[#dee3e7] truncate select-all">
                {errorData?.url || 'https://social.stream/p/private_reel_x8849?auth=null'}
              </p>
            </div>
          </div>
          <button
            aria-label="Limpiar entrada"
            onClick={onClear}
            className="w-8 h-8 rounded-lg bg-[#1b2024] hover:bg-[#252b2e] flex items-center justify-center text-[#bcc9c7] hover:text-[#dee3e7] transition-colors shrink-0"
            type="button"
          >
            <span className="material-symbols-outlined text-[18px]">close</span>
          </button>
        </div>
      </div>

      {/* 3. Main Error Card with Retro Hologram Radar */}
      <div className="relative w-full bg-[#171c20] border border-[#1E3640] rounded-2xl p-6 shadow-xl flex flex-col items-center text-center overflow-hidden">
        {/* Ambient CRT glow backdrop */}
        <div className="absolute -top-12 left-1/2 -translate-x-1/2 w-48 h-48 rounded-full bg-[#93000a]/20 blur-3xl pointer-events-none"></div>

        {/* Broken Signal Radar */}
        <div className="relative mt-2 mb-4 flex items-center justify-center">
          <div className="absolute w-24 h-24 rounded-full bg-[#93000a]/30 animate-ping opacity-25"></div>
          <div className="w-20 h-20 rounded-full bg-[#252b2e] border border-[#93000a]/50 flex items-center justify-center shadow-[0_0_24px_rgba(255,92,108,0.25)] relative z-10">
            <div className="w-14 h-14 rounded-full bg-[#93000a]/40 flex items-center justify-center text-[#ffb4ab]">
              <span className="material-symbols-outlined text-[32px] drop-shadow-[0_0_10px_rgba(255,180,171,0.6)]">
                warning
              </span>
            </div>
          </div>
          <span className="absolute -bottom-1 -right-1 z-20 w-6 h-6 rounded-full bg-[#090f12] border border-[#93000a] flex items-center justify-center text-[#ffb4ab] shadow-md">
            <span className="material-symbols-outlined text-[16px]">sync_problem</span>
          </span>
        </div>

        {/* Error Titles & Copy */}
        <h1 className="text-2xl text-[#dee3e7] font-bold mb-2 tracking-tight">
          Enlace no válido
        </h1>
        <p className="text-sm text-[#dee3e7] font-medium leading-relaxed max-w-[290px] mb-6">
          {errorData?.message || 'No pudimos leer este enlace. Revisa que sea público.'}
        </p>

        {/* Diagnostics Terminal Checklist */}
        <div className="w-full bg-[#1b2024] border border-[#1E3640] rounded-xl p-4 text-left shadow-inner">
          <div className="flex items-center gap-2 mb-3">
            <span className="material-symbols-outlined text-[#7dece4] text-[16px]">checklist_rtl</span>
            <span className="font-mono text-xs text-[#7dece4] uppercase tracking-widest font-semibold">
              Diagnóstico de acceso
            </span>
          </div>
          <ul className="flex flex-col gap-2.5">
            <li className="flex items-start gap-2.5">
              <div className="w-4 h-4 rounded-full bg-[#252b2e] flex items-center justify-center mt-0.5 shrink-0 text-[#7dece4]">
                <span className="material-symbols-outlined text-[12px]">lock_open</span>
              </div>
              <span className="text-xs text-[#dee3e7]">¿El perfil o publicación es privada?</span>
            </li>
            <li className="flex items-start gap-2.5">
              <div className="w-4 h-4 rounded-full bg-[#252b2e] flex items-center justify-center mt-0.5 shrink-0 text-[#7dece4]">
                <span className="material-symbols-outlined text-[12px]">visibility</span>
              </div>
              <span className="text-xs text-[#dee3e7]">¿El contenido sigue disponible en la red social?</span>
            </li>
            <li className="flex items-start gap-2.5">
              <div className="w-4 h-4 rounded-full bg-[#252b2e] flex items-center justify-center mt-0.5 shrink-0 text-[#7dece4]">
                <span className="material-symbols-outlined text-[12px]">spellcheck</span>
              </div>
              <span className="text-xs text-[#dee3e7]">Verifica que la URL esté completa y bien copiada</span>
            </li>
          </ul>
        </div>
      </div>

      {/* 4. Interactive Action Controls */}
      <div className="w-full flex flex-col gap-3">
        {/* Primary CTA: Reintentar */}
        <button
          onClick={handleRetry}
          disabled={retrying}
          className="w-full h-14 bg-[#5fd0c8] hover:bg-[#7FE0D8] active:bg-[#46B8B0] text-[#041014] font-bold text-base rounded-2xl shadow-[0_0_20px_rgba(95,208,200,0.32)] active:scale-[0.98] transition-all flex items-center justify-center gap-2.5 cursor-pointer disabled:opacity-60"
          type="button"
        >
          <span className={`material-symbols-outlined text-[24px] ${retrying ? 'animate-spin' : ''}`}>
            refresh
          </span>
          <span>{retrying ? 'Verificando...' : 'Reintentar'}</span>
        </button>

        {/* Secondary CTA: Pegar otro enlace */}
        <button
          onClick={onPasteAnother}
          className="w-full h-12 bg-[#252b2e] hover:bg-[#303639] border border-[#3d4948]/60 text-[#dee3e7] font-semibold text-sm rounded-xl shadow-sm active:scale-[0.99] transition-all flex items-center justify-center gap-2 cursor-pointer"
          type="button"
        >
          <span className="material-symbols-outlined text-[20px] text-[#7dece4]">content_paste</span>
          <span>Pegar otro enlace</span>
        </button>
      </div>

      {/* Usage disclaimer */}
      <div className="mt-2 px-3 py-3 rounded-xl bg-[#090f12] border border-[#1E3640] flex items-center gap-3">
        <span className="material-symbols-outlined text-[18px] text-[#7FA3A6] shrink-0">verified_user</span>
        <p className="text-xs font-mono text-[#869391] leading-snug">
          Descarga solo contenido que tengas derecho a usar. Muxivo no almacena archivos en servidores.
        </p>
      </div>

      {/* Toast Feedback */}
      {toast && (
        <div className="fixed bottom-20 left-4 right-4 max-w-[360px] mx-auto bg-[#303639] text-[#dee3e7] px-4 py-3 rounded-xl shadow-2xl flex items-center gap-3 border border-[#ffb4ab]/40 z-50 animate-fade-in">
          <span className="material-symbols-outlined text-[#ffb4ab] text-[20px]">sync</span>
          <span className="text-xs font-mono">{toast}</span>
        </div>
      )}
    </div>
  );
}
