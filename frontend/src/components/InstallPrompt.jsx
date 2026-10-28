import React from 'react';

export function InstallBanner({ onInstall, onDismiss }) {
  return (
    <div className="w-full mb-6 p-3.5 sm:p-4 rounded-2xl bg-gradient-to-r from-[#11181c] via-[#162329] to-[#11181c] border border-[#1E3640] shadow-[0_8px_28px_rgba(0,0,0,0.55)] flex items-center justify-between gap-3 animate-fade-in relative overflow-hidden group">
      {/* Glow accent */}
      <div className="absolute -left-6 -top-6 w-24 h-24 rounded-full bg-[#5fd0c8]/10 blur-xl pointer-events-none"></div>

      <div className="flex items-center gap-3 min-w-0 z-10">
        <div className="w-10 h-10 sm:w-11 sm:h-11 rounded-xl bg-[#0d101a] border border-[#3d4948]/70 flex items-center justify-center shrink-0 overflow-hidden shadow-[0_0_12px_rgba(95,208,200,0.25)]">
          <img src="/icon.png" alt="Muxivo Logo" className="w-full h-full object-cover" />
        </div>
        <div className="flex flex-col min-w-0 text-left">
          <div className="flex items-center gap-1.5">
            <span className="text-sm font-bold text-[#dee3e7] truncate">
              Instalar Muxivo
            </span>
            <span className="hidden sm:inline-block px-1.5 py-0.5 rounded text-[10px] font-mono bg-[#1E3640] text-[#5fd0c8]">
              PWA
            </span>
          </div>
          <span className="text-xs text-[#869391] truncate font-mono">
            Úsala como app nativa en tu teléfono
          </span>
        </div>
      </div>

      <div className="flex items-center gap-2 shrink-0 z-10">
        <button
          onClick={onInstall}
          className="px-3.5 py-2 rounded-xl bg-[#5fd0c8] hover:bg-[#7dece4] active:scale-95 text-[#041014] text-xs font-bold font-mono tracking-wide shadow-[0_0_14px_rgba(95,208,200,0.4)] transition-all flex items-center gap-1.5"
          id="install-now-btn"
        >
          <span className="material-symbols-outlined text-[16px]">install_mobile</span>
          <span>Instalar ahora</span>
        </button>

        <button
          onClick={onDismiss}
          className="w-8 h-8 rounded-lg text-[#869391] hover:text-[#dee3e7] hover:bg-[#1E3640]/50 flex items-center justify-center transition-colors"
          title="Ocultar"
          aria-label="Cerrar sugerencia de instalación"
        >
          <span className="material-symbols-outlined text-[18px]">close</span>
        </button>
      </div>
    </div>
  );
}

export function InstallModal({ isOpen, onClose, isIOS }) {
  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center px-4 bg-black/80 backdrop-blur-sm animate-fade-in">
      <div className="w-full max-w-sm rounded-2xl bg-[#141b20] border border-[#1E3640] p-5 shadow-2xl flex flex-col gap-4 text-left relative">
        {/* Header modal */}
        <div className="flex items-center justify-between pb-3 border-b border-[#1E3640]">
          <div className="flex items-center gap-2.5">
            <div className="w-9 h-9 rounded-lg bg-[#0d101a] border border-[#3d4948]/70 overflow-hidden flex items-center justify-center">
              <img src="/icon.png" alt="Muxivo" className="w-full h-full object-cover" />
            </div>
            <div>
              <h3 className="text-base font-bold text-[#dee3e7]">Instalar Muxivo</h3>
              <p className="text-xs text-[#869391] font-mono">Acceso directo en tu teléfono</p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="w-7 h-7 rounded-lg text-[#869391] hover:text-[#dee3e7] flex items-center justify-center"
          >
            <span className="material-symbols-outlined text-[18px]">close</span>
          </button>
        </div>

        {/* Content steps */}
        {isIOS ? (
          <div className="flex flex-col gap-3 text-xs text-[#bcc9c7] font-sans">
            <p className="text-xs text-[#dee3e7] font-medium">
              Sigue estos pasos en Safari para instalar Muxivo en tu iPhone o iPad:
            </p>
            <div className="flex items-start gap-3 p-2.5 rounded-xl bg-[#171c20] border border-[#1E3640]/80">
              <span className="w-6 h-6 rounded-full bg-[#252b2e] text-[#5fd0c8] font-mono font-bold flex items-center justify-center shrink-0 text-xs">
                1
              </span>
              <div className="flex-1">
                Toca el botón <span className="font-semibold text-white">Compartir</span> en la barra inferior de Safari (ícono con flecha hacia arriba).
              </div>
            </div>
            <div className="flex items-start gap-3 p-2.5 rounded-xl bg-[#171c20] border border-[#1E3640]/80">
              <span className="w-6 h-6 rounded-full bg-[#252b2e] text-[#5fd0c8] font-mono font-bold flex items-center justify-center shrink-0 text-xs">
                2
              </span>
              <div className="flex-1">
                Desplázate hacia abajo y pulsa <span className="font-semibold text-white">"Añadir a pantalla de inicio"</span>.
              </div>
            </div>
            <div className="flex items-start gap-3 p-2.5 rounded-xl bg-[#171c20] border border-[#1E3640]/80">
              <span className="w-6 h-6 rounded-full bg-[#252b2e] text-[#5fd0c8] font-mono font-bold flex items-center justify-center shrink-0 text-xs">
                3
              </span>
              <div className="flex-1">
                Confirma arriba a la derecha pulsando <span className="font-semibold text-white">"Añadir"</span>.
              </div>
            </div>
          </div>
        ) : (
          <div className="flex flex-col gap-3 text-xs text-[#bcc9c7] font-sans">
            <p className="text-xs text-[#dee3e7] font-medium">
              Para instalar Muxivo manualmente en tu navegador móvil:
            </p>
            <div className="flex items-start gap-3 p-2.5 rounded-xl bg-[#171c20] border border-[#1E3640]/80">
              <span className="w-6 h-6 rounded-full bg-[#252b2e] text-[#5fd0c8] font-mono font-bold flex items-center justify-center shrink-0 text-xs">
                1
              </span>
              <div className="flex-1">
                Abre el menú de opciones de tu navegador (los tres puntos <span className="font-semibold text-white">⋮</span>).
              </div>
            </div>
            <div className="flex items-start gap-3 p-2.5 rounded-xl bg-[#171c20] border border-[#1E3640]/80">
              <span className="w-6 h-6 rounded-full bg-[#252b2e] text-[#5fd0c8] font-mono font-bold flex items-center justify-center shrink-0 text-xs">
                2
              </span>
              <div className="flex-1">
                Selecciona <span className="font-semibold text-white">"Instalar aplicación"</span> o <span className="font-semibold text-white">"Añadir a la pantalla principal"</span>.
              </div>
            </div>
          </div>
        )}

        <button
          onClick={onClose}
          className="w-full py-2.5 rounded-xl bg-[#252b2e] hover:bg-[#30383c] text-[#5fd0c8] font-mono text-xs font-semibold tracking-wide border border-[#5fd0c8]/30 transition-all mt-1"
        >
          Entendido
        </button>
      </div>
    </div>
  );
}
