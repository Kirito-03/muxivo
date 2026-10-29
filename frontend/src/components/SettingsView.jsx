import React, { useState } from 'react';

export default function SettingsView({ onClearHistory, isStandalone = false, onInstall }) {
  const [defaultVideoQuality, setDefaultVideoQuality] = useState('1080p');
  const [defaultAudioFormat, setDefaultAudioFormat] = useState('MP3');
  const [autoPaste, setAutoPaste] = useState(true);
  const [clearing, setClearing] = useState(false);
  const [clearedNotice, setClearedNotice] = useState(false);

  const handleClear = () => {
    setClearing(true);
    setTimeout(() => {
      onClearHistory();
      setClearing(false);
      setClearedNotice(true);
      setTimeout(() => setClearedNotice(false), 2000);
    }, 700);
  };

  return (
    <div className="w-full max-w-[560px] mx-auto pb-24 md:pb-12 flex flex-col gap-6 text-left animate-fade-in">
      {/* Header */}
      <div className="flex flex-col gap-1">
        <div className="flex items-center gap-2 mb-0.5">
          <span className="inline-flex items-center justify-center w-7 h-7 rounded-lg bg-[#252b2e] border border-[#3d4948]/60 text-[#7dece4] shadow-[0_0_12px_rgba(95,208,200,0.2)]">
            <span className="material-symbols-outlined text-[18px]">tune</span>
          </span>
          <h1 className="text-2xl sm:text-3xl text-[#dee3e7] font-bold tracking-tight">
            Ajustes
          </h1>
        </div>
        <p className="text-sm text-[#bcc9c7]">
          Preferencias de descarga y almacenamiento local
        </p>
      </div>

      {/* 1. Sección: Descargas */}
      <section className="flex flex-col gap-2.5">
        <div className="flex items-center gap-1.5 px-1">
          <span className="material-symbols-outlined text-[#7dece4] text-[18px]">
            download_for_offline
          </span>
          <h2 className="text-xs font-mono font-bold text-[#7dece4] uppercase tracking-wider">
            Descargas
          </h2>
        </div>

        <div className="bg-[#171c20] border border-[#1E3640] rounded-2xl p-4 sm:p-5 flex flex-col gap-5 shadow-lg">
          {/* Calidad de video por defecto */}
          <div className="flex flex-col gap-2">
            <div>
              <span className="text-sm font-semibold text-[#dee3e7] block">
                Calidad de video por defecto
              </span>
              <span className="text-xs font-mono text-[#bcc9c7] block">
                Resolución preseleccionada al analizar videos
              </span>
            </div>

            <div className="grid grid-cols-3 gap-2 pt-1">
              {['1080p', '720p', '480p'].map((q) => {
                const isSelected = defaultVideoQuality === q;
                return (
                  <button
                    key={q}
                    type="button"
                    onClick={() => setDefaultVideoQuality(q)}
                    className={`flex items-center justify-center min-h-[46px] px-3 rounded-xl font-mono text-xs font-semibold transition-all ${
                      isSelected
                        ? 'bg-[#252b2e] border-2 border-[#5fd0c8] text-[#7dece4] shadow-[0_0_14px_rgba(95,208,200,0.25)]'
                        : 'bg-[#1b2024] border border-[#1E3640] text-[#bcc9c7] hover:text-[#dee3e7] hover:bg-[#252b2e]'
                    }`}
                  >
                    <span className="flex items-center gap-1.5">
                      {isSelected && (
                        <span className="w-1.5 h-1.5 rounded-full bg-[#5fd0c8] shadow-[0_0_6px_#5fd0c8]"></span>
                      )}
                      <span>{q}</span>
                    </span>
                  </button>
                );
              })}
            </div>
          </div>

          <div className="h-[1px] w-full bg-[#1E3640]"></div>

          {/* Formato de audio por defecto */}
          <div className="flex flex-col gap-2">
            <div>
              <span className="text-sm font-semibold text-[#dee3e7] block">
                Formato de audio por defecto
              </span>
              <span className="text-xs font-mono text-[#bcc9c7] block">
                Extracción automática para pistas de audio
              </span>
            </div>

            <div className="grid grid-cols-2 gap-2 pt-1">
              {['MP3', 'M4A'].map((fmt) => {
                const isSelected = defaultAudioFormat === fmt;
                return (
                  <button
                    key={fmt}
                    type="button"
                    onClick={() => setDefaultAudioFormat(fmt)}
                    className={`flex items-center justify-center min-h-[46px] px-4 rounded-xl font-mono text-xs font-semibold transition-all ${
                      isSelected
                        ? 'bg-[#252b2e] border-2 border-[#5fd0c8] text-[#7dece4] shadow-[0_0_14px_rgba(95,208,200,0.25)]'
                        : 'bg-[#1b2024] border border-[#1E3640] text-[#bcc9c7] hover:text-[#dee3e7] hover:bg-[#252b2e]'
                    }`}
                  >
                    <span className="flex items-center gap-1.5">
                      {isSelected && (
                        <span className="w-1.5 h-1.5 rounded-full bg-[#5fd0c8] shadow-[0_0_6px_#5fd0c8]"></span>
                      )}
                      <span>{fmt}</span>
                    </span>
                  </button>
                );
              })}
            </div>
          </div>
        </div>
      </section>

      {/* 2. Sección: Comodidad */}
      <section className="flex flex-col gap-2.5">
        <div className="flex items-center gap-1.5 px-1">
          <span className="material-symbols-outlined text-[#7dece4] text-[18px]">bolt</span>
          <h2 className="text-xs font-mono font-bold text-[#7dece4] uppercase tracking-wider">
            Comodidad
          </h2>
        </div>

        <div className="bg-[#171c20] border border-[#1E3640] rounded-2xl p-4 sm:p-5 shadow-lg">
          <label className="flex items-center justify-between gap-3 cursor-pointer" htmlFor="clipboard-toggle">
            <div className="flex items-start gap-3 flex-1 min-w-0 pr-1">
              <div className="w-9 h-9 rounded-xl bg-[#252b2e] border border-[#3d4948]/50 flex items-center justify-center text-[#7dece4] shrink-0 mt-0.5">
                <span className="material-symbols-outlined text-[20px]">content_paste</span>
              </div>
              <div className="flex flex-col">
                <span className="text-sm font-semibold text-[#dee3e7]">
                  Pegar enlace automáticamente
                </span>
                <span className="text-xs font-mono text-[#bcc9c7] pt-0.5">
                  Facilita pegar desde el portapapeles con un solo toque
                </span>
              </div>
            </div>

            {/* Retro Toggle Switch */}
            <div className="relative shrink-0 flex items-center justify-center p-1">
              <input
                id="clipboard-toggle"
                type="checkbox"
                checked={autoPaste}
                onChange={(e) => setAutoPaste(e.target.checked)}
                className="sr-only peer"
              />
              <div className="w-12 h-7 bg-[#252b2e] peer-checked:bg-[#5fd0c8] rounded-full transition-colors duration-200 border border-[#1E3640]"></div>
              <div className="absolute left-1.5 top-1.5 w-5 h-5 bg-[#090f12] peer-checked:bg-[#041014] rounded-full transition-all duration-200 peer-checked:translate-x-5 shadow-md flex items-center justify-center">
                <span className="w-1.5 h-1.5 rounded-full bg-[#5fd0c8]"></span>
              </div>
            </div>
          </label>
        </div>
      </section>

      {/* 3. Sección: Datos */}
      <section className="flex flex-col gap-2.5">
        <div className="flex items-center gap-1.5 px-1">
          <span className="material-symbols-outlined text-[#bcc9c7] text-[18px]">database</span>
          <h2 className="text-xs font-mono font-bold text-[#bcc9c7] uppercase tracking-wider">
            Datos locales
          </h2>
        </div>

        <div className="bg-[#171c20] border border-[#1E3640] rounded-2xl p-4 sm:p-5 shadow-lg flex items-center justify-between gap-3">
          <div className="flex items-center gap-3 min-w-0 flex-1">
            <div className="w-9 h-9 rounded-xl bg-[#93000a]/20 border border-[#93000a]/40 flex items-center justify-center text-[#ffb4ab] shrink-0">
              <span className="material-symbols-outlined text-[20px]">delete_sweep</span>
            </div>
            <div className="flex flex-col">
              <span className="text-sm font-semibold text-[#dee3e7]">Borrar historial</span>
              <span className="text-xs font-mono text-[#bcc9c7]">
                Elimina los registros locales guardados en localStorage
              </span>
            </div>
          </div>

          <button
            onClick={handleClear}
            disabled={clearing}
            type="button"
            className="px-3.5 py-2 rounded-xl bg-[#252b2e] hover:bg-[#93000a]/30 active:bg-[#93000a]/50 text-[#ffb4ab] font-mono text-xs font-semibold flex items-center gap-1.5 border border-[#3d4948]/50 hover:border-[#ffb4ab]/40 transition-all shrink-0 cursor-pointer disabled:opacity-50"
          >
            <span className={`material-symbols-outlined text-[16px] ${clearing ? 'animate-spin' : ''}`}>
              {clearedNotice ? 'check' : clearing ? 'sync' : 'delete_forever'}
            </span>
            <span>{clearedNotice ? 'Borrado' : clearing ? 'Borrando...' : 'Borrar'}</span>
          </button>
        </div>
      </section>

      {/* 4. Sección: Información */}
      <section className="flex flex-col gap-2.5">
        <div className="flex items-center gap-1.5 px-1">
          <span className="material-symbols-outlined text-[#bcc9c7] text-[18px]">info</span>
          <h2 className="text-xs font-mono font-bold text-[#bcc9c7] uppercase tracking-wider">
            Información del sistema
          </h2>
        </div>

        <div className="bg-[#171c20] border border-[#1E3640] rounded-2xl overflow-hidden shadow-lg divide-y divide-[#1E3640]">
          <div className="flex items-center justify-between px-4 py-3 bg-[#171c20]">
            <div className="flex items-center gap-3">
              <div className="w-8 h-8 rounded-lg bg-[#252b2e] flex items-center justify-center text-[#bcc9c7]">
                <span className="material-symbols-outlined text-[18px]">description</span>
              </div>
              <span className="text-sm text-[#dee3e7]">Términos de uso</span>
            </div>
            <span className="text-xs font-mono text-[#869391]">Uso personal</span>
          </div>

          <div className="flex items-center justify-between px-4 py-3 bg-[#171c20]">
            <div className="flex items-center gap-3">
              <div className="w-8 h-8 rounded-lg bg-[#252b2e] flex items-center justify-center text-[#bcc9c7]">
                <span className="material-symbols-outlined text-[18px]">policy</span>
              </div>
              <span className="text-sm text-[#dee3e7]">Aviso de derechos de autor</span>
            </div>
            <span className="text-xs font-mono text-[#869391]">WCAG AA Compatible</span>
          </div>

          <div className="flex items-center justify-between px-4 py-3 bg-[#171c20]">
            <div className="flex items-center gap-3">
              <div className="w-8 h-8 rounded-lg bg-[#252b2e] flex items-center justify-center text-[#5fd0c8]">
                <span className="material-symbols-outlined text-[18px]">install_mobile</span>
              </div>
              <div className="flex flex-col text-left">
                <span className="text-sm font-semibold text-[#dee3e7]">Instalar como aplicación</span>
                <span className="text-xs font-mono text-[#869391]">
                  {isStandalone ? 'App ya instalada' : 'Acceso rápido en tu teléfono'}
                </span>
              </div>
            </div>
            {!isStandalone && onInstall && (
              <button
                type="button"
                onClick={onInstall}
                className="px-3 py-1.5 rounded-lg bg-[#5fd0c8] hover:bg-[#7dece4] text-[#041014] text-xs font-bold font-mono shadow-[0_0_10px_rgba(95,208,200,0.3)] transition-all flex items-center gap-1 active:scale-95"
              >
                <span className="material-symbols-outlined text-[14px]">download</span>
                <span>Instalar</span>
              </button>
            )}
          </div>

          <div className="flex items-center justify-between px-4 py-3 bg-[#171c20]">
            <div className="flex items-center gap-3">
              <div className="w-8 h-8 rounded-lg bg-[#0d101a] border border-[#1E3640] overflow-hidden flex items-center justify-center shadow-[0_0_8px_rgba(95,208,200,0.2)]">
                <img src="/icon.png" alt="Muvixo" className="w-full h-full object-cover" />
              </div>
              <span className="text-sm font-semibold text-[#dee3e7]">Muvixo Web App</span>
            </div>
            <span className="font-mono text-xs text-[#7dece4] bg-[#252b2e] px-2.5 py-1 rounded-md border border-[#5fd0c8]/30">
              v1.2.0 (Phosphor Retro)
            </span>
          </div>
        </div>
      </section>

      {/* Footer */}
      <div className="pt-2 text-center">
        <p className="text-xs font-mono text-[#869391]">
          Muvixo • Descarga solo contenido que tengas derecho a usar.
        </p>
      </div>
    </div>
  );
}
