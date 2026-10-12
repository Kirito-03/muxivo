import React from 'react';

export default function BottomNav({ currentTab, onSelectTab }) {
  return (
    <nav className="md:hidden fixed bottom-0 w-full z-50 pb-safe bg-[#171c20]/95 backdrop-blur-xl border-t border-[#1E3640]/60 shadow-[0_-8px_24px_rgba(0,0,0,0.55)]">
      <div className="max-w-[480px] mx-auto flex items-center justify-around h-16 px-2">
        {/* Descargar */}
        <button
          onClick={() => onSelectTab('descargar')}
          className={`flex flex-col items-center justify-center min-w-[56px] min-h-[44px] gap-1 transition-all ${
            currentTab === 'descargar'
              ? 'text-[#5fd0c8] drop-shadow-[0_0_8px_rgba(95,208,200,0.45)] font-semibold'
              : 'text-[#7FA3A6] hover:text-[#D5EEEC]'
          }`}
        >
          <span
            className="material-symbols-outlined text-[22px]"
            style={{ fontVariationSettings: currentTab === 'descargar' ? "'FILL' 1" : "'FILL' 0" }}
          >
            download
          </span>
          <span className="font-mono text-[11px] tracking-wide">Descargar</span>
        </button>

        {/* Historial */}
        <button
          onClick={() => onSelectTab('historial')}
          className={`flex flex-col items-center justify-center min-w-[56px] min-h-[44px] gap-1 transition-all ${
            currentTab === 'historial'
              ? 'text-[#5fd0c8] drop-shadow-[0_0_8px_rgba(95,208,200,0.45)] font-semibold'
              : 'text-[#7FA3A6] hover:text-[#D5EEEC]'
          }`}
        >
          <span
            className="material-symbols-outlined text-[22px]"
            style={{ fontVariationSettings: currentTab === 'historial' ? "'FILL' 1" : "'FILL' 0" }}
          >
            history
          </span>
          <span className="font-mono text-[11px] tracking-wide">Historial</span>
        </button>

        {/* Ajustes */}
        <button
          onClick={() => onSelectTab('ajustes')}
          className={`flex flex-col items-center justify-center min-w-[56px] min-h-[44px] gap-1 transition-all ${
            currentTab === 'ajustes'
              ? 'text-[#5fd0c8] drop-shadow-[0_0_8px_rgba(95,208,200,0.45)] font-semibold'
              : 'text-[#7FA3A6] hover:text-[#D5EEEC]'
          }`}
        >
          <span
            className="material-symbols-outlined text-[22px]"
            style={{ fontVariationSettings: currentTab === 'ajustes' ? "'FILL' 1" : "'FILL' 0" }}
          >
            settings
          </span>
          <span className="font-mono text-[11px] tracking-wide">Ajustes</span>
        </button>
      </div>
    </nav>
  );
}
