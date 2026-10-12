import React from 'react';

export default function Header({ currentTab, onSelectTab }) {
  return (
    <header className="fixed top-0 w-full z-50 bg-[#060B0E]/90 backdrop-blur-md border-b border-[#1E3640]/60">
      <div className="h-16 max-w-[1440px] mx-auto px-4 sm:px-6 flex items-center justify-between">
        {/* Logo Muxivo */}
        <button
          onClick={() => onSelectTab('descargar')}
          className="flex items-center gap-3 group text-left transition-transform active:scale-95"
        >
          <div className="w-9 h-9 rounded-lg bg-[#252b2e] border border-[#3d4948]/60 flex items-center justify-center shadow-[0_0_12px_rgba(95,208,200,0.18)] group-hover:border-[#5fd0c8] group-hover:shadow-[0_0_16px_rgba(95,208,200,0.35)] transition-all">
            <span className="text-[#7dece4] font-bold text-lg tracking-tight">M</span>
          </div>
          <div className="flex items-baseline">
            <span className="text-[#dee3e7] text-xl font-bold tracking-tight lowercase">mux</span>
            <span className="text-[#5fd0c8] text-xl font-bold tracking-tight lowercase drop-shadow-[0_0_8px_rgba(95,208,200,0.45)]">ivo</span>
          </div>
        </button>

        {/* Desktop Navigation */}
        <nav className="hidden md:flex items-center gap-8">
          <button
            onClick={() => onSelectTab('descargar')}
            className={`pb-1 transition-all font-medium text-sm flex items-center gap-1.5 ${
              currentTab === 'descargar'
                ? 'text-[#7dece4] font-semibold border-b-2 border-[#5fd0c8]'
                : 'text-[#bcc9c7] hover:text-[#dee3e7]'
            }`}
          >
            <span className="material-symbols-outlined text-[18px]">download</span>
            <span>Descargar</span>
          </button>

          <button
            onClick={() => onSelectTab('historial')}
            className={`pb-1 transition-all font-medium text-sm flex items-center gap-1.5 ${
              currentTab === 'historial'
                ? 'text-[#7dece4] font-semibold border-b-2 border-[#5fd0c8]'
                : 'text-[#bcc9c7] hover:text-[#dee3e7]'
            }`}
          >
            <span className="material-symbols-outlined text-[18px]">history</span>
            <span>Historial</span>
          </button>

          <button
            onClick={() => onSelectTab('ajustes')}
            className={`pb-1 transition-all font-medium text-sm flex items-center gap-1.5 ${
              currentTab === 'ajustes'
                ? 'text-[#7dece4] font-semibold border-b-2 border-[#5fd0c8]'
                : 'text-[#bcc9c7] hover:text-[#dee3e7]'
            }`}
          >
            <span className="material-symbols-outlined text-[18px]">settings</span>
            <span>Ajustes</span>
          </button>
        </nav>
      </div>
    </header>
  );
}
