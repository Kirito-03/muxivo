import React from 'react';

export default function Header({ currentTab, onSelectTab, showInstallBtn = false, onInstall }) {
  return (
    <header className="fixed top-0 w-full z-50 bg-[#060B0E]/90 backdrop-blur-md border-b border-[#1E3640]/60">
      <div className="h-16 max-w-[1440px] mx-auto px-4 sm:px-6 flex items-center justify-between">
        {/* Logo Muvixo */}
        <button
          onClick={() => onSelectTab('descargar')}
          className="flex items-center group text-left transition-transform active:scale-95"
          title="Muvixo - Inicio"
        >
          <img
            src="/logo-dark.png"
            alt="Muvixo"
            className="h-8 sm:h-9 w-auto object-contain transition-transform duration-200 group-hover:scale-105 drop-shadow-[0_0_12px_rgba(95,208,200,0.25)]"
          />
        </button>

        <div className="flex items-center gap-4 sm:gap-6">
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
      </div>
    </header>
  );
}
