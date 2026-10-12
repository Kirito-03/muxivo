import React, { useState } from 'react';

const KIND_ICONS = {
  video: 'play_circle',
  audio: 'graphic_eq',
  image: 'collections',
};

function formatTimestamp(ts) {
  if (!ts) return '';
  try {
    const d = new Date(ts * 1000);
    const now = new Date();
    const diffMs = now - d;
    const diffMin = Math.floor(diffMs / 60000);
    if (diffMin < 1) return 'Hace un momento';
    if (diffMin < 60) return `Hace ${diffMin} min`;
    const diffHr = Math.floor(diffMin / 60);
    if (diffHr < 24) return `Hace ${diffHr} hora${diffHr !== 1 ? 's' : ''}`;
    const diffDays = Math.floor(diffHr / 24);
    if (diffDays === 1) return 'Ayer';
    return `Hace ${diffDays} días`;
  } catch {
    return '';
  }
}

export default function HistoryView({ history = [], loading = false, onClearHistory, onRedownload }) {
  const [searchQuery, setSearchQuery] = useState('');
  const [filterKind, setFilterKind] = useState('todos');

  // Counts for filters (using server 'kind' field)
  const counts = {
    todos: history.length,
    video: history.filter((i) => i.kind === 'video').length,
    audio: history.filter((i) => i.kind === 'audio').length,
    imagen: history.filter((i) => i.kind === 'image').length,
  };

  // Filtered items
  const filtered = history.filter((item) => {
    const matchesKind =
      filterKind === 'todos' ||
      (filterKind === 'imagen' ? item.kind === 'image' : item.kind === filterKind);
    const q = searchQuery.toLowerCase().trim();
    const matchesQuery = !q || (item.label || '').toLowerCase().includes(q);
    return matchesKind && matchesQuery;
  });

  return (
    <div className="w-full max-w-[760px] mx-auto pb-24 md:pb-12 flex flex-col gap-6 animate-fade-in">
      {/* 1. Header Section */}
      <div className="flex flex-col gap-1 text-left">
        <h1 className="text-2xl sm:text-3xl text-[#dee3e7] font-bold tracking-tight">
          Historial de descargas
        </h1>
        <p className="text-sm text-[#bcc9c7]">
          Archivos procesados en esta sesión del servidor
        </p>
      </div>

      {/* 2. Search Bar */}
      <div className="relative w-full">
        <div className="absolute inset-y-0 left-0 pl-4 flex items-center pointer-events-none text-[#869391]">
          <span className="material-symbols-outlined text-[20px]">search</span>
        </div>
        <input
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          className="w-full h-12 pl-12 pr-4 bg-[#1b2024] border border-[#1E3640] rounded-xl text-[#dee3e7] placeholder:text-[#869391] font-mono text-xs sm:text-sm focus:outline-none focus:border-[#5fd0c8] focus:bg-[#252b2e] transition-all shadow-sm"
          id="searchInput"
          placeholder="Buscar por título o plataforma..."
          type="text"
        />
        {searchQuery && (
          <button
            onClick={() => setSearchQuery('')}
            className="absolute inset-y-0 right-0 pr-3 flex items-center text-[#869391] hover:text-[#dee3e7]"
          >
            <span className="material-symbols-outlined text-[18px]">close</span>
          </button>
        )}
      </div>

      {/* 3. Filter Pills and Clear Button */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-wrap items-center gap-2">
          {[
            { id: 'todos', label: 'Todos', count: counts.todos },
            { id: 'video', label: 'Video', count: counts.video },
            { id: 'audio', label: 'Audio', count: counts.audio },
            { id: 'imagen', label: 'Imagen', count: counts.imagen },
          ].map((pill) => {
            const isActive = filterKind === pill.id;
            return (
              <button
                key={pill.id}
                onClick={() => setFilterKind(pill.id)}
                type="button"
                className={`px-3.5 py-1.5 rounded-full text-xs font-mono font-medium transition-all ${
                  isActive
                    ? 'bg-[#5fd0c8]/20 text-[#7dece4] border border-[#5fd0c8] shadow-[0_0_12px_rgba(95,208,200,0.2)]'
                    : 'bg-[#1b2024] border border-[#1E3640] text-[#bcc9c7] hover:text-[#dee3e7] hover:bg-[#252b2e]'
                }`}
              >
                {pill.label} ({pill.count})
              </button>
            );
          })}
        </div>

        {history.length > 0 && (
          <button
            onClick={onClearHistory}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-xl text-[#bcc9c7] hover:text-[#ffb4ab] hover:bg-[#93000a]/20 border border-transparent hover:border-[#ffb4ab]/30 transition-all text-xs font-mono group"
            title="Borrar historial local"
          >
            <span className="material-symbols-outlined text-[18px] transition-transform group-hover:scale-110">delete</span>
            <span>Limpiar historial</span>
          </button>
        )}
      </div>

      {/* 4. History List */}
      <div className="flex flex-col gap-3">
        {loading && (
          <div className="flex items-center justify-center py-8 gap-3">
            <span className="material-symbols-outlined text-[24px] text-[#5fd0c8] animate-spin">refresh</span>
            <span className="text-sm font-mono text-[#bcc9c7]">Cargando historial...</span>
          </div>
        )}

        {!loading && filtered.length === 0 && (
          <div className="flex flex-col items-center justify-center p-12 bg-[#1b2024] border border-[#1E3640] rounded-2xl text-center">
            <span className="material-symbols-outlined text-[#7dece4] text-5xl mb-3">history_toggle_off</span>
            <p className="text-base text-[#dee3e7] font-semibold mb-1">
              {history.length === 0 ? 'El historial está vacío' : 'No se encontraron resultados'}
            </p>
            <p className="text-xs text-[#bcc9c7]">
              {history.length === 0
                ? 'Realiza tu primera descarga para ver el historial aquí.'
                : 'Intenta con otro término de búsqueda o selecciona otra categoría.'}
            </p>
          </div>
        )}

        {!loading &&
          filtered.map((item, i) => {
            const icon = KIND_ICONS[item.kind] || 'download';
            const timeLabel = formatTimestamp(item.ts);

            return (
              <div
                key={item.ts || i}
                className="flex items-center justify-between p-3.5 sm:p-4 bg-[#1b2024] border border-[#1E3640] rounded-xl hover:bg-[#252b2e] hover:border-[#5fd0c8]/40 transition-all shadow-sm gap-3 sm:gap-4 group"
              >
                <div className="flex items-center gap-3.5 min-w-0 flex-1">
                  {/* Kind Icon */}
                  <div className="w-12 h-12 rounded-xl bg-[#252b2e] border border-[#3d4948]/60 flex items-center justify-center shrink-0 text-[#5fd0c8]">
                    <span className="material-symbols-outlined text-[22px]">{icon}</span>
                  </div>

                  {/* Details */}
                  <div className="flex flex-col min-w-0">
                    <span className="text-sm font-bold text-[#dee3e7] truncate group-hover:text-[#5fd0c8] transition-colors">
                      {item.label || '(sin título)'}
                    </span>
                    <div className="flex flex-wrap items-center gap-2 mt-1">
                      <span className="px-2 py-0.5 rounded-full bg-[#252b2e] border border-[#3d4948]/60 text-[#7dece4] text-[11px] font-mono font-medium">
                        {item.kind?.toUpperCase() || 'ARCHIVO'}
                      </span>
                      <span className="text-xs font-mono text-[#bcc9c7]">
                        {item.ok} archivo{item.ok !== 1 ? 's' : ''}
                        {item.fail > 0 && ` • ${item.fail} error${item.fail !== 1 ? 'es' : ''}`}
                        {timeLabel && ` • ${timeLabel}`}
                      </span>
                    </div>

                    {/* Files list */}
                    {item.files && item.files.length > 0 && (
                      <div className="flex flex-wrap gap-1.5 mt-1.5">
                        {item.files.slice(0, 3).map((f, fi) => (
                          <a
                            key={fi}
                            href={f.url}
                            download={f.name}
                            className="flex items-center gap-1 px-2 py-0.5 rounded-lg bg-[#5fd0c8]/10 border border-[#5fd0c8]/30 text-[#5fd0c8] text-[11px] font-mono hover:bg-[#5fd0c8]/20 transition-colors"
                            title={`Descargar ${f.name}`}
                            onClick={(e) => e.stopPropagation()}
                          >
                            <span className="material-symbols-outlined text-[12px]">download</span>
                            <span className="truncate max-w-[100px]">{f.name}</span>
                          </a>
                        ))}
                        {item.files.length > 3 && (
                          <span className="text-[11px] font-mono text-[#869391] self-center">
                            +{item.files.length - 3} más
                          </span>
                        )}
                      </div>
                    )}
                  </div>
                </div>

                {/* ZIP download if available */}
                {item.zip && (
                  <a
                    href={item.zip.url}
                    download={item.zip.name}
                    className="shrink-0 flex items-center gap-2 px-3.5 py-2 bg-[#5fd0c8]/15 hover:bg-[#5fd0c8] text-[#7dece4] hover:text-[#041014] border border-[#5fd0c8]/30 rounded-xl text-xs font-semibold font-mono transition-all shadow-[0_0_12px_rgba(95,208,200,0.12)] hover:shadow-[0_0_16px_rgba(95,208,200,0.3)] active:scale-95"
                    title="Descargar ZIP"
                    onClick={(e) => e.stopPropagation()}
                  >
                    <span className="material-symbols-outlined text-[18px]">folder_zip</span>
                    <span className="hidden sm:inline">ZIP</span>
                  </a>
                )}
              </div>
            );
          })}
      </div>

      {/* 5. Info Banner */}
      <div className="flex flex-col gap-2 pt-2">
        <div className="flex items-center gap-3 p-4 bg-[#1b2024] border border-[#1E3640] rounded-xl shadow-sm">
          <span className="material-symbols-outlined text-[#7dece4] text-[22px] shrink-0">folder_open</span>
          <p className="text-xs font-mono text-[#bcc9c7]">
            Los archivos se guardan en el servidor durante la sesión activa. Descárgalos antes de que expiren.
          </p>
        </div>
        <p className="text-center text-xs font-mono text-[#869391]">
          Descarga solo contenido que tengas derecho a usar. Muxivo no almacena archivos de forma permanente.
        </p>
      </div>
    </div>
  );
}
