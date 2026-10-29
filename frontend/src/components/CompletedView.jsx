import React, { useState } from 'react';

const KIND_ICONS = {
  video: 'smart_display',
  audio: 'audiotrack',
  image: 'image',
  zip: 'folder_zip',
  file: 'description',
};

function getDownloadUrl(url, name) {
  if (!url) return '';
  try {
    const parsed = new URL(url, window.location.origin);
    parsed.searchParams.set('download', '1');
    if (name) {
      parsed.searchParams.set('name', name);
    }
    return parsed.toString();
  } catch {
    const sep = url.includes('?') ? '&' : '?';
    const nameParam = name ? `&name=${encodeURIComponent(name)}` : '';
    return `${url}${sep}download=1${nameParam}`;
  }
}

function FileCard({ file, showAction = true }) {
  const [downloading, setDownloading] = useState(false);
  const icon = KIND_ICONS[file.kind] || 'description';

  const handleDownload = async () => {
    setDownloading(true);
    try {
      const a = document.createElement('a');
      a.href = getDownloadUrl(file.url, file.name);
      a.download = file.name || 'archivo';
      a.style.display = 'none';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
    } catch {
      window.open(getDownloadUrl(file.url, file.name), '_blank');
    } finally {
      setTimeout(() => setDownloading(false), 1500);
    }
  };

  return (
    <div className="bg-[#252b2e] border border-[#3d4948]/40 rounded-xl p-3 flex items-center gap-3 hover:bg-[#303639] transition-colors group">
      <div className="w-10 h-10 rounded-lg bg-[#1b2024] border border-[#1E3640] flex items-center justify-center shrink-0 text-[#5fd0c8]">
        <span className="material-symbols-outlined text-[22px]">{icon}</span>
      </div>
      <div className="flex-1 min-w-0">
        <p className="text-xs font-bold text-[#dee3e7] truncate">{file.name}</p>
        <p className="text-[11px] font-mono text-[#869391]">{file.kind?.toUpperCase()}</p>
      </div>
      {showAction && (
        <button
          onClick={handleDownload}
          disabled={downloading}
          className="w-9 h-9 rounded-lg bg-[#5fd0c8]/10 hover:bg-[#5fd0c8] hover:text-[#041014] text-[#5fd0c8] flex items-center justify-center shrink-0 transition-all disabled:opacity-50"
          title={`Descargar ${file.name}`}
          type="button"
        >
          {downloading ? (
            <span className="material-symbols-outlined text-[16px] animate-spin">refresh</span>
          ) : (
            <span className="material-symbols-outlined text-[16px]">download</span>
          )}
        </button>
      )}
    </div>
  );
}

export default function CompletedView({ completedItem, onNewDownload }) {
  const [toast, setToast] = useState('');

  const triggerToast = (msg) => {
    setToast(msg);
    setTimeout(() => setToast(''), 2500);
  };

  // Collect all downloadable files from the result
  const files = completedItem?.files || [];
  const showZip = Boolean(completedItem?.type === 'image' && completedItem?.zip);
  const zip = showZip ? completedItem?.zip : null;
  const failures = completedItem?.failures || [];
  const successCount = completedItem?.success || files.length || 0;
  const hasFiles = files.length > 0 || zip;

  const handleShare = () => {
    if (navigator.clipboard) {
      navigator.clipboard.writeText(window.location.href);
    }
    triggerToast('Enlace copiado para compartir');
  };

  return (
    <div className="max-w-[480px] w-full mx-auto pb-24 md:pb-12 flex flex-col gap-5 animate-fade-in">
      {/* 1. Main Success Card */}
      <div className="relative w-full rounded-2xl bg-[#171c20] border border-[#1E3640] shadow-2xl p-6 flex flex-col items-center text-center overflow-hidden">
        {/* Ambient green phosphor glow */}
        <div className="absolute -top-16 left-1/2 -translate-x-1/2 w-48 h-48 bg-[#3DDC97]/15 rounded-full blur-3xl pointer-events-none"></div>

        {/* Pulsing check circle */}
        <div className="relative mb-5 flex items-center justify-center">
          <div className="absolute inset-0 rounded-full bg-[#3DDC97]/25 blur-md animate-pulse"></div>
          <div className="relative w-20 h-20 rounded-full bg-[#252b2e] border border-[#3d4948] shadow-lg flex items-center justify-center shadow-[0_0_24px_rgba(61,220,151,0.25)]">
            <div className="w-14 h-14 rounded-full bg-[#1b2024] flex items-center justify-center">
              <span
                className="material-symbols-outlined text-[36px] text-[#3DDC97] drop-shadow-[0_0_12px_rgba(61,220,151,0.6)]"
                style={{ fontVariationSettings: "'FILL' 1" }}
              >
                check_circle
              </span>
            </div>
          </div>
        </div>

        {/* Terminal Pill */}
        <div className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full bg-[#1b2024] border border-[#1E3640] mb-3 shadow-sm">
          <span className="w-2 h-2 rounded-full bg-[#3DDC97] shadow-[0_0_6px_#3DDC97]"></span>
          <span className="font-mono text-xs text-[#7dece4] tracking-wider uppercase">
            Estado: 200 OK • {successCount} archivo{successCount !== 1 ? 's' : ''} listo{successCount !== 1 ? 's' : ''}
          </span>
        </div>

        <h1 className="text-2xl sm:text-3xl text-[#dee3e7] font-bold tracking-tight mb-2">
          ¡Listo!
        </h1>
        <p className="text-sm text-[#bcc9c7] max-w-[300px] leading-relaxed">
          {completedItem?.message || `${successCount} archivo${successCount !== 1 ? 's' : ''} descargado${successCount !== 1 ? 's' : ''} correctamente.`}
        </p>

        {/* Platform/Kind row */}
        {(completedItem?.platform || completedItem?.type) && (
          <div className="flex items-center gap-2 mt-3 font-mono text-xs text-[#869391]">
            {completedItem.platform && (
              <span className="px-2 py-0.5 rounded-full bg-[#252b2e] border border-[#3d4948]/60 text-[#dee3e7]">
                {completedItem.platform}
              </span>
            )}
            {completedItem.type && (
              <span className="px-2 py-0.5 rounded-full bg-[#5fd0c8]/10 border border-[#5fd0c8]/30 text-[#5fd0c8]">
                {completedItem.type.toUpperCase()}
              </span>
            )}
          </div>
        )}
      </div>

      {/* 2. Files to Download */}
      {hasFiles && (
        <div className="bg-[#1b2024] border border-[#1E3640] rounded-2xl p-4 shadow-xl flex flex-col gap-3">
          <div className="flex items-center gap-2 mb-1">
            <span className="material-symbols-outlined text-[#5fd0c8] text-[20px]">download</span>
            <h2 className="text-sm font-bold text-[#dee3e7]">
              Archivos disponibles ({files.length + (showZip ? 1 : 0)})
            </h2>
          </div>

          <div className="flex flex-col gap-2">
            {/* ZIP only for galleries */}
            {showZip && <FileCard file={zip} showAction={true} />}
            {/* Individual files */}
            {files.map((f, i) => (
              <FileCard key={i} file={f} showAction={!(files.length === 1 && !showZip)} />
            ))}
          </div>
        </div>
      )}

      {/* 3. Failures (if any) */}
      {failures.length > 0 && (
        <div className="bg-[#1b2024] border border-[#ff808b]/30 rounded-2xl p-4 shadow-xl flex flex-col gap-3">
          <div className="flex items-center gap-2">
            <span className="material-symbols-outlined text-[#ff808b] text-[18px]">warning</span>
            <h3 className="text-sm font-bold text-[#ff808b]">
              {failures.length} archivo{failures.length !== 1 ? 's' : ''} con error
            </h3>
          </div>
          <div className="flex flex-col gap-2">
            {failures.map((f, i) => (
              <div key={i} className="bg-[#ff808b]/5 border border-[#ff808b]/20 rounded-lg p-2.5">
                <p className="text-xs font-bold text-[#dee3e7] truncate">{f.label}</p>
                <p className="text-[11px] font-mono text-[#ff808b] mt-0.5">{f.reason}</p>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* 4. Action Buttons */}
      <div className="w-full flex flex-col gap-3">
        {files.length === 1 && !showZip ? (
          <a
            href={getDownloadUrl(files[0].url, files[0].name)}
            download={files[0].name}
            className="w-full h-14 rounded-2xl bg-[#5fd0c8] hover:bg-[#7FE0D8] active:bg-[#46B8B0] text-[#041014] text-base font-bold flex items-center justify-center gap-2.5 shadow-[0_0_20px_rgba(95,208,200,0.35)] active:scale-[0.98] transition-all cursor-pointer"
          >
            <span className="material-symbols-outlined text-[24px]">download</span>
            <span>
              Descargar{' '}
              {completedItem?.type === 'audio'
                ? 'Audio'
                : completedItem?.type === 'image' || files[0]?.kind === 'image'
                  ? 'Imagen'
                  : 'Video'}
            </span>
          </a>
        ) : null}

        <button
          onClick={onNewDownload}
          className={`w-full h-14 rounded-2xl ${
            files.length === 1 && !showZip
              ? 'bg-[#252b2e] hover:bg-[#303639] border border-[#3d4948]/60 text-[#dee3e7]'
              : 'bg-[#5fd0c8] hover:bg-[#7FE0D8] active:bg-[#46B8B0] text-[#041014] shadow-[0_0_20px_rgba(95,208,200,0.35)]'
          } text-base font-bold flex items-center justify-center gap-2.5 active:scale-[0.98] transition-all cursor-pointer`}
          type="button"
        >
          <span className="material-symbols-outlined text-[24px]">add_link</span>
          <span>Descargar otro</span>
        </button>

        {/* Secondary options */}
        <div className="flex items-center justify-center gap-4 mt-1">
          <button
            onClick={handleShare}
            className="flex items-center gap-1.5 py-1.5 px-3 rounded-lg text-[#bcc9c7] hover:text-[#dee3e7] text-xs font-mono transition-colors"
            type="button"
          >
            <span className="material-symbols-outlined text-[18px]">share</span>
            <span>Compartir</span>
          </button>
        </div>
      </div>

      {/* Ethical Usage Notice */}
      <div className="px-3 py-3 rounded-xl bg-[#090f12] border border-[#1E3640] flex items-start gap-2.5 text-left">
        <span className="material-symbols-outlined text-[18px] text-[#5fd0c8] shrink-0 mt-0.5">
          verified_user
        </span>
        <p className="text-xs font-mono text-[#869391] leading-relaxed">
          Descarga solo contenido que tengas derecho a usar. Muvixo respeta la propiedad intelectual y los términos de servicio.
        </p>
      </div>

      {/* Floating Toast Notification */}
      {toast && (
        <div className="fixed bottom-20 left-4 right-4 max-w-[360px] mx-auto bg-[#303639] text-[#dee3e7] px-4 py-3 rounded-xl shadow-2xl flex items-center gap-3 border border-[#5fd0c8]/40 z-50 animate-fade-in">
          <span className="material-symbols-outlined text-[#7dece4] text-[20px]">info</span>
          <span className="text-xs font-mono">{toast}</span>
        </div>
      )}
    </div>
  );
}
