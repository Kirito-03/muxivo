import React, { useState, useEffect, useCallback } from 'react';
import Header from './components/Header';
import BottomNav from './components/BottomNav';
import HomeView from './components/HomeView';
import VideoDetectionView from './components/VideoDetectionView';
import AudioDetectionView from './components/AudioDetectionView';
import ImageDetectionView from './components/ImageDetectionView';
import DownloadingView from './components/DownloadingView';
import CompletedView from './components/CompletedView';
import ErrorView from './components/ErrorView';
import HistoryView from './components/HistoryView';
import SettingsView from './components/SettingsView';
import { detectMedia, getHistory, getSessionId } from './utils/api';

// Platform display label helpers
const PLATFORM_LABELS = {
  tiktok: 'TikTok',
  instagram: 'Instagram',
  youtube: 'YouTube',
  soundcloud: 'SoundCloud',
  twitter: 'X / Twitter',
  x: 'X / Twitter',
  facebook: 'Facebook',
  direct: 'Enlace directo',
  other: 'Web',
};

function platformLabel(p) {
  return PLATFORM_LABELS[p?.toLowerCase()] || p || 'Web';
}

function kindToType(kind) {
  if (kind === 'audio') return 'audio';
  if (kind === 'image') return 'image';
  return 'video';
}

export default function App() {
  const [currentTab, setCurrentTab] = useState('descargar');
  // flowState: 'input' | 'detecting' | 'detected' | 'downloading' | 'completed' | 'error'
  const [flowState, setFlowState] = useState('input');
  const [urlInput, setUrlInput] = useState('');
  const [mediaData, setMediaData] = useState(null);
  const [activeDownload, setActiveDownload] = useState(null);
  const [detectError, setDetectError] = useState(null);

  // Session ID — persisted to keep history tied to this browser
  const [sessionId] = useState(() => getSessionId());

  // History — synced from server
  const [history, setHistory] = useState([]);
  const [historyLoading, setHistoryLoading] = useState(false);

  const refreshHistory = useCallback(async () => {
    setHistoryLoading(true);
    try {
      const data = await getHistory(sessionId);
      setHistory(data.items || []);
    } catch {
      // history fetch failures are non-critical
    } finally {
      setHistoryLoading(false);
    }
  }, [sessionId]);

  // Load history on mount and after each completed download
  useEffect(() => {
    refreshHistory();
  }, [refreshHistory]);

  // Handle URL analysis — calls the real /api/detect endpoint
  const handleAnalyzeUrl = useCallback(async (rawUrl) => {
    const url = (rawUrl || '').trim();
    if (!url) return;

    setUrlInput(url);
    setFlowState('detecting');
    setDetectError(null);

    try {
      const result = await detectMedia(url);

      if (result.error || (!result.detected_kind && !result.type)) {
        // Backend returned an error or couldn't detect
        setMediaData({
          type: 'error',
          url,
          platform: platformLabel(result.platform),
          message: result.message || result.error || 'No pudimos leer este enlace. Revisa que sea público.',
          errorCode: '0x404_DETECT_FAILED',
        });
        setFlowState('error');
        return;
      }

      // Build a normalized mediaData object from the backend response
      const detectedType = kindToType(result.detected_kind);
      const platform = platformLabel(result.platform);
      const resolvedUrl = result.resolved_url || url;

      const normalized = {
        type: detectedType,
        url,
        resolvedUrl,
        platform,
        rawPlatform: result.platform,
        allowedKinds: result.allowed_kinds || [detectedType],
        summary: result.summary || null,
        message: result.message || null,
        // Image gallery data
        isGallery: result.is_gallery || false,
        imageCandidates: result.items || [],
        // Files already available (pre-extracted)
        preFiles: result.files || [],
        // These will be filled in by the detection view after /api/options
        options: null,
        // Placeholder metadata — detection views fetch real options
        title: result.summary || platform,
        creator: null,
        thumbnail: null,
      };

      setMediaData(normalized);
      setFlowState('detected');
    } catch (err) {
      setMediaData({
        type: 'error',
        url,
        message: err.message || 'Error al conectar con el servidor.',
        errorCode: '0x500_SERVER_ERROR',
      });
      setFlowState('error');
    }
  }, []);

  // Handle Start Download — called by detection views
  const handleStartDownload = useCallback((downloadConfig) => {
    // downloadConfig: { url, resolvedUrl, kind, format, detail, imageUrls, title, platform, type, thumbnail }
    setActiveDownload({ ...downloadConfig, sessionId });
    setFlowState('downloading');
  }, [sessionId]);

  // Handle Download Complete — called by DownloadingView after POST /api/download
  const handleDownloadComplete = useCallback((result) => {
    // result: { files, zip, label, message, ok, ... }
    setActiveDownload((prev) => ({ ...prev, ...result }));
    setFlowState('completed');
    // Refresh history from server
    refreshHistory();
  }, [refreshHistory]);

  // Reset to input state for a new download
  const handleResetToInput = useCallback(() => {
    setUrlInput('');
    setMediaData(null);
    setActiveDownload(null);
    setDetectError(null);
    setFlowState('input');
    setCurrentTab('descargar');
  }, []);

  // Re-analyze a URL from history
  const handleRedownload = useCallback((histItem) => {
    const url = histItem.url || histItem.label || '';
    if (url.startsWith('http')) {
      handleAnalyzeUrl(url);
    }
    setCurrentTab('descargar');
  }, [handleAnalyzeUrl]);

  // Clear all history (local only — server history clears via session expiry)
  const handleClearHistory = useCallback(() => {
    setHistory([]);
  }, []);

  const handleSelectTab = useCallback((tab) => {
    setCurrentTab(tab);
    if (tab === 'historial') {
      refreshHistory();
    }
  }, [refreshHistory]);

  return (
    <div className="bg-[#060B0E] text-[#D5EEEC] font-sans min-h-screen relative selection:bg-[#5fd0c8] selection:text-[#041014] flex flex-col">
      {/* Top Header */}
      <Header
        currentTab={currentTab}
        onSelectTab={handleSelectTab}
      />

      {/* Main Content Area */}
      <main className="flex-1 w-full pt-20 px-4 sm:px-6 relative z-10 flex flex-col items-center">
        <div className="w-full max-w-[1440px] mx-auto py-4">

          {/* TAB 1: DESCARGAR */}
          {currentTab === 'descargar' && (
            <>
              {/* State: input */}
              {flowState === 'input' && (
                <HomeView
                  urlInput={urlInput}
                  setUrlInput={setUrlInput}
                  onAnalyzeUrl={handleAnalyzeUrl}
                  recentDownloads={history}
                  onRedownload={handleRedownload}
                />
              )}

              {/* State: detecting (loading) */}
              {flowState === 'detecting' && (
                <div className="flex flex-col items-center justify-center py-24 gap-6">
                  <div className="w-16 h-16 rounded-full bg-[#1b2024] border border-[#1E3640] flex items-center justify-center shadow-[0_0_24px_rgba(95,208,200,0.2)]">
                    <span className="material-symbols-outlined text-[32px] text-[#5fd0c8] animate-spin">refresh</span>
                  </div>
                  <div className="text-center">
                    <p className="text-[#dee3e7] font-semibold text-lg">Analizando enlace...</p>
                    <p className="text-[#869391] font-mono text-sm mt-1 truncate max-w-xs">{urlInput}</p>
                  </div>
                </div>
              )}

              {/* State: detected */}
              {flowState === 'detected' && mediaData && (
                <>
                  {mediaData.type === 'video' && (
                    <VideoDetectionView
                      mediaData={mediaData}
                      onChangeUrl={handleResetToInput}
                      onStartDownload={handleStartDownload}
                      recentDownloads={history}
                      onRedownload={handleRedownload}
                      sessionId={sessionId}
                    />
                  )}
                  {mediaData.type === 'audio' && (
                    <AudioDetectionView
                      mediaData={mediaData}
                      onChangeUrl={handleResetToInput}
                      onStartDownload={handleStartDownload}
                      sessionId={sessionId}
                    />
                  )}
                  {mediaData.type === 'image' && (
                    <ImageDetectionView
                      mediaData={mediaData}
                      onChangeUrl={handleResetToInput}
                      onStartDownload={handleStartDownload}
                      sessionId={sessionId}
                    />
                  )}
                </>
              )}

              {/* State: downloading */}
              {flowState === 'downloading' && activeDownload && (
                <DownloadingView
                  downloadItem={activeDownload}
                  onCancel={() => setFlowState('detected')}
                  onComplete={handleDownloadComplete}
                />
              )}

              {/* State: completed */}
              {flowState === 'completed' && activeDownload && (
                <CompletedView
                  completedItem={activeDownload}
                  onNewDownload={handleResetToInput}
                />
              )}

              {/* State: error */}
              {flowState === 'error' && (
                <ErrorView
                  errorData={mediaData}
                  onRetry={() => handleAnalyzeUrl(mediaData?.url || urlInput)}
                  onPasteAnother={handleResetToInput}
                  onClear={handleResetToInput}
                />
              )}
            </>
          )}

          {/* TAB 2: HISTORIAL */}
          {currentTab === 'historial' && (
            <HistoryView
              history={history}
              loading={historyLoading}
              onClearHistory={handleClearHistory}
              onRedownload={handleRedownload}
            />
          )}

          {/* TAB 3: AJUSTES */}
          {currentTab === 'ajustes' && (
            <SettingsView onClearHistory={handleClearHistory} />
          )}
        </div>
      </main>

      {/* Mobile Bottom Navigation */}
      <BottomNav currentTab={currentTab} onSelectTab={handleSelectTab} />
    </div>
  );
}
