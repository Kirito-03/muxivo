import { useState, useEffect } from 'react';

export function usePwaInstall() {
  const [deferredPrompt, setDeferredPrompt] = useState(null);
  const [isStandalone, setIsStandalone] = useState(false);
  const [isIOS, setIsIOS] = useState(false);
  const [isMobile, setIsMobile] = useState(false);
  const [bannerDismissed, setBannerDismissed] = useState(() => {
    try {
      const ts = localStorage.getItem('muxivo_pwa_dismissed');
      if (ts && Date.now() - parseInt(ts, 10) < 24 * 60 * 60 * 1000) {
        return true;
      }
    } catch {}
    return false;
  });
  const [showIosModal, setShowIosModal] = useState(false);

  useEffect(() => {
    const standaloneCheck =
      window.matchMedia('(display-mode: standalone)').matches ||
      window.navigator.standalone === true;
    setIsStandalone(standaloneCheck);

    const ua = navigator.userAgent || '';
    const iosCheck = /iPad|iPhone|iPod/.test(ua) && !window.MSStream;
    setIsIOS(iosCheck);

    const mobileCheck =
      /Android|iPhone|iPad|iPod|Mobile|BlackBerry|IEMobile|Opera Mini/i.test(ua) ||
      window.innerWidth <= 768;
    setIsMobile(mobileCheck);

    const handler = (e) => {
      e.preventDefault();
      setDeferredPrompt(e);
    };

    window.addEventListener('beforeinstallprompt', handler);

    const appInstalledHandler = () => {
      setIsStandalone(true);
      setDeferredPrompt(null);
    };
    window.addEventListener('appinstalled', appInstalledHandler);

    return () => {
      window.removeEventListener('beforeinstallprompt', handler);
      window.removeEventListener('appinstalled', appInstalledHandler);
    };
  }, []);

  const installApp = async () => {
    if (deferredPrompt) {
      try {
        deferredPrompt.prompt();
        const choice = await deferredPrompt.userChoice;
        if (choice.outcome === 'accepted') {
          setDeferredPrompt(null);
        }
      } catch {
        setShowIosModal(true);
      }
      return;
    }

    // If on iOS or browser without programmatic prompt
    setShowIosModal(true);
  };

  const dismissBanner = () => {
    setBannerDismissed(true);
    try {
      localStorage.setItem('muxivo_pwa_dismissed', Date.now().toString());
    } catch {}
  };

  // Show banner if not standalone, is on mobile (or prompt available), and not dismissed
  const showBanner = !isStandalone && (isMobile || !!deferredPrompt) && !bannerDismissed;

  return {
    isInstallable: !!deferredPrompt || isIOS || isMobile,
    isStandalone,
    isIOS,
    isMobile,
    showBanner,
    dismissBanner,
    installApp,
    showIosModal,
    setShowIosModal,
  };
}
