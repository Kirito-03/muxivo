// Detector de medios por URL y dominio para Muxivo
export const DEMO_LINKS = [
  {
    label: 'TikTok Video',
    type: 'video',
    url: 'https://www.tiktok.com/@trenddance/video/7391829402',
    icon: 'smart_display'
  },
  {
    label: 'SoundCloud Audio',
    type: 'audio',
    url: 'https://soundcloud.com/artist/midnight-city-lights',
    icon: 'graphic_eq'
  },
  {
    label: 'Instagram Galería',
    type: 'image',
    url: 'https://instagram.com/p/C8k9Lx2vQnM_tokyo_cyber',
    icon: 'collections'
  },
  {
    label: 'Enlace privado (Error)',
    type: 'error',
    url: 'https://social.stream/p/private_reel_x8849?auth=null',
    icon: 'link_off'
  }
];

export const INITIAL_HISTORY = [
  {
    id: 'hist-1',
    title: 'Trend Dance 2025 #viral',
    creator: '@trenddance',
    type: 'video',
    platform: 'TikTok',
    format: 'Video MP4 • 1080p',
    size: '18.4 MB',
    time: 'Hace 10 min',
    thumbnail: 'https://lh3.googleusercontent.com/aida-public/AB6AXuDQ2_daIvjywJoOgPsAVwmsHeF7Jd4xbAfT9n4Q-diYRtU6GQU9VlY0g-TInHBGEhoaIJy2VxQRVg9mMMy81HSlbu9eR_Rdycxqx8aVlJub9-6k18jo9kcvQt_Qvf1xzFMqIQ84vkIjFUiTVSmZVaAe-ai3uWCxt_-OlmYdqiaWPuU7AZfPqcHBkO2O0iW4m5ST0KMSldzv1jgGqmh8RkbKaTyOdICzxdkR5i3fkZb9Ijvk-MxpLMGV',
    url: 'https://www.tiktok.com/@trenddance/video/7391829402'
  },
  {
    id: 'hist-2',
    title: 'Midnight City Lights - Neon Dreamer',
    creator: 'Neon Dreamer',
    type: 'audio',
    platform: 'SoundCloud',
    format: 'Audio MP3 • 320 kbps',
    size: '8.7 MB',
    time: 'Hace 1 hora',
    thumbnail: 'https://lh3.googleusercontent.com/aida-public/AB6AXuAUfqB2Dw-hl2BGqm86RvoAZDykowyFGUDbMq3PR-efb_B8kkeY1ilW1c_EVnvmv7ccXfCrXd4zPNwJozeUGJ8yE73p0oHZQcrNd46dcW2V8T6-czW55e29IrtdQ_W7m6qzgcY3ra-isFvl6eHOtgJi2R9S1KUzcnBbCRYUGFkXu-HNM8qLIBRbYIXqIxPjPbKfzU-oLZReEJ65V3b09fE84PwjE70hnWSUylcXwbUWZYEf4S0Xcwir',
    url: 'https://soundcloud.com/artist/midnight-city-lights'
  },
  {
    id: 'hist-3',
    title: 'Sunset in Kyoto Photo Set',
    creator: 'Tokyo Lens',
    type: 'image',
    platform: 'Instagram',
    format: 'Galería JPG • 4 fotos',
    size: '14.2 MB',
    time: 'Ayer',
    thumbnail: 'https://lh3.googleusercontent.com/aida-public/AB6AXuCZNqt3uHK1cSLypc6uFq-GpXHi1qRDc7kEpa3OR225Qx0FlnEWT7gaXsDuVwJfa0up2tJKOmCCzeW9tIxKBSk2q80kNQPbgh1ug2m48_OtqQralUGTSJOrchZCGOUAGAcjKIBoj_MvbToh-wLeTomAkT-AAP7T9RZJtIgS3aapuZt0NTDmizprjx19V4cSpxnTn7kWPICsnPXhuBe54cAseX7h4AXI6lBqa0qfVIIuFNWrCRzBHHdX',
    url: 'https://instagram.com/p/C8k9Lx2vQnM_tokyo_cyber'
  },
  {
    id: 'hist-4',
    title: 'Lo-Fi Chill Beats Radio',
    creator: 'Retro Wave',
    type: 'audio',
    platform: 'SoundCloud',
    format: 'Audio M4A • 256 kbps',
    size: '12.3 MB',
    time: 'Hace 2 días',
    thumbnail: 'https://lh3.googleusercontent.com/aida-public/AB6AXuB3w83P9AawOc1UxAob7XrNT2FB2BjKKKLOnDjJdfafcx1Ro_Ymq7xijbfSsHe6RldZrD2AY1LhhRhdEAE1Kl2e8Y6jvDUEVvVX65g8M3tmeDM7fgjDAW0EuNYz8_6pmi1wJ9QCztmiA6r1gjpQOJVFs20j-b8YYZuehYo0iC-CUmH8jvyu74DU4g1rmwskeK3PuRFTQXAMafeVpRFdKNR-357R4Hjm7ay-ZauqiBbRvZ0oNDsmEbbQ',
    url: 'https://soundcloud.com/retrowave/lofi-chill'
  },
  {
    id: 'hist-5',
    title: 'Tutorial Animación Web CSS',
    creator: 'Dev Frontend',
    type: 'video',
    platform: 'YouTube',
    format: 'Video MP4 • 720p',
    size: '34.1 MB',
    time: 'Hace 3 días',
    thumbnail: 'https://lh3.googleusercontent.com/aida-public/AB6AXuDsxE2sfbb_4uEjvzHakCLhmuiAzk1Iw7os3JCtqIkY-45kPsTvBO2fG7SkU9SeBkSY-mYmzNDj2nRFA0gKQG8WskJc71DG2YX6qjP4dTWr1vQwvIISe7CnGrYG9jiOO5QuoMiWXclED5ewyxkPchMy2J0_sv5BO9sbfgeoELhRdEd-98GM2FjJeSSaoPyhsdiiMLcYfCaZb-SYMiUnPI_K9Ess_4Ds6zuIaOkxhEdZOZ2NZulCMNDW',
    url: 'https://youtube.com/watch?v=css-anim-2025'
  }
];

export function detectMediaType(rawUrl) {
  const url = (rawUrl || '').trim();
  if (!url) return null;

  const lower = url.toLowerCase();

  // Caso 1: Error simulado o enlaces inválidos / privados
  if (
    lower.includes('error') ||
    lower.includes('private') ||
    lower.includes('auth=null') ||
    lower.includes('invalid') ||
    (!lower.startsWith('http://') && !lower.startsWith('https://') && !lower.includes('.'))
  ) {
    return {
      type: 'error',
      url,
      errorCode: '0x404_INVALID_NODE',
      message: 'No pudimos leer este enlace. Revisa que sea público.'
    };
  }

  // Caso 2: Audio (SoundCloud, Spotify, Bandcamp, o extensiones .mp3, .m4a, .wav, etc.)
  if (
    lower.includes('soundcloud.com') ||
    lower.includes('spotify.com') ||
    lower.includes('bandcamp.com') ||
    lower.includes('audio') ||
    lower.endsWith('.mp3') ||
    lower.endsWith('.m4a') ||
    lower.endsWith('.wav') ||
    lower.endsWith('.flac')
  ) {
    return {
      type: 'audio',
      url,
      title: 'Midnight City Lights',
      creator: 'Neon Dreamer',
      platform: lower.includes('spotify') ? 'Spotify' : 'SoundCloud',
      platformIcon: 'cloud',
      badgeColor: '#5FD99A',
      thumbnail: 'https://lh3.googleusercontent.com/aida-public/AB6AXuBWz-t5dDae1gXhZqs0RloFJNF2INDxkHrEQXxzwNZlEpLWnh7c3eV5phH1hHHb2dFhD9Yzu_MjmMNW6dx0NvrBYmt3SqCQnjyZrQf52YiX_0dGpOaS4-8mOnkwrBxkMqin7YEfJXuyp5X1TFSLIFl4EpzMEiKj708Bw0P085wAgbLwHYItuhYQtFu6iuPDaccEIddfWrQUiM3FI9y6cNZVDliYkW_9w0QLf4wLzrGABGcxOq4j-Yv5',
      duration: '3:45',
      options: [
        { id: 'mp3', label: 'MP3', quality: '320 kbps', size: '8.7 MB', isDefault: true },
        { id: 'm4a', label: 'M4A', quality: '256 kbps', size: '6.9 MB', isDefault: false }
      ]
    };
  }

  // Caso 3: Imagen / Galería (Instagram post p/, Flickr, Pinterest, o extensiones .jpg, .png, etc.)
  if (
    lower.includes('instagram.com/p/') ||
    lower.includes('pinterest.com') ||
    lower.includes('flickr.com') ||
    lower.includes('gallery') ||
    lower.includes('photo') ||
    lower.endsWith('.jpg') ||
    lower.endsWith('.png') ||
    lower.endsWith('.webp') ||
    lower.endsWith('.jpeg')
  ) {
    return {
      type: 'image',
      url,
      title: 'Fotografía urbana nocturna Tokio',
      creator: '@tokyo_cyber',
      platform: 'Instagram',
      platformIcon: 'photo_camera',
      badgeColor: '#E8B86A',
      photos: [
        'https://lh3.googleusercontent.com/aida-public/AB6AXuDi0ZU_cN5qYeVAQNwCA-ifDu9do1EeJ4kP-dQ7eQ19tcIiundlx4W5UfIcatzPLOkLuEhdDnNn6C0hAEDXH5N1g8Ee7HyT8cn4I4shTHfDoYDodlBfVRDWU_-hIHDALa4A7Os-jz9NhNQ_gouAP21t2AIJAy6ovxvblYxv2bylsrzTU15S91XlaRxlUSSgOAe4AsAG9KUUcvBfG88-e7Vz0V9r1CpxcBrv6yG1kCLS3ziy7PHeT4j9',
        'https://lh3.googleusercontent.com/aida-public/AB6AXuCZNqt3uHK1cSLypc6uFq-GpXHi1qRDc7kEpa3OR225Qx0FlnEWT7gaXsDuVwJfa0up2tJKOmCCzeW9tIxKBSk2q80kNQPbgh1ug2m48_OtqQralUGTSJOrchZCGOUAGAcjKIBoj_MvbToh-wLeTomAkT-AAP7T9RZJtIgS3aapuZt0NTDmizprjx19V4cSpxnTn7kWPICsnPXhuBe54cAseX7h4AXI6lBqa0qfVIIuFNWrCRzBHHdX',
        'https://lh3.googleusercontent.com/aida-public/AB6AXuAUfqB2Dw-hl2BGqm86RvoAZDykowyFGUDbMq3PR-efb_B8kkeY1ilW1c_EVnvmv7ccXfCrXd4zPNwJozeUGJ8yE73p0oHZQcrNd46dcW2V8T6-czW55e29IrtdQ_W7m6qzgcY3ra-isFvl6eHOtgJi2R9S1KUzcnBbCRYUGFkXu-HNM8qLIBRbYIXqIxPjPbKfzU-oLZReEJ65V3b09fE84PwjE70hnWSUylcXwbUWZYEf4S0Xcwir',
        'https://lh3.googleusercontent.com/aida-public/AB6AXuDIobSBg3g5mrrQ5R2U9GZr5oOirr6Tr8QzZBOOr3IWz8S5veLx81j3Ks5Ih3u7Mw8vhHM_PHHP6HUXr-dGmXGw-8rNGFq4GZt0MHciA_sD0yqhzi-IaniYe_UzI8ojrDWhva76Xm25YCpiRkNSfcuvF9Qx00pXVDLvr44o9jSKwTTMGy9_WW2UOK_TFQ7sqI5oVGsJn96f8Cv59YlQL4EpLdTGSNXEn9G3_zKsFtiURvAZ7cnsjRvp'
      ],
      options: [
        { id: 'jpg', label: 'JPG', size: '3.2 MB', isDefault: true },
        { id: 'png', label: 'PNG', size: '9.8 MB', isDefault: false }
      ]
    };
  }

  // Caso 4: Video (TikTok, YouTube, Reels, X, o predeterminado para enlaces de video)
  let platform = 'TikTok';
  let platformIcon = 'smart_display';
  if (lower.includes('youtube') || lower.includes('youtu.be')) {
    platform = 'YouTube';
    platformIcon = 'play_circle';
  } else if (lower.includes('instagram.com/reel')) {
    platform = 'Instagram Reels';
    platformIcon = 'video_library';
  } else if (lower.includes('x.com') || lower.includes('twitter.com')) {
    platform = 'X / Twitter';
    platformIcon = 'public';
  }

  return {
    type: 'video',
    url,
    title: 'Trend Dance 2025 #viral • Coreografía urbana',
    creator: '@trenddance',
    timeAgo: 'Publicado hace 3 horas',
    platform,
    platformIcon,
    badgeColor: '#6FB8E8',
    duration: '0:42',
    audioTag: 'Audio estéreo',
    thumbnail: 'https://lh3.googleusercontent.com/aida-public/AB6AXuDQ2_daIvjywJoOgPsAVwmsHeF7Jd4xbAfT9n4Q-diYRtU6GQU9VlY0g-TInHBGEhoaIJy2VxQRVg9mMMy81HSlbu9eR_Rdycxqx8aVlJub9-6k18jo9kcvQt_Qvf1xzFMqIQ84vkIjFUiTVSmZVaAe-ai3uWCxt_-OlmYdqiaWPuU7AZfPqcHBkO2O0iW4m5ST0KMSldzv1jgGqmh8RkbKaTyOdICzxdkR5i3fkZb9Ijvk-MxpLMGV',
    options: [
      { id: '1080p', label: '1080p (Full HD)', size: '18.4 MB', isDefault: true },
      { id: '720p', label: '720p (HD)', size: '11.2 MB', isDefault: false },
      { id: '480p', label: '480p (SD)', size: '6.1 MB', isDefault: false }
    ],
    audioOption: {
      id: 'mp3',
      label: 'MP3 • 2.4 MB'
    }
  };
}
