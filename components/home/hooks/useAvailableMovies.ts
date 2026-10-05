'use client';

/**
 * useAvailableMovies
 *
 * 过滤掉"点进去搜不到任何源"的豆瓣推荐条目，避免出现打开后是空结果的海报。
 *
 * 可用性探测复用现有的 /api/search-parallel（服务端搜索，SSE）接口，
 * 带有并发限制与本地缓存，避免每次进入首页都重新探测。
 */

import { useEffect, useMemo, useRef, useState } from 'react';
import { settingsStore } from '@/lib/store/settings-store';
import { userSourcesStore } from '@/lib/store/user-sources-store';
import type { VideoSource } from '@/lib/types';

export interface DoubanMovie {
  id: string;
  title: string;
  cover: string;
  rate: string;
  url: string;
}

type Availability = 'available' | 'unavailable';

interface CacheRecord {
  status: Availability;
  fingerprint: string;
  at: number;
}

const CACHE_KEY = 'kvideo-douban-availability-v1';
const CACHE_TTL_MS = 6 * 60 * 60 * 1000; // 6 小时
const PROBE_CONCURRENCY = 3;

function readCache(): Record<string, CacheRecord> {
  if (typeof window === 'undefined') return {};
  try {
    const raw = window.localStorage.getItem(CACHE_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw) as Record<string, CacheRecord>;
    const now = Date.now();
    const result: Record<string, CacheRecord> = {};
    for (const [title, record] of Object.entries(parsed)) {
      if (record && typeof record.at === 'number' && now - record.at < CACHE_TTL_MS) {
        result[title] = record;
      }
    }
    return result;
  } catch {
    return {};
  }
}

function writeCache(cache: Record<string, CacheRecord>): void {
  if (typeof window === 'undefined') return;
  try {
    window.localStorage.setItem(CACHE_KEY, JSON.stringify(cache));
  } catch {
    // 忽略写入失败（隐私模式 / 超出配额）
  }
}

function getEnabledSources(): VideoSource[] {
  const settings = settingsStore.getSettings();
  const enabled = settings.sources.filter((source) => source.enabled);
  const userSources = userSourcesStore
    .getSources()
    .filter((source) => source.enabled !== false) as unknown as VideoSource[];

  const merged: VideoSource[] = [...enabled];
  for (const source of userSources) {
    if (!merged.find((item) => item.id === source.id)) {
      merged.push(source);
    }
  }
  return merged;
}

/**
 * 探测单个标题是否有任意源能搜到结果。
 * - 一旦收到含视频的 SSE 事件即判定为 available，并立即中断请求；
 * - 流正常结束且始终没有视频时判定为 unavailable；
 * - 网络/解析失败时一律按 available 处理（fail-open，避免误隐藏）。
 */
async function probeTitle(
  query: string,
  sources: VideoSource[],
  signal: AbortSignal
): Promise<Availability> {
  const controller = new AbortController();
  const onAbort = () => controller.abort();
  signal.addEventListener('abort', onAbort, { once: true });

  try {
    const response = await fetch('/api/search-parallel', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query, sources }),
      signal: controller.signal,
    });

    if (!response.ok || !response.body) return 'available';

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let hasVideo = false;
    let totalFound = 0;

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const chunks = buffer.split('\n\n');
        buffer = chunks.pop() ?? '';

        for (const chunk of chunks) {
          const dataLine = chunk.split('\n').find((line) => line.startsWith('data:'));
          if (!dataLine) continue;

          let event: { type?: string; videos?: unknown[]; totalVideosFound?: number };
          try {
            event = JSON.parse(dataLine.slice(5).trim());
          } catch {
            continue;
          }

          if (event.type === 'videos' && Array.isArray(event.videos) && event.videos.length > 0) {
            hasVideo = true;
          } else if (typeof event.totalVideosFound === 'number') {
            totalFound = event.totalVideosFound;
          }
        }

        if (hasVideo) break;
      }
    } finally {
      try {
        await reader.cancel();
      } catch {
        // 忽略
      }
    }

    return hasVideo || totalFound > 0 ? 'available' : 'unavailable';
  } catch {
    return 'available';
  } finally {
    signal.removeEventListener('abort', onAbort);
    controller.abort();
  }
}

export function useAvailableMovies(movies: DoubanMovie[]): DoubanMovie[] {
  const [statuses, setStatuses] = useState<Record<string, Availability>>({});
  const statusesRef = useRef<Record<string, Availability>>({});
  const pendingRef = useRef<Set<string>>(new Set());
  const cacheRef = useRef<Record<string, CacheRecord>>({});
  const [settingsVersion, setSettingsVersion] = useState(0);

  // 载入本地缓存，先按缓存结果渲染（避免首屏闪动）
  useEffect(() => {
    const cache = readCache();
    cacheRef.current = cache;

    const initial: Record<string, Availability> = {};
    for (const [title, record] of Object.entries(cache)) {
      initial[title] = record.status;
    }
    statusesRef.current = { ...statusesRef.current, ...initial };
    setStatuses((prev) => ({ ...prev, ...initial }));
  }, []);

  // 源配置异步加载完成后需要重新触发探测
  useEffect(() => {
    const unsubscribe = settingsStore.subscribe(() => {
      setSettingsVersion((version) => version + 1);
    });
    return () => unsubscribe();
  }, []);

  useEffect(() => {
    const sources = getEnabledSources();
    if (sources.length === 0) return;

    const fingerprint = sources
      .map((source) => source.id)
      .sort()
      .join('|');

    const titles = Array.from(new Set(movies.map((movie) => movie.title).filter(Boolean))).filter(
      (title) => {
        const cached = cacheRef.current[title];
        const fresh =
          cached && cached.fingerprint === fingerprint && Date.now() - cached.at < CACHE_TTL_MS;
        if (fresh) return false;
        return !pendingRef.current.has(title);
      }
    );

    if (titles.length === 0) return;

    const controller = new AbortController();
    let index = 0;

    const worker = async () => {
      while (index < titles.length) {
        if (controller.signal.aborted) return;
        const title = titles[index++];
        if (pendingRef.current.has(title)) continue;

        pendingRef.current.add(title);
        try {
          const status = await probeTitle(title, sources, controller.signal);
          if (controller.signal.aborted) return;

          statusesRef.current = { ...statusesRef.current, [title]: status };
          cacheRef.current = {
            ...cacheRef.current,
            [title]: { status, fingerprint, at: Date.now() },
          };
          writeCache(cacheRef.current);
          setStatuses((prev) => ({ ...prev, [title]: status }));
        } finally {
          pendingRef.current.delete(title);
        }
      }
    };

    const workers = Array.from(
      { length: Math.min(PROBE_CONCURRENCY, titles.length) },
      () => worker()
    );
    Promise.all(workers).catch(() => {
      // 忽略单个探测失败
    });

    return () => controller.abort();
  }, [movies, settingsVersion]);

  return useMemo(
    () => movies.filter((movie) => statuses[movie.title] !== 'unavailable'),
    [movies, statuses]
  );
}
