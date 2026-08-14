import type { RingBuffer } from '../types/metrics'
import { latestValue, ringToArrays } from '../types/metrics'
import { seriesKey } from './metricsStore'

export function computeRate(buf: RingBuffer): number {
  if (buf.filled < 2) return 0
  const { ts, v } = ringToArrays(buf)
  const dt = (ts[ts.length - 1] - ts[0]) / 1000
  if (dt <= 0) return 0
  const dv = v[v.length - 1] - v[0]
  if (dv < 0) return 0
  return dv / dt
}

export function computeTasksPerMinute(buf: RingBuffer): number {
  return computeRate(buf) * 60
}

export interface HistogramBucket {
  le: number
  count: number
}

export function computeP95FromBuckets(buckets: HistogramBucket[]): number {
  if (buckets.length === 0) return 0
  const sorted = [...buckets].sort((a, b) => a.le - b.le)
  const total = sorted[sorted.length - 1].count
  if (total <= 0) return 0
  const target = total * 0.95
  let prevLe = 0
  let prevCount = 0
  for (const b of sorted) {
    if (b.count >= target) {
      if (!Number.isFinite(b.le)) {
        const lastFinite = sorted
          .filter((x) => Number.isFinite(x.le))
          .map((x) => x.le)
          .pop()
        return lastFinite ?? 0
      }
      const bucketSize = b.count - prevCount
      if (bucketSize <= 0) return b.le
      const frac = (target - prevCount) / bucketSize
      return prevLe + frac * (b.le - prevLe)
    }
    prevLe = b.le
    prevCount = b.count
  }
  return sorted[sorted.length - 1].le
}

export { latestValue }

// Must enumerate EVERY status the worker emits for agentfm_tasks_total: this
// list is the denominator of the success rate, so a missing value makes those
// tasks vanish from the chart entirely rather than counting as failures. A
// worker whose memory ceiling kills every task would otherwise show a 100%
// success rate — the failure becomes less visible, not more.
// Source of truth: internal/metrics/metrics.go (Status* constants).
const TASK_STATUSES = ['ok', 'error', 'rejected', 'timeout', 'oom_killed'] as const

export function computeSuccessRateSeries(
  buffers: Map<string, RingBuffer>,
): number[] {
  const okBuf = buffers.get(seriesKey('agentfm_tasks_total', { status: 'ok' }))
  if (!okBuf) return []
  const okValues = ringToArrays(okBuf).v
  if (okValues.length === 0) return []

  const otherValues = TASK_STATUSES.filter((s) => s !== 'ok').map((status) => {
    const buf = buffers.get(seriesKey('agentfm_tasks_total', { status }))
    if (!buf) return new Array<number>(okValues.length).fill(0)
    return ringToArrays(buf).v
  })

  const out: number[] = []
  for (let i = 0; i < okValues.length; i++) {
    const ok = okValues[i] ?? 0
    let other = 0
    for (const series of otherValues) {
      other += series[i] ?? 0
    }
    const denom = ok + other
    out.push(denom === 0 ? 1 : ok / denom)
  }
  return out
}
