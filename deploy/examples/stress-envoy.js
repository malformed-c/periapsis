import http from "k6/http"
import { check, sleep } from "k6"
import { Rate } from "k6/metrics"
import { textSummary } from "https://jslib.k6.io/k6-summary/0.0.2/index.js"

const errorRate = new Rate("errors")

const TARGET = __ENV.TARGET || "http://localhost:80"

export const options = {
  scenarios: {
    ramp: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "15s", target: 100 },
        { duration: "30s", target: 500 },
        { duration: "60s", target: 1000 },
        { duration: "60s", target: 1000 },
        { duration: "15s", target: 0 },
      ],
    },
  },
  thresholds: {
    // p(99) is intentionally lenient: a 5s timeout means worst-case latency
    // for slow pods is ~5s, and we care more about p(95) in normal operation.
    http_req_duration: ["p(95)<500", "p(99)<3000"],
    errors: ["rate<0.01"],
  },
}

// VU-local state: accumulated per this VU's lifetime.
// k6 runs each VU in its own JS context so these are not shared across VUs,
// but that is fine — we aggregate in handleSummary via named checks.
const vuPawns = new Set()
const vuPods  = new Set()

export default function () {
  const res = http.get(TARGET, { timeout: "5s" })

  const ok = check(res, {
    "status 200": (r) => r.status === 200,
    "has body":   (r) => r.body && r.body.length > 0,
  })

  // Fix: add to errorRate on every request (ok→0, fail→1) so Rate denominator
  // is total requests, not just failures. Previously only errorRate.add(1) was
  // called on failure which made Rate always report 100%.
  errorRate.add(!ok)

  if (ok && res.body) {
    const pawnMatch = res.body.match(/pawn<\/span><span class="value">([^<]+)<\/span>/)
    const podMatch  = res.body.match(/pod<\/span><span class="value">([^<]+)<\/span>/)

    if (pawnMatch && podMatch) {
      const pawn = pawnMatch[1].trim()
      const pod  = podMatch[1].trim()

      // Named checks per pawn — these accumulate across all VUs and appear in
      // k6's built-in summary, giving a hit-count breakdown per node.
      // Pods are too numerous to do the same (thousands of check names), so we
      // track uniqueness only via the VU-local Sets.
      check(res, { [`pawn: ${pawn}`]: () => true })

      vuPawns.add(pawn)
      vuPods.add(pod)
    }
  }

  sleep(0.05)
}

// Module-level accumulators for teardown. Not cross-VU in OSS k6, so we
// can only report what the first VU's context sees. The named checks above
// give the real per-pawn distribution; this section shows unique pod count
// as seen by VU 1 (a lower bound on the real unique-pod count).
const seenPods  = new Set()
const seenPawns = new Set()

export function teardown() {
  vuPods.forEach(p  => seenPods.add(p))
  vuPawns.forEach(p => seenPawns.add(p))
}

export function handleSummary(data) {
  // Harvest pawn distribution from named checks.
  const pawnHits = {}
  let totalPawnHits = 0

  Object.entries(data.metrics).forEach(([key, metric]) => {
    if (!key.startsWith("checks{")) return
    const nameMatch = key.match(/check:pawn: ([^}]+)/)
    if (!nameMatch) return
    const pawn  = nameMatch[1].trim()
    const count = metric.values?.passes ?? 0
    pawnHits[pawn] = (pawnHits[pawn] || 0) + count
    totalPawnHits += count
  })

  const sortedPawns = Object.entries(pawnHits).sort((a, b) => b[1] - a[1])

  let report = `\n  █ CLUSTER LOAD DISTRIBUTION\n`
  report += `    Unique pawns (nodes) responding : ${sortedPawns.length}\n`
  report += `    Unique pods seen by VU-1        : ${seenPods.size} (lower bound)\n\n`

  if (sortedPawns.length > 0) {
    report += `    PAWN HIT DISTRIBUTION:\n`
    sortedPawns.forEach(([name, count]) => {
      const pct = totalPawnHits > 0 ? ((count / totalPawnHits) * 100).toFixed(1) : "0.0"
      const bar = "█".repeat(Math.round(parseFloat(pct) / 2))
      report += `    ${name.padEnd(22)} ${bar.padEnd(50)} ${pct.padStart(5)}%  (${count} hits)\n`
    })
  } else {
    report += `    No pawn data — body regex may not match or all requests failed.\n`
  }

  return {
    stdout: textSummary(data, { indent: " ", enableColors: true }) + report,
  }
}
