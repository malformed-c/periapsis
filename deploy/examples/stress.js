import http from "k6/http"
import { check, sleep } from "k6"
import { Rate } from "k6/metrics"
import { textSummary } from "https://jslib.k6.io/k6-summary/0.0.2/index.js"

const errorRate = new Rate("errors")

// Target: ClusterIP service.
// Override: k6 run -e TARGET=http://<clusterip> stress.js
const TARGET = __ENV.TARGET || "http://10.43.48.129"

export const options = {
  scenarios: {
    ramp: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "15s", target: 100 },  // warm up
        { duration: "30s", target: 500 },  // ramp
        { duration: "60s", target: 500 },  // sustain
        { duration: "30s", target: 1000 },  // push
        { duration: "60s", target: 1000 },  // sustain peak
        { duration: "15s", target: 0 },  // cool down
      ],
    },
  },
  thresholds: {
    "http_req_duration{expected_response:true}": ["p(95)<500", "p(99)<2000"],
    errors: ["rate<0.05"],
  },
}

const vuPods = new Set()
const vuPawns = new Set()

export default function () {
  const res = http.get(TARGET, { timeout: "5s" })

  const ok = check(res, {
    "status 200": (r) => r.status === 200,
    "has body": (r) => r.body && r.body.length > 0,
    "has perigeos": (r) => r.body && r.body.includes("perigeos"),
  })

  errorRate.add(!ok)

  if (ok && res.body) {
    const pawnMatch = res.body.match(/pawn<\/span><span class="value">([^<]+)<\/span>/)
    const podMatch = res.body.match(/pod<\/span><span class="value">([^<]+)<\/span>/)

    if (pawnMatch && podMatch) {
      const pawn = pawnMatch[1].trim()
      const pod = podMatch[1].trim()
      check(res, { [`pawn: ${pawn}`]: () => true })
      vuPods.add(pod)
      vuPawns.add(pawn)
    } else {
      check(res, { "pawn: UNKNOWN": () => true })
    }
  }

  sleep(0.05)
}

export function handleSummary(data) {
  const pawnHits = {}
  let totalPawnHits = 0

  // data.root_group.checks is the correct place for named check results in
  // k6 OSS. data.metrics does NOT expose per-tag sub-metrics for check().
  const checks = data.root_group?.checks ?? []
  checks.forEach(c => {
    const m = c.name.match(/^pawn: (.+)$/)
    if (!m) return
    const pawn = m[1].trim()
    pawnHits[pawn] = (pawnHits[pawn] || 0) + c.passes
    totalPawnHits += c.passes
  })

  const sortedPawns = Object.entries(pawnHits).sort((a, b) => b[1] - a[1])

  let report = `\n  █ CLUSTER LOAD DISTRIBUTION (direct ClusterIP)\n`
  report += `    Unique pawns (nodes) responding : ${sortedPawns.length}\n`
  report += `    Unique pods (VU-1 view)         : ${vuPods.size} seen across ${vuPawns.size} pawns\n\n`

  if (sortedPawns.length > 0) {
    report += `    PAWN HIT DISTRIBUTION:\n`
    sortedPawns.forEach(([name, count]) => {
      const pct = totalPawnHits > 0 ? ((count / totalPawnHits) * 100).toFixed(1) : "0.0"
      const bar = "█".repeat(Math.round(parseFloat(pct) / 2))
      report += `    ${name.padEnd(22)} ${bar.padEnd(50)} ${pct.padStart(5)}%  (${count} hits)\n`
    })
  } else {
    report += `    No pawn data - body regex may not match or all requests failed.\n`
  }

  return {
    stdout: textSummary(data, { indent: " ", enableColors: true }) + report,
  }
}
