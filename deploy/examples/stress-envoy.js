import http from "k6/http"
import { check, sleep } from "k6"
import { Rate, Counter } from "k6/metrics"
import { textSummary } from "https://jslib.k6.io/k6-summary/0.0.2/index.js"

const errorRate = new Rate("errors")
const podHits = new Counter("pod_hits")

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
    http_req_duration: ["p(95)<500", "p(99)<1000"],
    errors: ["rate<0.05"], // Relaxed for debugging
  },
}

export default function () {
  // Added a 5s timeout so one slow pod doesn't ruin the whole test
  const res = http.get(__ENV.TARGET || "http://localhost:80", { timeout: "5s" })

  const ok = check(res, {
    "status 200": (r) => r.status === 200,
  })

  if (ok && res.body) {
    // Better Regex: Specifically looks for the 'pawn' and 'pod' values in your HTML
    const pawnMatch = res.body.match(/pawn<\/span><span class="value">([^<]+)<\/span>/)
    const podMatch = res.body.match(/pod<\/span><span class="value">([^<]+)<\/span>/)

    if (pawnMatch && podMatch) {
      podHits.add(1, { pawn: pawnMatch[1], pod: podMatch[1] })
    }
  } else {
    errorRate.add(1)
  }

  sleep(0.05)
}

export function handleSummary(data) {
  const pawnStats = {}
  const podStats = {}
  let totalHits = 0

  Object.keys(data.metrics).forEach(key => {
    if (key.startsWith("pod_hits")) {
      const count = data.metrics[key].values.count
      const pawn = key.match(/pawn:([^,}]+)/)?.[1]
      const pod = key.match(/pod:([^,}]+)/)?.[1]

      if (pawn && pod) {
        pawnStats[pawn] = (pawnStats[pawn] || 0) + count
        podStats[pod] = (podStats[pod] || 0) + count
        totalHits += count
      }
    }
  })

  const sortedPawns = Object.entries(pawnStats).sort((a, b) => b[1] - a[1])
  const sortedPods = Object.entries(podStats).sort((a, b) => b[1] - a[1])

  let report = `\n  █ CLUSTER LOAD DISTRIBUTION\n`
  report += `    Total Unique Pods Responded: ${Object.keys(podStats).length} / 2000\n\n`

  if (sortedPawns.length > 0) {
    report += `    TOP 10 NODES (PAWNS):\n`
    sortedPawns.slice(0, 10).forEach(([name, count]) => {
      const pct = ((count / totalHits) * 100).toFixed(1)
      report += `    - ${name.padEnd(20)}: ${count.toString().padStart(8)} hits (${pct}%)\n`
    })

    report += `\n    TOP 10 BUSIEST PODS:\n`
    sortedPods.slice(0, 10).forEach(([name, count]) => {
      report += `    - ${name.padEnd(45)}: ${count} hits\n`
    })
  }

  return {
    stdout: textSummary(data, { indent: " ", enableColors: true }) + report,
  }
}
