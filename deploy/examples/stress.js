import http from "k6/http"
import { check, sleep } from "k6"
import { Rate } from "k6/metrics"

const errorRate = new Rate("errors")

// Target: ClusterIP service (override with -e TARGET=...)
const TARGET = __ENV.TARGET || "http://10.43.48.129"

export const options = {
  scenarios: {
    ramp: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "15s", target: 100 },
        { duration: "30s", target: 500 },
        { duration: "60s", target: 500 },
        { duration: "30s", target: 1000 },
        { duration: "60s", target: 1000 },
        { duration: "15s", target: 0 },
      ],
    },
  },
  thresholds: {
    http_req_duration: ["p(95)<500", "p(99)<3000"],
    errors: ["rate<0.01"],
  },
}

export default function () {
  const res = http.get(TARGET, { timeout: "5s" })

  const ok = check(res, {
    "status 200":   (r) => r.status === 200,
    "has body":     (r) => r.body && r.body.length > 0,
    "has perigeos": (r) => r.body && r.body.includes("perigeos"),
  })

  // Add 0 on success, 1 on failure so Rate = failures / total requests.
  errorRate.add(!ok)

  if (res.status === 200 && res.body) {
    const pawnMatch = res.body.match(/pawn<\/span><span class="value">([^<]+)<\/span>/)
    if (pawnMatch) {
      // Named checks per pawn accumulate across all VUs in k6's summary,
      // giving a hit-count breakdown per node without custom metrics.
      check(res, { [`pawn: ${pawnMatch[1].trim()}`]: () => true })
    } else {
      check(res, { "pawn: UNKNOWN": () => true })
    }
  }

  sleep(0.05)
}
