import http from "k6/http";
import { check, sleep } from "k6";
import { Rate, Trend } from "k6/metrics";

// Custom metrics
const errorRate = new Rate("errors");
const podDistribution = new Trend("unique_pods_seen");

// Target: ClusterIP service (override with -e TARGET=...)
const TARGET = __ENV.TARGET || "http://10.43.48.129";

export const options = {
  scenarios: {
    // Ramp: gentle start -> full load -> sustained -> cool down
    ramp: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "15s", target: 100 },   // warm up
        { duration: "30s", target: 500 },   // ramp to 500 concurrent
        { duration: "60s", target: 500 },   // sustain
        { duration: "30s", target: 1000 },  // push to 1k
        { duration: "60s", target: 1000 },  // sustain peak
        { duration: "15s", target: 0 },     // cool down
      ],
    },
  },
  thresholds: {
    http_req_duration: ["p(95)<500", "p(99)<1000"],
    errors: ["rate<0.01"],
  },
};

export default function () {
  const res = http.get(TARGET);

  const ok = check(res, {
    "status 200": (r) => r.status === 200,
    "has body": (r) => r.body && r.body.length > 0,
    "has perigeos": (r) => r.body && r.body.includes("perigeos"),
  });

  errorRate.add(!ok);
  sleep(0.05); // 50ms think time between requests
}
