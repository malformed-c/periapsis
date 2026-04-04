import http from "k6/http";
import { check, sleep } from "k6";
import { Rate } from "k6/metrics";

const errorRate = new Rate("errors");

// Target: Envoy Gateway on hostNetwork port 80 (override with -e TARGET=...)
const TARGET = __ENV.TARGET || "http://localhost:80";

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
  sleep(0.05);
}
