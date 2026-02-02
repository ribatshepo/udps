#!/bin/bash
# =============================================================================
# UDPS Smoke Tests for CI/CD pipelines
# =============================================================================

set -e

UDPS_BASE_URL="${UDPS_BASE_URL:-http://localhost:8081}"
PASSED=0
FAILED=0
TOTAL=0

run_test() {
    local name="$1"
    local url="$2"
    local expected_status="${3:-200}"
    TOTAL=$((TOTAL + 1))

    echo -n "  Testing: ${name}... "

    HTTP_STATUS=$(curl -sf -o /dev/null -w "%{http_code}" --max-time 10 "${url}" 2>/dev/null || echo "000")

    if [[ "$HTTP_STATUS" == "$expected_status" ]]; then
        echo "PASS (${HTTP_STATUS})"
        PASSED=$((PASSED + 1))
    else
        echo "FAIL (expected ${expected_status}, got ${HTTP_STATUS})"
        FAILED=$((FAILED + 1))
    fi
}

echo "=============================================="
echo "UDPS Smoke Tests"
echo "=============================================="
echo "Target: ${UDPS_BASE_URL}"
echo ""

echo "Health Checks:"
run_test "Liveness probe" "${UDPS_BASE_URL}/health/live"
run_test "Readiness probe" "${UDPS_BASE_URL}/health/ready"
run_test "Health endpoint" "${UDPS_BASE_URL}/health"

echo ""
echo "Metrics:"
run_test "Prometheus metrics" "${UDPS_BASE_URL/8081/9090}/metrics"

echo ""
echo "=============================================="
echo "Results: ${PASSED}/${TOTAL} passed, ${FAILED} failed"
echo "=============================================="

if [[ $FAILED -gt 0 ]]; then
    echo "::error::${FAILED} smoke test(s) failed"
    exit 1
fi

echo "All smoke tests passed"
