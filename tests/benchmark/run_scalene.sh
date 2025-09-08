#!/usr/bin/env bash
set -euo pipefail

# Run Scalene profiler on the Vortex benchmark.
# Requires: pip install scalene

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/../.."
PYTHON_BIN="${PYTHON_BIN:-${ROOT_DIR}/.venv/bin/python}"

OUT_DIR="${OUT_DIR:-${ROOT_DIR}/.bench_out/scalene}"
MODE="${MODE:---quick}"
LABEL="${LABEL:-scalene-run}"

mkdir -p "${OUT_DIR}"

echo "Running Scalene with output to: ${OUT_DIR}"

# Focus profiling on our modules to reduce noise (build as an array for safe quoting)
INCLUDE_FLAGS=(
  --cpu
  --memory
  --profile-interval 0.002
  --outfile "${OUT_DIR}/scalene_report.html"
  --html
  --reduced-profile
  --cli
  --program-path "${ROOT_DIR}"
  --profile-all
)

# Run benchmark under scalene; pass through args
"${PYTHON_BIN}" -m scalene "${INCLUDE_FLAGS[@]}" \
  "${ROOT_DIR}/tests/benchmark/vortex_benchmark.py" ${MODE} --instrument --profile-mem --run-label "${LABEL}" --out-dir "${OUT_DIR}" "$@" \
  | tee "${OUT_DIR}/scalene_report.txt"

echo "Scalene complete. Reports at:"
echo "  - ${OUT_DIR}/scalene_report.txt"
echo "  - ${OUT_DIR}/scalene_report.html"