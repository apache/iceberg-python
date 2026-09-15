#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Verifies a PyIceberg release candidate, following the steps in
# mkdocs/docs/verify-release.md: signatures, checksums, license
# documentation (RAT), and the test suite of the source distribution.

set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <version>"
  echo " e.g.: $0 0.6.1rc3"
  exit 1
fi

PYICEBERG_VERSION="$1"
# remove the rcX qualifier, the artifacts inside the RC are named after the release
PYICEBERG_RELEASE_VERSION="${PYICEBERG_VERSION%rc*}"
PYICEBERG_VERIFICATION_DIR="${PYICEBERG_VERIFICATION_DIR:-/tmp/pyiceberg/${PYICEBERG_VERSION}}"

# Set to 0 to skip a step, e.g. VERIFY_TEST=0 ./dev/release/verify_rc.sh 0.6.1rc3
: "${VERIFY_SIGN:=1}"
: "${VERIFY_CHECKSUM:=1}"
: "${VERIFY_LICENSE:=1}"
: "${VERIFY_TEST:=1}"

if type shasum >/dev/null 2>&1; then
  sha512_verify="shasum -a 512 --check"
else
  sha512_verify="sha512sum --check"
fi

import_gpg_keys() {
  echo "--- Importing KEYS"
  curl --fail --location --show-error --silent https://downloads.apache.org/iceberg/KEYS | gpg --import
}

download_rc() {
  echo "--- Downloading pyiceberg-${PYICEBERG_VERSION}"
  svn checkout "https://dist.apache.org/repos/dist/dev/iceberg/pyiceberg-${PYICEBERG_VERSION}/" "${PYICEBERG_VERIFICATION_DIR}"
}

verify_signatures() {
  echo "--- Verifying signatures"
  for name in pyiceberg-*.whl pyiceberg-*.tar.gz; do
    gpg --verify "${name}.asc" "${name}"
  done
}

verify_checksums() {
  echo "--- Verifying checksums"
  for name in pyiceberg-*.whl.sha512 pyiceberg-*.tar.gz.sha512; do
    ${sha512_verify} "${name}"
  done
}

extract_source_distribution() {
  echo "--- Extracting pyiceberg-${PYICEBERG_RELEASE_VERSION}.tar.gz"
  tar xzf "pyiceberg-${PYICEBERG_RELEASE_VERSION}.tar.gz"
}

verify_license_documentation() {
  echo "--- Running RAT checks"
  ./dev/check-license
}

test_source_distribution() {
  echo "--- Installing and running the tests, this spins up Docker containers"
  make install
  make test-coverage
}

echo "Verifying pyiceberg-${PYICEBERG_VERSION} in ${PYICEBERG_VERIFICATION_DIR}"

if [ "${VERIFY_SIGN}" -gt 0 ]; then
  import_gpg_keys
fi

download_rc
cd "${PYICEBERG_VERIFICATION_DIR}"

if [ "${VERIFY_SIGN}" -gt 0 ]; then
  verify_signatures
fi

if [ "${VERIFY_CHECKSUM}" -gt 0 ]; then
  verify_checksums
fi

extract_source_distribution
cd "pyiceberg-${PYICEBERG_RELEASE_VERSION}"

if [ "${VERIFY_LICENSE}" -gt 0 ]; then
  verify_license_documentation
fi

if [ "${VERIFY_TEST}" -gt 0 ]; then
  test_source_distribution
fi

echo "RC looks good! Cast your vote on the dev mailing list."
