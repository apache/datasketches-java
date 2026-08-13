#!/usr/bin/env bash

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

set -euo pipefail

# Pin the archive so compatibility tests always use an immutable snapshot set.
readonly TCK_REVISION="d363b12d293b395d90abb42677f9ea63178dbc0d"
readonly TCK_ARCHIVE_URL="https://api.github.com/repos/apache/datasketches-tck/tarball/${TCK_REVISION}"
readonly SCRIPT_DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPOSITORY_ROOT="$(cd "${SCRIPT_DIRECTORY}/.." && pwd)"
readonly SERIALIZATION_DATA="${REPOSITORY_ROOT}/serialization_test_data"

usage() {
  echo "Usage: $0 [cpp] [go]"
  echo "Download C++ and/or Go serialization snapshots (both by default)."
}

if [[ $# -eq 0 ]]; then
  set -- cpp go
fi

languages=()
for language in "$@"; do
  case "${language}" in
    cpp | go)
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "Unsupported language: ${language}" >&2
      usage >&2
      exit 2
      ;;
  esac

  case " ${languages[*]-} " in
    *" ${language} "*)
      ;;
    *)
      languages+=("${language}")
      ;;
  esac
done

for command in curl tar mktemp; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "Required command not found: ${command}" >&2
    exit 1
  fi
done

mkdir -p "${SERIALIZATION_DATA}"
temporary_directory="$(mktemp -d "${TMPDIR:-/tmp}/datasketches-tck.XXXXXX")"
staging_directory=""

cleanup() {
  rm -rf "${temporary_directory}"
  if [[ -n "${staging_directory}" && -d "${staging_directory}" ]]; then
    rm -rf "${staging_directory}"
  fi
}
trap cleanup EXIT

archive_path="${temporary_directory}/datasketches-tck.tar.gz"
echo "Downloading serialization snapshots from ${TCK_ARCHIVE_URL}"
curl \
  --fail \
  --location \
  --silent \
  --show-error \
  --connect-timeout 60 \
  --max-time 120 \
  --header "Accept: application/vnd.github+json" \
  --header "User-Agent: apache-datasketches-java" \
  --header "X-GitHub-Api-Version: 2022-11-28" \
  --output "${archive_path}" \
  "${TCK_ARCHIVE_URL}"

for language in "${languages[@]}"; do
  staging_directory="$(
    mktemp -d "${SERIALIZATION_DATA}/.${language}_generated_files.XXXXXX"
  )"
  count=0

  while IFS= read -r member; do
    case "${member}" in
      */serialization/"${language}"/snapshots/*.sk)
        name="${member##*/}"
        output="${staging_directory}/${name}"
        if [[ -e "${output}" || -L "${output}" ]]; then
          echo "Duplicate snapshot in archive: ${name}" >&2
          exit 1
        fi
        tar -xOzf "${archive_path}" "${member}" > "${output}"
        count=$((count + 1))
        ;;
    esac
  done < <(tar -tzf "${archive_path}")

  if [[ ${count} -eq 0 ]]; then
    echo "No ${language} snapshots found in the TCK archive" >&2
    exit 1
  fi

  destination="${SERIALIZATION_DATA}/${language}_generated_files"
  if [[ -L "${destination}" ]]; then
    echo "Snapshot output path cannot be a symbolic link: ${destination}" >&2
    exit 1
  fi
  if [[ -e "${destination}" && ! -d "${destination}" ]]; then
    echo "Snapshot output path is not a directory: ${destination}" >&2
    exit 1
  fi
  if [[ -d "${destination}" ]]; then
    rm -rf "${destination}"
  fi
  mv "${staging_directory}" "${destination}"
  staging_directory=""
  echo "Extracted ${count} ${language} snapshots into ${destination}"
done
