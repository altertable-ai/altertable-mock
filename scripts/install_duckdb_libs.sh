#!/bin/bash -e

VER="v1.4.1"

INST="${1}"
EXT_DIR="${2}"

run_as_root() {
    if command -v sudo >/dev/null 2>&1; then
        sudo "$@"
    else
        if [ "$(id -u)" -eq 0 ]; then
            "$@"
        else
            echo "Error: Need root privileges to run '$*'. Please install sudo or run as root." >&2
            exit 1
        fi
    fi
}

main () {
    OS="$(uname -s)"
    ARCH="$(uname -m)"

    command -v curl >/dev/null 2>&1 || { echo >&2 "Required tool curl could not be found. Aborting."; exit 1; }
    command -v unzip >/dev/null 2>&1 || { echo >&2 "Required tool unzip could not be found. Aborting."; exit 1; }
    command -v gzip >/dev/null 2>&1 || { echo >&2 "Required tool unzip could not be found. Aborting."; exit 1; }


    DUCKDB_DIST=linux-amd64
    EXTENSION_DIST=linux_amd64

    if [ -z "${INST}" ]; then
        echo "Usage: ${0} <install-path> [<extension-path>]"
        exit 1
    fi

    mkdir -p "${INST}"

    URL="https://github.com/duckdb/duckdb/releases/download/${VER}/libduckdb-${DUCKDB_DIST}.zip"

    curl --fail --location --progress-bar "${URL}" -o "libduckdb-${DUCKDB_DIST}.zip" || exit 1
    yes | run_as_root unzip "libduckdb-${DUCKDB_DIST}.zip" -d "${INST}" || exit 1
    rm "libduckdb-${DUCKDB_DIST}.zip"

    echo "Successfully installed DuckDB libraries to ${INST}"
}

main
