#!/usr/bin/env bash
set -eo pipefail

v=${1:-22}
bins=(clang llvm-config lld ld.lld FileCheck)

# Install prerequisites for llvm.sh.
apt-get update -qq
apt-get install -y --no-install-recommends \
    lsb-release wget software-properties-common gnupg ca-certificates

# Use the official LLVM install script which handles distro detection,
# GPG key import, and apt source configuration for all Debian/Ubuntu versions.
llvm_sh=$(mktemp)
wget -qO "$llvm_sh" https://apt.llvm.org/llvm.sh
chmod +x "$llvm_sh"
"$llvm_sh" "$v" all
rm -f "$llvm_sh"

for bin in "${bins[@]}"; do
    if ! command -v "$bin-$v" &>/dev/null; then
        echo "Warning: $bin-$v not found" 1>&2
        continue
    fi
    ln -fs "$(which "$bin-$v")" "/usr/bin/$bin"
done

echo "LLVM $v installed:"
llvm-config --version
