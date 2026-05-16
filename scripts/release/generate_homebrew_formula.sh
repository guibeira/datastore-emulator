#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 3 ]; then
  echo "usage: $0 <version> <checksums_file> <output_file> [owner] [repo]" >&2
  exit 1
fi

version="$1"
checksums_file="$2"
output_file="$3"
owner="${4:-guibeira}"
repo="${5:-datastore-emulator}"

if [ ! -f "${checksums_file}" ]; then
  echo "checksums file not found: ${checksums_file}" >&2
  exit 1
fi

linux_tar="datastore-emulator-${version}-x86_64-unknown-linux-gnu.tar.gz"
darwin_x64_tar="datastore-emulator-${version}-x86_64-apple-darwin.tar.gz"
darwin_arm64_tar="datastore-emulator-${version}-aarch64-apple-darwin.tar.gz"

lookup_sha() {
  local tar_name="$1"
  awk -v target="${tar_name}" '$2 == target { print $1 }' "${checksums_file}"
}

linux_sha="$(lookup_sha "${linux_tar}")"
darwin_x64_sha="$(lookup_sha "${darwin_x64_tar}")"
darwin_arm64_sha="$(lookup_sha "${darwin_arm64_tar}")"

test -n "${linux_sha}"
test -n "${darwin_x64_sha}"
test -n "${darwin_arm64_sha}"

mkdir -p "$(dirname "${output_file}")"

cat > "${output_file}" <<EOF
class DatastoreEmulator < Formula
  desc "Google Cloud Datastore emulator written in Rust"
  homepage "https://github.com/${owner}/${repo}"
  license "MIT"
  version "${version}"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/${owner}/${repo}/releases/download/v${version}/${darwin_arm64_tar}"
      sha256 "${darwin_arm64_sha}"
    else
      url "https://github.com/${owner}/${repo}/releases/download/v${version}/${darwin_x64_tar}"
      sha256 "${darwin_x64_sha}"
    end
  end

  on_linux do
    url "https://github.com/${owner}/${repo}/releases/download/v${version}/${linux_tar}"
    sha256 "${linux_sha}"
  end

  def install
    bin.install "datastore-emulator"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/datastore-emulator --version")
  end
end
EOF
