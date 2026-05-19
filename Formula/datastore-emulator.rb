class DatastoreEmulator < Formula
  desc "Google Cloud Datastore emulator written in Rust"
  homepage "https://github.com/guibeira/datastore-emulator"
  license "MIT"
  version "0.1.2"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.2/datastore-emulator-0.1.2-aarch64-apple-darwin.tar.gz"
      sha256 "7b3d4bd86dd211e0fcd6a79ebb7df67fa5c9f5d3c54e0de987aade9ba646e1fa"
    else
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.2/datastore-emulator-0.1.2-x86_64-apple-darwin.tar.gz"
      sha256 "fc0aff118ad9d2e64c71418da6174833bf302e78ce0841a5c872c80a92e6e92e"
    end
  end

  on_linux do
    url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.2/datastore-emulator-0.1.2-x86_64-unknown-linux-gnu.tar.gz"
    sha256 "3e336554305bf6ecbefc3ded454b06a2afde6a9b1964a168aa41a3db9ed13d80"
  end

  def install
    bin.install "datastore-emulator"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/datastore-emulator --version")
  end
end
