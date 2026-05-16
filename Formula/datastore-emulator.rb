class DatastoreEmulator < Formula
  desc "Google Cloud Datastore emulator written in Rust"
  homepage "https://github.com/guibeira/datastore-emulator"
  license "MIT"
  version "0.1.1"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.1/datastore-emulator-0.1.1-aarch64-apple-darwin.tar.gz"
      sha256 "26156a3d6af71bb74f4a49ac836b90ef8c9152b200685bf5ef46094c04d5121c"
    else
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.1/datastore-emulator-0.1.1-x86_64-apple-darwin.tar.gz"
      sha256 "5230cf7d095c54c8e9fed100701533cd73f18bb749b3101597727e1c855147b0"
    end
  end

  on_linux do
    url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.1/datastore-emulator-0.1.1-x86_64-unknown-linux-gnu.tar.gz"
    sha256 "10db28ecf5a72493d6f70139d391621d3ebd641bd15b2884e26cf2f73ea4464d"
  end

  def install
    bin.install "datastore-emulator"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/datastore-emulator --version")
  end
end
