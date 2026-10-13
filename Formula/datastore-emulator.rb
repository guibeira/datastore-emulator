class DatastoreEmulator < Formula
  desc "Google Cloud Datastore emulator written in Rust"
  homepage "https://github.com/guibeira/datastore-emulator"
  license "MIT"
  version "0.1.7"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.7/datastore-emulator-0.1.7-aarch64-apple-darwin.tar.gz"
      sha256 "72cffaf69a08c4c265b3152324f03766613d384f933aecabac91da3efe310fd9"
    else
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.7/datastore-emulator-0.1.7-x86_64-apple-darwin.tar.gz"
      sha256 "4e458051eb38e2e4dbe4a6293787b7b7f63f7ee835f171a2b4aed55631f13b5c"
    end
  end

  on_linux do
    url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.7/datastore-emulator-0.1.7-x86_64-unknown-linux-gnu.tar.gz"
    sha256 "1e985980e99cf930be60b902cc0f37de08afdf1b95b69ab12381bbdf88b04aa4"
  end

  def install
    bin.install "datastore-emulator"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/datastore-emulator --version")
  end
end
