class DatastoreEmulator < Formula
  desc "Google Cloud Datastore emulator written in Rust"
  homepage "https://github.com/guibeira/datastore-emulator"
  license "MIT"
  version "0.1.12"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.12/datastore-emulator-0.1.12-aarch64-apple-darwin.tar.gz"
      sha256 "e17fe40b40322f35c00b09d48c02aa588d6f8dbbb327cf46ea128e4f75769722"
    else
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.12/datastore-emulator-0.1.12-x86_64-apple-darwin.tar.gz"
      sha256 "4b82d41183b39439a16cb6c9c22078bd85eeb0b66d3b3ca315287c0aafc868e3"
    end
  end

  on_linux do
    url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.12/datastore-emulator-0.1.12-x86_64-unknown-linux-gnu.tar.gz"
    sha256 "8a58b045a34daac9e8bcf05ec44ec9365928ee5cfa8f1ec5b2423f0b81c0912d"
  end

  def install
    bin.install "datastore-emulator"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/datastore-emulator --version")
  end
end
