class DatastoreEmulator < Formula
  desc "Google Cloud Datastore emulator written in Rust"
  homepage "https://github.com/guibeira/datastore-emulator"
  license "MIT"
  version "0.1.14"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.14/datastore-emulator-0.1.14-aarch64-apple-darwin.tar.gz"
      sha256 "cb29ca67ab62077a8d5cf06974475abddf881aa519efcf4f53ac1165769fdad8"
    else
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.14/datastore-emulator-0.1.14-x86_64-apple-darwin.tar.gz"
      sha256 "9eed296b86f537a847773f3d4103847729c5ad2d20a5b2a162d1b5bf49258f82"
    end
  end

  on_linux do
    url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.14/datastore-emulator-0.1.14-x86_64-unknown-linux-gnu.tar.gz"
    sha256 "b322e1aa51a9c1f16362c5f58d45602a6b20d2122150879a1792d32cad97d1b1"
  end

  def install
    bin.install "datastore-emulator"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/datastore-emulator --version")
  end
end
