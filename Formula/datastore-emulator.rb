class DatastoreEmulator < Formula
  desc "Google Cloud Datastore emulator written in Rust"
  homepage "https://github.com/guibeira/datastore-emulator"
  license "MIT"
  version "0.1.11"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.11/datastore-emulator-0.1.11-aarch64-apple-darwin.tar.gz"
      sha256 "69e61474260d1a705f1543341383631f4dc317a68aa21447f32ce7fcee5f2166"
    else
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.11/datastore-emulator-0.1.11-x86_64-apple-darwin.tar.gz"
      sha256 "77d4ba9d7cb646b19ce6caa9ce74f37906da8e425ac2d51542daf748a5be666d"
    end
  end

  on_linux do
    url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.11/datastore-emulator-0.1.11-x86_64-unknown-linux-gnu.tar.gz"
    sha256 "8b4077c4ea88fe6282f87fdc470e80ae820943b38cac4b30593446a647263d1c"
  end

  def install
    bin.install "datastore-emulator"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/datastore-emulator --version")
  end
end
