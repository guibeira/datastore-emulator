class DatastoreEmulator < Formula
  desc "Google Cloud Datastore emulator written in Rust"
  homepage "https://github.com/guibeira/datastore-emulator"
  license "MIT"
  version "0.1.8"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.8/datastore-emulator-0.1.8-aarch64-apple-darwin.tar.gz"
      sha256 "d5afcc3583a1ecec06c4cb9e6701bd9d272755d077a7081039645bc66e8635cb"
    else
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.8/datastore-emulator-0.1.8-x86_64-apple-darwin.tar.gz"
      sha256 "b09f04f5e5cc2599e362847734c625cbcef49c2e1b1f2b57e25b4bf17df0f920"
    end
  end

  on_linux do
    url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.8/datastore-emulator-0.1.8-x86_64-unknown-linux-gnu.tar.gz"
    sha256 "659c0f7cd6c622f4f671c0d8d76fd05170601414f2602bf5c096ee80bbcf55f2"
  end

  def install
    bin.install "datastore-emulator"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/datastore-emulator --version")
  end
end
