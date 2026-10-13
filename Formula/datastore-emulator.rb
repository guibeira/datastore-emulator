class DatastoreEmulator < Formula
  desc "Google Cloud Datastore emulator written in Rust"
  homepage "https://github.com/guibeira/datastore-emulator"
  license "MIT"
  version "0.1.6"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.6/datastore-emulator-0.1.6-aarch64-apple-darwin.tar.gz"
      sha256 "e290c840153b72b34c8d35fbcc2900240192ab5a7af512c8350423cf97c01b2d"
    else
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.6/datastore-emulator-0.1.6-x86_64-apple-darwin.tar.gz"
      sha256 "c6a5673f0fa6fa8281c87ff593917cf7fbc6a252768cfbacf994a3514ae9f3ea"
    end
  end

  on_linux do
    url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.6/datastore-emulator-0.1.6-x86_64-unknown-linux-gnu.tar.gz"
    sha256 "d599c0a399e7846d19a089fad97e333b480d4aa8bf76e92c3dfe048e102600fd"
  end

  def install
    bin.install "datastore-emulator"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/datastore-emulator --version")
  end
end
