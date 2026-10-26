class DatastoreEmulator < Formula
  desc "Google Cloud Datastore emulator written in Rust"
  homepage "https://github.com/guibeira/datastore-emulator"
  license "MIT"
  version "0.1.10"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.10/datastore-emulator-0.1.10-aarch64-apple-darwin.tar.gz"
      sha256 "35cbf99f5da1fb46dac602a2c54c715a40f8bbacf372aa7fda50a3cff8cec442"
    else
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.10/datastore-emulator-0.1.10-x86_64-apple-darwin.tar.gz"
      sha256 "430abc4e6bb79068806c741c8c54e8e197776bfb1c9ce8266f9db1066c2f77cf"
    end
  end

  on_linux do
    url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.10/datastore-emulator-0.1.10-x86_64-unknown-linux-gnu.tar.gz"
    sha256 "c65ff1ef7128a21525f4519839816e8df1afb8a11fc336b894230adda135fae5"
  end

  def install
    bin.install "datastore-emulator"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/datastore-emulator --version")
  end
end
