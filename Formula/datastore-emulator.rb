class DatastoreEmulator < Formula
  desc "Google Cloud Datastore emulator written in Rust"
  homepage "https://github.com/guibeira/datastore-emulator"
  license "MIT"
  version "0.1.9"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.9/datastore-emulator-0.1.9-aarch64-apple-darwin.tar.gz"
      sha256 "8c32f8a2f9465996d4082dc5892eec77e2354f1cf7961403b18dd2661b3a601b"
    else
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.9/datastore-emulator-0.1.9-x86_64-apple-darwin.tar.gz"
      sha256 "d9fba57761b66b390df8f40d82083b94f01ed1b306e09f15d4811164e2ea41e6"
    end
  end

  on_linux do
    url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.9/datastore-emulator-0.1.9-x86_64-unknown-linux-gnu.tar.gz"
    sha256 "ea44dcbfa631790c022a90548fa9971a7d394faff93a57705890f18281685d7a"
  end

  def install
    bin.install "datastore-emulator"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/datastore-emulator --version")
  end
end
