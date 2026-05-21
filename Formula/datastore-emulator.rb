class DatastoreEmulator < Formula
  desc "Google Cloud Datastore emulator written in Rust"
  homepage "https://github.com/guibeira/datastore-emulator"
  license "MIT"
  version "0.1.3"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.3/datastore-emulator-0.1.3-aarch64-apple-darwin.tar.gz"
      sha256 "1e4e88516d67dd2de000aff68c744162317560adb3924862a1fea426c8ced044"
    else
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.3/datastore-emulator-0.1.3-x86_64-apple-darwin.tar.gz"
      sha256 "d167266331dcf1172097996ab4e244eb28b804764446897776be4bf0755eeec8"
    end
  end

  on_linux do
    url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.3/datastore-emulator-0.1.3-x86_64-unknown-linux-gnu.tar.gz"
    sha256 "3aef3bab4a348e803b3df739b6b65d7b0776e0df7a8496caa55efd0291d75fbe"
  end

  def install
    bin.install "datastore-emulator"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/datastore-emulator --version")
  end
end
