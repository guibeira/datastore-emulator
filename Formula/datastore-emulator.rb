class DatastoreEmulator < Formula
  desc "Google Cloud Datastore emulator written in Rust"
  homepage "https://github.com/guibeira/datastore-emulator"
  license "MIT"
  version "0.1.4"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.4/datastore-emulator-0.1.4-aarch64-apple-darwin.tar.gz"
      sha256 "ded866cdfda17882dc320d5e8ae5462528b6748f1f1d6d0c181528173177461d"
    else
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.4/datastore-emulator-0.1.4-x86_64-apple-darwin.tar.gz"
      sha256 "b6a1d81e8abfff493f65b1fccab49a454412f094709d43a5a32b42353169fdd9"
    end
  end

  on_linux do
    url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.4/datastore-emulator-0.1.4-x86_64-unknown-linux-gnu.tar.gz"
    sha256 "9cb590df54a5071e7404fbe1ab9a49a2bb6b6c79e501d0cf523c68ff16c4d872"
  end

  def install
    bin.install "datastore-emulator"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/datastore-emulator --version")
  end
end
