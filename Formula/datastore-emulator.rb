class DatastoreEmulator < Formula
  desc "Google Cloud Datastore emulator written in Rust"
  homepage "https://github.com/guibeira/datastore-emulator"
  license "MIT"
  version "0.1.5"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.5/datastore-emulator-0.1.5-aarch64-apple-darwin.tar.gz"
      sha256 "81ec7706c6cb129f80b7602b05eb7813eb638b42a42b324fce7590d8426cf765"
    else
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.5/datastore-emulator-0.1.5-x86_64-apple-darwin.tar.gz"
      sha256 "2f37c42bc9a24bf2f971a56a483004361e84a7634854e3a02b1bd4f50505d200"
    end
  end

  on_linux do
    url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.5/datastore-emulator-0.1.5-x86_64-unknown-linux-gnu.tar.gz"
    sha256 "3427e05d15c49a2f75f7f7b02eaa7f1e8e52025211ea77c0c98dbdfb9bf92b97"
  end

  def install
    bin.install "datastore-emulator"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/datastore-emulator --version")
  end
end
