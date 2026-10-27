class DatastoreEmulator < Formula
  desc "Google Cloud Datastore emulator written in Rust"
  homepage "https://github.com/guibeira/datastore-emulator"
  license "MIT"
  version "0.1.13"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.13/datastore-emulator-0.1.13-aarch64-apple-darwin.tar.gz"
      sha256 "39a0f975771cea27374251fa5b58bd71ad2f846127e0bc55cd4b0664346cb9fc"
    else
      url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.13/datastore-emulator-0.1.13-x86_64-apple-darwin.tar.gz"
      sha256 "309259adf30b7bcac5d78ee5cd7e0668f480196c75394c806a3b2be195419c32"
    end
  end

  on_linux do
    url "https://github.com/guibeira/datastore-emulator/releases/download/v0.1.13/datastore-emulator-0.1.13-x86_64-unknown-linux-gnu.tar.gz"
    sha256 "86989c6aa40949de3a93d2af74f79b22740deaf8f2d66c95e7cab97edf7f0fab"
  end

  def install
    bin.install "datastore-emulator"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/datastore-emulator --version")
  end
end
