class SqlInspect < Formula
  desc "Static SQL inspection and optional LLM-backed explanations"
  homepage "https://github.com/kraftaa/sql-inspect"
  version "0.1.3"
  license "MIT"

  on_macos do
    on_arm do
      url "https://github.com/kraftaa/sql-inspect/releases/download/v#{version}/sql-inspect-macos-aarch64.tar.gz"
      sha256 "e64d3fd365220a33c732d88a514fa9cf834cd118318f0d4cb965653b90d1cbce"
    end
  end

  on_linux do
    on_intel do
      url "https://github.com/kraftaa/sql-inspect/releases/download/v#{version}/sql-inspect-linux-x86_64.tar.gz"
      sha256 "dd817800e47dc6e81305eec1123dc672b7130d5baa7e00d445972019a823ba24"
    end
  end

  def install
    bin.install "sql-inspect"
  end

  test do
    output = shell_output("#{bin}/sql-inspect --help")
    assert_match "sql-inspect", output
  end
end
