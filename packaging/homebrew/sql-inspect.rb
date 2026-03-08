class SqlInspect < Formula
  desc "Static SQL inspection and optional LLM-backed explanations"
  homepage "https://github.com/kraftaa/sql-inspect"
  version "__VERSION__"
  license "MIT"

  on_macos do
    on_arm do
      url "https://github.com/kraftaa/sql-inspect/releases/download/v#{version}/sql-inspect-macos-aarch64.tar.gz"
      sha256 "__SHA256_MACOS_AARCH64__"
    end
  end

  on_linux do
    on_intel do
      url "https://github.com/kraftaa/sql-inspect/releases/download/v#{version}/sql-inspect-linux-x86_64.tar.gz"
      sha256 "__SHA256_LINUX_X86_64__"
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
