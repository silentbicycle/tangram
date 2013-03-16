require 'formula'

class Tangram < Formula
  homepage 'https://github.com/silentbicycle/tangram'
  url 'https://github.com/silentbicycle/tangram/archive/v0.1-1.tar.gz'
  sha1 '34bd7022d0faad96145cd219872ed9d46b3598bb'
  version '0.1-1'

  depends_on 'lua'
  depends_on 'luarocks'
  depends_on 'hashchop'

  def install
    system 'luarocks install tangram-0.1-1.rockspec'
  end
  def test
    system 'tangram test'
  end
end
