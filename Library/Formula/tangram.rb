require 'formula'

class Tangram < Formula
  homepage 'https://github.com/silentbicycle/tangram'
  url 'https://github.com/silentbicycle/tangram/archive/v0.1-0.tar.gz'
  sha1 '74e0c4000982139eae3abe82ce7f3d51b24f1dc4'
  version '0.1-0'

  depends_on 'lua'
  depends_on 'luarocks'
  depends_on 'hashchop'

  def install
    system 'luarocks install tangram-0.1-0.rockspec'
  end
  def test
    system 'tangram test'
  end
end
