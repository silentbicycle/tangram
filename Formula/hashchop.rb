require 'formula'

class Hashchop < Formula
  homepage 'https://github.com/silentbicycle/hashchop'
  url 'https://github.com/silentbicycle/hashchop/archive/master.tar.gz'
  sha1 '3452e20fb41e5a0f04a09b69e9978587030dfd75'
  version '0.8-0'

  depends_on 'lua'
  depends_on 'luarocks'

  def install
    system 'luarocks install hashchop-0.8-0.rockspec'
  end
  
end
