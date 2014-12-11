
require 'msgpack'

msg = ["test", [[Time.now.to_i, ARGV[0]]], {'chunk' => 1}]
print MessagePack.pack(msg)


