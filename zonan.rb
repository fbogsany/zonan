#!/usr/bin/env ruby

require 'zookeeper'
require 'zk'
require 'optparse'

# zonan.rb -d 60 -p 65 -s "server1:2181,server2:2181,server3:2181"

options = {}
option_parser = OptionParser.new do |opts|
  opts.banner = "Usage: zonan.rb [options]"
  opts.on("-s", "--server SERVERS") { |servers| options[:servers] = servers }
  opts.on("-d", "--duration SECONDS", Integer) { |duration| options[:duration] = duration }
  opts.on("-p", "--processes COUNT", Integer) { |count| options[:process_count] = count }
end

option_parser.parse!
unless [:servers, :duration, :process_count].all? { |key| options.include? key }
  puts option_parser
  exit
end
servers, duration, process_count = options.values_at(:servers, :duration, :process_count)
MAX_SHOP_ID = 200_000

def lock_cycle(zk)
  shop_id = rand(MAX_SHOP_ID)
  lock = ZK::Locker.shared_locker(zk, 'lock', "/shopify/shops/#{shop_id}")
  exit 1 unless lock.lock
  exit 1 unless lock.unlock
end

pipes = process_count.times.map { IO.pipe }
readers = pipes.map do |reader, writer|
  pid = fork do
    reader.close
    cycles = 0
    ZK.open(servers) do |zk|
      fin = Time.now + duration
      cycles += 10.times { lock_cycle(zk) } until fin < Time.now
    end
    writer.write cycles
  end
  writer.close
  reader
end

lock_count = readers.reduce(0) do |memo, reader|
  result = reader.read.to_i
  reader.close
  memo + result
end

if Process.waitall.all? { |ary| ary.last.success? }
  p "Hammered #{lock_count} in #{duration} seconds (#{lock_count.to_f / duration} op/s)"
else
  p "FAILED"
end
