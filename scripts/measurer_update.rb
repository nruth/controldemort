#!/usr/bin/env ruby

start_node = ENV['START_NODE'] || 1801

# Launch the load gens over SSH
# connect to nodes and issue load gen startup command
require 'net/ssh'
require 'thread'
hosts = Queue.new 
((start_node.to_i)..1812).each {|n| hosts.push("s#{n}.it.kth.se")}

password = ENV['PASSWORD'] || raise('set PASSWORD env var for ssh login')

sessions = []
halt = false
begin
  # launch ssh connections in parallel, using a queue as a threadsafe session store
  sessions = Queue.new
  threads = (1..12).map do
    Thread.start do
      host = hosts.pop # pop head of array
      print "connecting to #{host}\n"
      sessions.push Net::SSH.start(host, 'nru', :password => password)
    end
  end
  # wait for all to connect
  threads.each(&:join)

  # convert queue to array for richer api; no longer threading
  sessions = [].tap {|arr| arr.push sessions.pop until sessions.empty?}

  sessions.each do |s|
    s.exec('cd /nobackup/nru; rm -rf /nobackup/nru/*; wget --no-verbose https://dl.dropbox.com/u/570150/voldemort-0.90.1-nruth.zip && unzip -q voldemort-0.90.1-nruth.zip')
  end

  # process multiple Net::SSH connections in parallel
  condition = Proc.new { |s| s.busy? }
  loop do
    sessions.delete_if { |ssh| !ssh.process(0.1, &condition) }
    break if sessions.empty?
  end
rescue Exception => e
  abort(e)
ensure
  sessions.each(&:close)
end
