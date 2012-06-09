#!/usr/bin/env ruby

require 'net/ssh'
require 'thread'

hosts = Queue.new 
(1801..1812).each {|n| hosts.push("s#{n}.it.kth.se")}
password = ENV['PASSWORD'] || raise('set PASSWORD env var for ssh login')
cmd = "kill $(ps ux | grep [j]ava | awk '{print $2}')"

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
    s.exec(cmd)
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
  # compatible with sessions as queue or array 
  while s = sessions.pop
    s.close
  end
end