#!/usr/bin/env ruby

require 'net/ssh'
require 'thread'
  
def reset_load_generators
  puts `#{File.join(File.dirname(__FILE__), *%w[reset-generator-javas.rb])}`
end

def lakka_round_robin
  @lakka ||= 1
  hostname = "lakka-#{@lakka}.it.kth.se"
  @lakka += 1
  @lakka = 1 if @lakka > 5 
  hostname
end


reset_load_generators

LOAD_GENERATOR_THROUGHPUT_UPPER_LIMIT = 3000

start_node = ENV['START_NODE'] || 1801

record_selection = ENV['GEN'] || 'uniform'

# 1 - decide target throughput 
target_throughput = ARGV[0].to_i

# 2 - decide how many load gens needed
# add a node and ignore integer division remainder
# this way the load generators should always be under-saturated
nodes = (target_throughput / LOAD_GENERATOR_THROUGHPUT_UPPER_LIMIT) + 1
per_client_target_throughput = target_throughput / nodes
puts "Using #{nodes} load generators"

# 3 - Launch the load gens over SSH
# connect to nodes and issue load gen startup command
hosts = Queue.new 
((start_node.to_i)..1812).each {|n| hosts.push("s#{n}.it.kth.se")}

total_client_ops = 1000*1000*1000

password = ENV['PASSWORD'] || raise('set PASSWORD env var for ssh login')

ssh_connection_thread = Thread.start do
  sessions = []
  halt = false
  begin
    # launch ssh connections in parallel, using a queue as a threadsafe session store
    sessions = Queue.new
    threads = (1..nodes).map do
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
      # the rest do as they are told
      puts cmd = %Q(cd /nobackup/nru/voldemort-0.90.1-nruth; JVM_SIZE="-Xms1g -Xmx1g" bin/voldemort-performance-tool.sh --url tcp://#{lakka_round_robin}:6666 --store-name trickystore2 --threads 6 --value-size 1024 --record-count 150000 -r 100 -w 0 -m 0 -d 0 --record-selection #{record_selection} --ops-count #{total_client_ops} --target-throughput #{per_client_target_throughput})
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
    sessions.each(&:close)
  end
end

# halt after after 5 minutes
kill_thread = Thread.start do
  8.downto(1) do |n| 
    print "#{n} MINUTES REMAIN\n"
    sleep(60)
  end
end
kill_thread.join

# 4 - scp measurement log from controller
system "scp emdc@lakka-6.it.kth.se:nick/measurements.log . && ssh emdc@lakka-6.it.kth.se 'rm nick/measurements.log'"

# 5 - store log in dir based on cluster size and target throughput
dir_name = "1-node-#{target_throughput}-gets"

result_path = "/Users/nruth/Dropbox/thesis-results/#{dir_name}"
 system "mkdir -p #{result_path} && mv measurements.log #{result_path}/"
Dir.chdir result_path do
  # 6 - convert log to gnuplot format
  system "~/thesis-code/scripts/convert-log-to-gnuplot.rb measurements.log"

  # 7 plot
  system "~/thesis-code/scripts/gnuplot-hist-cmd.rb | gnuplot"
  
  # 8 launch (mac os)
  system "open graph.pdf"
end

reset_load_generators

puts "check #{result_path} for output"