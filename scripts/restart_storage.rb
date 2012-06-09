#!/usr/bin/env ruby
# run from project root

ssh_nodes = (1..5).map {|n| "emdc@lakka-#{n}.it.kth.se"}

ssh_nodes.each do |node|
  kill_existing = '~/nick/voldemort-0.90.1-nruth/bin/voldemort-stop.sh'
  launch = 'cd ~/nick && scripts/server.sh'
  puts `ssh #{node} '#{kill_existing}; #{launch}'`
end

puts 'servers launched'