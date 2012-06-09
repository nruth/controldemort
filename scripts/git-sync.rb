#!/usr/bin/env ruby
# run from project root

ssh_nodes = (1..6).map {|n| "emdc@lakka-#{n}.it.kth.se"}

threads = ssh_nodes.map do |node|
  Thread.start(node) do |node|
    puts node
    puts `ssh #{node} 'cd nick; git fetch; git reset --hard origin/master'`
    puts 'synced'
  end
end

threads.each(&:join)