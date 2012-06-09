#!/usr/bin/env ruby
# run from project root
ssh_nodes = (1..6).map {|n| "emdc@lakka-#{n}.it.kth.se"}

threads = ssh_nodes.map do |node|
  Thread.start(node) do |node|
    puts node
    puts `rsync -qr -e ssh ./voldemort-0.90.1-nruth ./scripts ./cloud ./controller #{node}:'~/nick'`
    puts 'sent'
  end
end

threads.each(&:join)