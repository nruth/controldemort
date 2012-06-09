#!/usr/bin/env ruby

# see diagram control-pipeline

# measurements -> planner -> queue -> layout executor

# planner = deliberation layer
# is fed new workload histograms by the measurement receiver
# and identifies overloaded and underloaded nodes
# which it passes to the executor
#
# executor consumes imbalanced nodes, feeding them to the voldemort rebalancer
# collector handles voldemort measurement clients

initial_cluster_xml = File.read(
  File.join(File.dirname(__FILE__), 
  *%w[.. voldemort-0.90.1-nruth config lakka config cluster.xml])
)

#ensure we're at this layout already
require_relative 'lib/data_mover.rb'
puts "Resetting storage to standard cluster.xml"
DataMover.move_to_xml(initial_cluster_xml)

require_relative 'lib/executor.rb'
require_relative 'lib/planner.rb'
require_relative 'lib/collector.rb'

executor = Executor.new()
planner = Planner.new(executor)
collector = Collector.new(planner)

# start measuring last, as this will begin control
threads = []
threads.push executor.exec!
threads.push planner.exec!
threads.push collector.exec!

# puts planner.identify_imbalanced_nodes({
#   '0' => {'GET' => 100}, 
#   '1' => {'GET' => 100}, '4' => {'GET' => 100},
#   '2' => {'GET' => 100}, '3' => {'GET' => 100},
#   '5' => {'GET' => 100}, '6' => {'GET' => 100},
#   '7' => {'GET' => 10000}, '8' => {'GET' => 100},
#   '9' => {'GET' => 100}, '10' => {'GET' => 100},
#   '11' => {'GET' => 100}
# })


# block until all threads terminate
threads.each(&:join)
