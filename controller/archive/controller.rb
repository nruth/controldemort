require 'cluster'

# CONTROLLER ENTRY POINT
# Everything stems from or services delegation from this class
# Any low-level content here should be extracted to its own class
class Controller
  attr_accessor :layout_optimiser
  attr_accessor :cluster

  SLA_UPPER = 4500
  SLA_LOWER = 3000

  # layout_optimiser :: optimisation strategy, produces layouts from data workloads and current layout
  def initialize(layout_optimiser)
    self.layout_optimiser = layout_optimiser
    self.cluster = Cluster.new
  end

  # decide whether and how to actuate in response to sensor data
  def control_decision(workload)  
    if workload.out_of_range?
      cluster.repartition optimise_partitions_for(workload)
    end
  end

  # returns a new list of partition bins for the cluster to adopt
  # this only indicates the shape of the data partitioning,
  # i.e. the result of bin-packing
  # no knowledge of physical nodes, assigning bins to nodes is done elsewhere
  def optimise_partitions_for(workload)
    layout_optimiser.optimise_partitions_for(cluster.current_partitions, workload)
  end
end
