class Planner
  SAMPLE_PERIOD = 15
  OVERLOAD = 5000
  UNDERLOAD = 4000

  attr_accessor :executor
  def initialize(executor)
    self.executor = executor
    self.histograms = Queue.new
  end

  def exec!
    Thread.start do
      loop do
        print "Planner: looking for queued histogram\n"
        identify_imbalanced_nodes(histograms.deq)
      end
    end
  end
  
  attr_accessor :histograms
  def next_histogram(histogram)
    print "replacing next histogram\n"
    histograms.clear
    histograms.enq(histogram)
  end

  def identify_imbalanced_nodes(histogram)
    print "investigating dequeued histogram\n"
    overloaded, underutilised = analyse(histogram, executor.current_layout)
    executor.update_node_lists(overloaded, underutilised, histogram)
  end
  
  private
  
  # returns [overloaded, underutilised] nodes
  def analyse(histogram, layout)
    print "analysing #{histogram} with #{layout}\n"
    overloaded = []
    underutilised = []
    
    layout.nodes.each do |node|
      # print "analysing node #{node}\n"
      node_parts = layout.partitions_for_node(node)
      # print "partitions for #{node}: #{node_parts}\n"
      node_workload = node_parts.reduce(0) do |accum, partition|
        # only consider get requests
        # print "reducing #{accum} #{partition}"
        accum + histogram[partition]['GET']
      end
 
      node_workload /= SAMPLE_PERIOD
      # print "node #{node} workload: #{node_workload}\n"
      if (node_workload > OVERLOAD) and (node_parts.length > 1)
        overloaded.push({node: node, partitions: node_parts, total_workload: node_workload})
        print "node #{node} overloaded: #{node_parts}, #{node_workload}\n"
      elsif node_workload < UNDERLOAD
        underutilised.push({node: node, partitions: node_parts, total_workload: node_workload})
        print "node #{node} underloaded: #{node_parts}, #{node_workload}\n"
      else
        print "node #{node} in safe zone or alone: #{node_parts}, #{node_workload}\n"
      end
    end
    [overloaded, underutilised]
  end
end