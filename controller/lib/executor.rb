require 'thread'
require_relative('layout.rb')
require 'monitor'

class Executor
  def load_remote_layout
    layout = nil
    10.times do |n|
      begin
        vold_path = File.join(File.dirname(__FILE__), *%w[.. .. voldemort-0.90.1-nruth])
        cmd = "cd #{vold_path}; bin/voldemort-admin-tool.sh --get-metadata cluster.xml --url 'tcp://lakka-1.it.kth.se:6666'"
        # print "running #{cmd}\n"
        xml_report = `#{cmd}`
        xml = %r|(<cluster>.*?</cluster>)|m.match(xml_report)[1]
        raise 'invalid response' unless xml
        return Layout.from_xml(xml)
      rescue Exception => e
        print "RPC Layout retrieval error, retry attempt #{n}: #{e}"
        sleep(2)
      end
    end    
  end

  def current_layout
    @layout ||= load_remote_layout
  end

  private
  attr_accessor :underutilised
  attr_accessor :overloaded
  attr_accessor :histogram

  public
  def initialize()
    @lock = Monitor.new
    @lists_updated = @lock.new_cond
    self.histogram = Hash.new do |hash, key|
      hash[key] = {'GET' => 0, 'PUT' => 0, 'DELETE' => 0}
    end
    self.overloaded = []
    self.underutilised = []
  end

  def exec!
    Thread.start do
      loop do
        if ENV['LOAD_ASSUMPTION'] == 'uniform'
          # perform simple work-stealing
          # treat overloaded queue as a boolean flag rather indicating an extra node is needed
          print "controlling with uniform load assumed\n"  
          if not overloaded.empty?
            # clear until new data arrives
            overloaded.clear
            introduce_new_node_and_work_steal
          end
        else

          # consume overloaded nodes until all gone
          # lock inside the loop, so the list can be replaced between jobs
          most_overloaded = true
          while most_overloaded
            @lock.synchronize do
              if most_overloaded = overloaded.pop
                print "#{Time.now} Executor wants to rebalance: #{most_overloaded}\n"
                move_hottest_partition(most_overloaded, histogram)
              end
            end
          end
        end
    
        #consume_at_most_one_underutilised
        @lock.synchronize do
          if least_utilised = underutilised.pop
            print "executor wants to remove underutilised: #{least_utilised}\n"
          end
        end
        
        # wait for update if no work left to do
        @lock.synchronize do
          @lists_updated.wait_while { overloaded.empty? && underutilised.empty? }
        end
      end
    end
  end

  def update_node_lists(overloaded, underutilised, histogram)
    print "updating node list\n"
    @lock.synchronize do
      print "taken executor node list lock\n"
      self.histogram = histogram

      # sort into highest-load-first order
      self.overloaded = overloaded.sort {|a, b| a[:total_workload] <=> b[:total_workload]}        

      # sort into lowest-load-first order
      self.underutilised = underutilised.sort {|a, b| b[:total_workload] <=> a[:total_workload]}
      # print "waking up executor\n"
      @lists_updated.signal
    end
  end

  private

  #move a random partition from the hot node (assuming uniform load over partitions)  
  def move_random_partition(node, histogram)
    #pick a random partition
    partition = node[:partitions].sample 
    raise "failed to find partition" unless partition
    move_partition_to_most_utilised_node_with_space_for_it(node, partition, histogram)
  end
  
  # node: {node: string, partitions: [strings], total_workload: int}
  # histogram: {partition_string: {request_string: count}}
  def move_hottest_partition(node, histogram)
    hottest_partition = node[:partitions].max_by {|p| histogram[p]['GET']}
    raise "failed to find hottest" unless hottest_partition
    move_partition_to_most_utilised_node_with_space_for_it(node, hottest_partition, histogram)
  end

  def move_partition_to_most_utilised_node_with_space_for_it(node, partition, histogram)
    current_layout = self.current_layout

    # get the underused node with high load and enough space
    partition_workload = histogram[partition]['GET']/Planner::SAMPLE_PERIOD.to_f
    stealing_node = underutilised.detect {|n| (n[:total_workload] + partition_workload) < Planner::OVERLOAD }
    stealing_node = stealing_node ? stealing_node[:node] : current_layout.empty_node
    if stealing_node
      layout = current_layout.move_partition_from_to!(partition, node[:node], stealing_node)
      DataMover.move_to_layout(layout)
    else
      print "Executor: failed to find stealing node for #{partition} with workload #{partition_workload}\n"
    end
  end
  
  def introduce_new_node_and_work_steal
    layout = current_layout
    
    # grab an empty node, if available
    if stealer = current_layout.empty_node
      # take partitions from the other nodes, largest partition set first
      partition_count = layout.nodes.map {|n| layout.partitions_for_node(n).length}.reduce(:+)
      node_count = layout.nodes.length
      steal_target = partition_count / (node_count + 1)
      steal_target.times do
        node_with_most = layout.nodes.max_by {|n| layout.partitions_for_node(n).length} or raise("node_with_most not found")
        #select random node to steal
        partition = layout.partitions_for_node(node_with_most).sample or raise "partition not found for #{node_with_most.inspect} in #{layout.inspect}"
        layout.move_partition_from_to!(partition, node_with_most, stealer)
      end
      DataMover.move_to_layout(layout)
    else
      print "#{Time.now} Executor failed to expand the cluster, giving up\n"
    end
  end
end
