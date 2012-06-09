require 'nokogiri'

# represents a cluster in terms of node addresses and data partitions
class Layout
  protected
  attr_accessor :partitions_for_nodes

  public

  def self.from_xml(xml)
    layout = new
    
    nxml = Nokogiri::XML(xml)
    nodes = nxml.css('server')
    nodes.each do |node|
      id = node.at_css('id').content
      partitions = node.at_css('partitions').content.split(',')
      partitions.map!(&:strip)
      layout.add(id, partitions)
    end
    layout
  end
  
  def initialize
    self.partitions_for_nodes = Hash.new
  end

  def add(node, partitions)
    partitions_for_nodes[node] = partitions
  end

  def nodes
    partitions_for_nodes.keys
  end

  # returns array of partition id strings
  def partitions_for_node(node)
    partitions_for_nodes[node]
  end

  def size
    partitions_for_nodes.size
  end

  def includes?(node)
    partitions_for_nodes.member?(node)
  end

  def includes_partition?(p)
    partitions_for_nodes.has_value?(p)
  end

  def empty_node
    nodes.detect do |n|
      partitions_for_nodes[n] == []
    end
  end

  def hashmap
    partitions_for_nodes.dup
  end

  def to_s
    hashmap.to_s
  end
  
  # mutate self to new layout with 1 partition moved
  # keeps partitions for a node sorted
  # returns self
  def move_partition_from_to!(partition, from, to)
    raise "could not find #{partition} in #{from}" unless partitions_for_nodes[from].delete(partition)
    partitions_for_nodes[to].push(partition)
    partitions_for_nodes[to].sort!
    print "#{Time.now} Moved #{partition} from #{from} to #{to}: #{partitions_for_nodes[from]} -> #{partitions_for_nodes[to]}\n"
    self
  end
end
