## How to achieve new layout
#
# Dumb version: one large uninterruptable transaction

# semi-dumb version: one large abortable transaction; 
# but rollback semantics matter (can we make new changes, or it blocks until recovered?)

# Smart version, as per SCADS Director: decompose into many small transactions
# This is better, as traffic spikes might occur and the controller needs to scrap
# the old plan and react in a new fashion. 

# Dumb smart version: first-fit serialize 1-step plan for LayoutA -> LayoutB steps
# Further work: concurrent transfers 1-per-node.

class DataMover

  # blocks until completion
  # expects instance of Layout, which it will convert to xml and send to Voldemort
  def self.move_to_layout(layout)
    new_cluster_xml = generate_cluster_xml(layout.hashmap)
    blocking_voldemort_rebalancer('tcp://lakka-1.it.kth.se:6666', new_cluster_xml)
  end

  # blocks until completion
  # expects next cluster.xml in a string
  def self.move_to_xml(cluster_xml_string)
    blocking_voldemort_rebalancer('tcp://lakka-1.it.kth.se:6666', cluster_xml_string)
  end

  # bootstrap node url is any server node's url, preferably server id 0
  # https://github.com/voldemort/voldemort/wiki/Voldemort-Rebalancing
  def self.blocking_voldemort_rebalancer(cluster_node_url, new_cluster_xml)
    # ./bin/voldemort-rebalance.sh
    # --url <url> --target-cluster <path>
    # --current-cluster <path> --current-stores <path> --target-cluster <path>
    print "running rebalance\n"
    require 'tempfile'
    tmp_cluster_xml = Tempfile.new('new_cluster.xml')
    tmp_cluster_xml.write(new_cluster_xml)
    tmp_cluster_xml.close
    vold_path = File.join(File.dirname(__FILE__), *%w[.. .. voldemort-0.90.1-nruth])
    cmd = "cd #{vold_path}; bin/voldemort-rebalance.sh --url #{cluster_node_url} --target-cluster #{tmp_cluster_xml.path} > rebalance.log"
    print "running #{cmd}\n"
    system cmd
    print "rebalancing finished\n"
  end
  
  # {'0' => ['1', '2', '3'], '1' => ['4', '5']}
  # TODO: shouldn't be hard-coded to live lakka nodes
  def self.generate_cluster_xml(node_partitions_map)
    builder = Nokogiri::XML::Builder.new do |xml|
    xml.cluster {
      xml.name "Tricky"
      node_partitions_map.each_pair do |node, partitions|
        xml.server {
          xml.id node.to_i
          xml.host "Lakka-#{node.to_i+1}.it.kth.se"
          xml.send 'http-port', '8081'
          xml.send 'socket-port', '6666'
          xml.send 'admin-port', '6667'
          xml.partitions partitions.join(',')
        }
      end
    }
    end
    xml = builder.to_xml
    # print xml + "\n"
    xml
  end
end
