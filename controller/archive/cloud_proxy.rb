# responsible for cloud node provisioning
require 'rest_client'

class CloudProxy
  def self.allocate_node!
    begin 
      RestClient.post 'localhost:6775/nodes/allocate'
    rescue RestClient::InsufficientStorage
      raise 'cluster nodes exceded'
    end    
  end
  
  # returns array of hostnames of added nodes
  def self.grow_cluster_by!(n)
    (1..times).map {allocate_node}
  end

  # terminate the instances at these addresses
  def self.deallocate_nodes!(node_hostnames)
    node_hostnames.map{|host| deallocate_node(host)}
  end

  # terminate instance at this address
  def self.deallocate_node!(hostname)
    RestClient.post("localhost:6775/nodes/release/#{hostname}")
  end
    
  # returns an array of storage node addresses
  def self.storage_nodes!
    nodes = RestClient.get("localhost:6775/nodes").split(' ')
    nodes.map{|n| n.strip!} # remove leading/trailing spaces
    nodes
  end
end