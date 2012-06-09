require 'executor'
describe Executor do
  describe "update_node_lists(overloaded, underutilised) sorts the lists" do
    let(:nodes) do
    [
      {node: '0', partitions: %w(1 2 3), total_workload: 100},
      {node: '1', partitions: %w(4 5 6), total_workload: 70000},
      {node: '2', partitions: %w(7 8 9), total_workload: 400}
    ]
    end

    before(:each) do
      initial_layout = '<cluster>
        <name>Lakka</name>
        <server>
          <id>0</id>
          <host>Lakka-1.it.kth.se</host>
          <http-port>8081</http-port>
          <socket-port>6666</socket-port>
          <admin-port>6667</admin-port>
          <partitions>0,1,2,3,4,5</partitions>
        </server>
      </cluster>
      '
      @e = Executor.new(initial_layout)
    end
    
    specify "most loaded first in overloaded" do
      @e.update_node_lists(nodes, nodes)
      @e.send(:most_overloaded)[:node].should == '1'
    end
    
    specify "least utilised first in underutilised" do
      @e.update_node_lists(nodes, nodes)
      @e.send(:least_utilised)[:node].should == '0'
    end
  end
end