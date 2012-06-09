require 'layout'

describe "Layout" do
  describe "populating" do
    it "starts empty" do
      Layout.new.size.should == 0
    end

    it  "accumulates partitions for nodes as a set" do
      l = Layout.new
      l.add('a', '1,2,3')
      l.size.should == 1
      l.add('b', 'foo')
      l.size.should == 2
      l.add('a', '4,5')
      l.size.should == 2
    end

    it "overwrites same key" do
      l = Layout.new
      l.add('a', '1,2,3')
      l.add('a', '4,5')
      l.partitions_for_node('a').should == '4,5'
    end
  end

  describe "#partitions_for_node" do
    it "returns a node's partitions string" do
      l = Layout.new
      l.add('a', '1,2,3')
      l.partitions_for_node('a').should == '1,2,3'
    end
  end

  specify "from_xml" do
    l = Layout.from_xml('
    <cluster>
      <name>Lakka</name>

      <server>
        <id>0</id>
        <host>Lakka-1.it.kth.se</host>
        <http-port>8081</http-port>
        <socket-port>6666</socket-port>
        <admin-port>6667</admin-port>
        <partitions>6,7,8,9,10,11</partitions>
      </server>
      
      <server>
        <id>1</id>
        <host>Lakka-2.it.kth.se</host>
        <http-port>8081</http-port>
        <socket-port>6666</socket-port>
        <admin-port>6667</admin-port>
        <partitions>0,1,2,3,4,5</partitions>
      </server>
    </cluster>
    ')
    
    l.nodes.should == ['0', '1']
    l.partitions_for_node('0').should == %w(6 7 8 9 10 11)
    l.partitions_for_node('1').should == %w(0 1 2 3 4 5)
  end

  # describe "#nodes_not_in(a, b)" do
  #   it "returns nodes of layout a not present in layout b" do
  #     pending "is anything using this?"
  #     a = Layout.new
  #     a.add('x', 'asdf')
  #     a.add('y', '234')
  #     a.add('z', 'z')      
  #     b = Layout.new
  #     b.add('x', '1,2,3')
  #     a.nodes_not_in(b).should == %w(y z)
  #   end
  # end
end
