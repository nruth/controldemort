require 'planner'
require 'layout'

describe Planner do
  describe 'analyse(histogram, layout)' do
    let(:histogram) do
      {
        '0' => {'GET' => 1000}, 
        '1' => {'GET' => 300}, '4' => {'GET' => 200},
        '2' => {'GET' => 3000}, '3' => {'GET' => 2000}
      }
    end
    
    let(:layout) do
      l = Layout.new
      l.add('0', %w(2 3))
      l.add('1', %w(1 4))
      l.add('2', %w(0))
      l
    end

    describe "returns [overloaded, underutilised] nodes" do
      before(:each) do
        p = Planner.new(mock)
        @overloaded, @underutilised = *p.send(:analyse, histogram, layout)
      end

      it "load above 4500 is overloaded" do
        @overloaded.length.should == 1
        @overloaded.should include({node: '0', partitions: %w(2 3), total_workload: 5000})
      end

      it "load below 3000 is underloaded" do
        @underutilised.length.should == 2
        @underutilised.should include({node: '1', partitions: %w(1 4), total_workload: 500})
        @underutilised.should include({node: '2', partitions: %w(0), total_workload: 1000})        
      end
    end
  end
end