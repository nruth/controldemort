## How to position partition bins in the group
#
# Having obtained a good division of partitions into bins, how to assign to nodes
# given that we have an existing plan?
#
# Minimise a cost function, which measures the transferred data required to
# achieve the plan
#
# assume new plan and old plan, where both are accurate (migration was not aborted
# before completion)
#
# e.g. 
#   [A: 1, 2, 3], [B: 4, 5, 6] => [A: 1, 2], [B: 3, 4, 5, 6] cost 1
#   [A: 1, 2, 3], [B: 4, 5, 6] => [A: 4, 5, 6], [B: 1, 2, 3] cost 6
#
# sum for all nodes in plan:
#   sum for all partitions of node in new plan: 
#     1 if partition is unknown to node
#     0 if partition is stored by node in old plan
#
# OR better, consider size of partitions
#
# sum for all nodes in plan:
#   sum for all partitions of node in new plan: 
#     sizeof(partition) if partition is unknown to node
#     0 if partition is stored by node in old plan


class PartitionLayoutMapper
  def self.revise_layout(layout, partitions, new_nodes=[])
    nodes = layout.nodes + new_nodes
    new_layout = Layout.new

    # naive approach, ignore costs
    # TODO: optimise using first-fit, or jump to constraint solver
    partitions.each do |partition|
      new_layout.add(nodes.pop, partition)
    end
  end
end
