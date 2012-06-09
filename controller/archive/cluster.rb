# think-of-an-object cluster; responsible for group membership, resizing, repartitioning
class Cluster
  # size of the storage cluster
  def size
    CloudProxy.storage_nodes!.length
  end

  def repartition(partitions)
    newsize = partitions.length
    if size < newsize
      # add new nodes before deciding where to place data
      new_nodes = grow_cluster_by(newsize - oldsize)
      change_layout PartitionLayoutMapper.revise_layout(layout, partitions, new_nodes)
    elsif newsize < size
      # move data first to prevent data loss
      change_layout PartitionLayoutMapper.revise_layout(layout, partitions)
      remove_unused_nodes(layout)
    else 
      # same nodes but new data locations
      change_layout PartitionLayoutMapper.revise_layout(layout, partitions)
    end
  end

  # updates the current stored layout & tells storage to use this layout
  # blocks until completion of layout change
  def change_layout(new_layout)
    DataMover.from_to(layout, new_layout)
  end

  def remove_unused_nodes(layout)
    nodes = CloudProxy.storage_nodes!
    unused_nodes = nodes.reject {|n| layout.includes?(n)}
    CloudProxy.deallocate_nodes!(unused_nodes)
  end
end
