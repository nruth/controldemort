package voldemort.performance.benchmark.generator;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.routing.RoutingStrategy;

// despite seemingly using the client routing, this doesn't work, instead
// distributing requests across the cluster as before
public class SamePartitionGenerator extends IntegerGenerator {

    // this must be set before construction, so that a list of keys for a
    // partition can be generated
    public static void setRoutingResolver(RoutingStrategy resolver) {
        SamePartitionGenerator.resolver = (ConsistentRoutingStrategy) resolver;
    }

    public static volatile ConsistentRoutingStrategy resolver;
    private final List<Integer> keys;
    private final Random rand = new Random();

    public SamePartitionGenerator(int partition) {
        this(partition, 200);
    }

    // keyProvider produces random (valid) byte array keys for us to match to a
    // partition
    // resolver can match byte keys to integer partitions
    public SamePartitionGenerator(int partition, int n_keys) {

        // predetermine a set of keys in the same partition
        Set<Integer> keySet = new HashSet<Integer>(n_keys);
        this.keys = new ArrayList<Integer>(n_keys);
        int nextKey = 1;
        while(keySet.size() < n_keys) {
            boolean alreadySeen = keySet.contains(nextKey);
            boolean inPartition = resolver.getPartitionList(intToBytes(nextKey)).get(0) == partition;
            if(inPartition && !alreadySeen) {
                keys.add(nextKey);
                keySet.add(nextKey);
            }
            nextKey += 1;
        }

        System.out.println("GENERATED KEYS: " + keys.toString());
    }

    @Override
    public int nextInt() {
        // randomly hit the keys to distribute load (avoid same-key contention)
        // could use round-robin / iterate over them instead
        int idx = rand.nextInt(keys.size());
        return keys.get(idx);
    }

    // copied from Workload.ByteArrayKeyProvider#next()
    private static byte[] intToBytes(int i) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(i);
        return bos.toByteArray();
    }
}
