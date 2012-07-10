package voldemort.client;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.serialization.Serializer;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.versioning.InconsistencyResolver;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * 
 * Wraps a DefaultStoreClient instance; method calls are instrumented and
 * delegated
 * 
 * @author Nicholas Trevor Rutherford
 * 
 * @param <K> The key type
 * @param <V> The value type
 */
public class InstrumentedDefaultStoreClient<K, V> implements StoreClient<K, V> {

    // all real work is delegated to this client
    private DefaultStoreClient<K, V> client;
    private volatile Store<K, V, Object> store;
    private final Logger logger = Logger.getLogger(InstrumentedDefaultStoreClient.class);

    // holds {partition: {request: count}}
    private PartitionAccessHistogram partition_usage;

    @SuppressWarnings("unchecked")
    public InstrumentedDefaultStoreClient(String storeName,
                                          InconsistencyResolver<Versioned<V>> resolver,
                                          StoreClientFactory storeFactory,
                                          int maxMetadataRefreshAttempts) {

        // to store request counts
        this.partition_usage = new PartitionAccessHistogram();

        // delegate actual request processing to default store client
        this.client = new DefaultStoreClient<K, V>(storeName,
                                                   resolver,
                                                   storeFactory,
                                                   maxMetadataRefreshAttempts);

        // make the default client's raw store accessible, for routing info
        try {
            Field client_store_field = this.client.getClass().getDeclaredField("store");
            client_store_field.setAccessible(true);
            this.store = (Store<K, V, Object>) client_store_field.get(client);
        } catch(Exception e) {
            throw new UnsupportedOperationException(e.toString());
        }

        logger.info("booted instrumented client");
    }

    public Versioned<V> get(K key, Object transforms) {
        partition_usage.recordGet(partition_for_key(key));
        return client.get(key, transforms);
    }

    public Versioned<V> get(K key, Versioned<V> defaultValue) {
        partition_usage.recordGet(partition_for_key(key));
        return client.get(key, defaultValue);
    }

    public Versioned<V> get(K key) {
        partition_usage.recordGet(partition_for_key(key));
        return client.get(key);
    }

    public Version put(K key, V value, Object transforms) {
        partition_usage.recordPut(partition_for_key(key));
        // logger.info("put recorded for " + partition_for_key(key));
        return client.put(key, value, transforms);
    }

    public Version put(K key, V value) {
        partition_usage.recordPut(partition_for_key(key));
        // logger.info("put recorded for " + partition_for_key(key));
        return client.put(key, value);
    }

    public Version put(K key, Versioned<V> versioned) throws ObsoleteVersionException {
        partition_usage.recordPut(partition_for_key(key));
        // logger.info("put recorded for " + partition_for_key(key));
        return client.put(key, versioned);
    }

    public boolean delete(K key) {
        partition_usage.recordDelete(partition_for_key(key));
        return client.delete(key);
    }

    public boolean delete(K key, Version version) {
        partition_usage.recordDelete(partition_for_key(key));
        return client.delete(key, version);
    }

    public boolean applyUpdate(UpdateAction<K, V> action) {
        return applyUpdate(action, 3);
    }

    public boolean applyUpdate(UpdateAction<K, V> action, int maxTries) {
        // copy-pasted from DefaultStoreClient because need the actions to run
        // methods on this store, so they are counted
        // can't hard-code the counts because the action doesn't tell us which
        // partition or key it's working on (could be several)
        boolean success = false;
        try {
            for(int i = 0; i < maxTries; i++) {
                try {
                    // logger.info("applying update with instrumented client");
                    action.update(this);
                    success = true;
                    return success;
                } catch(ObsoleteVersionException e) {
                    // ignore for now
                }
            }
        } finally {
            if(!success)
                action.rollback();
        }

        // if we got here we have seen too many ObsoleteVersionExceptions
        // and have rolled back the updates
        return false;
    }

    public V getValue(K key) {
        throw new UnsupportedOperationException("Not yet implemented.");
        // return client.getValue(key);
    }

    public V getValue(K key, V defaultValue) {
        throw new UnsupportedOperationException("Not yet implemented.");
        // return client.getValue(key, defaultValue);
    }

    public Map<K, Versioned<V>> getAll(Iterable<K> keys) {
        throw new UnsupportedOperationException("Not yet implemented.");
        // return client.getAll(keys);
    }

    public Map<K, Versioned<V>> getAll(Iterable<K> keys, Map<K, Object> transforms) {
        throw new UnsupportedOperationException("Not yet implemented.");
        // return client.getAll(keys, transforms);
    }

    public boolean putIfNotObsolete(K key, Versioned<V> versioned) {
        throw new UnsupportedOperationException("Not yet implemented.");
        // return client.putIfNotObsolete(key, versioned);
    }

    public List<Node> getResponsibleNodes(K key) {
        return client.getResponsibleNodes(key);
    }

    private enum Request {
        GET,
        PUT,
        DELETE
    }

    private int partition_for_key(K key) {
        // assumes DefaultClientStore is modified so that #store is public
        // access by reflection-hack
        RoutingStrategy routing = (RoutingStrategy) store.getCapability(StoreCapabilityType.ROUTING_STRATEGY);
        @SuppressWarnings("unchecked")
        Serializer<K> keySerializer = (Serializer<K>) store.getCapability(StoreCapabilityType.KEY_SERIALIZER);

        // key -> bytes, and look-up its partitions
        // assuming 1 replica as in experiment
        return routing.getPartitionList(keySerializer.toBytes(key)).get(0);
    }

    private class PartitionAccessHistogram {

        public PartitionAccessHistogram() {
            partition_histograms = new HashMap<Integer, Map<Request, Integer>>();
        }

        public void recordGet(Integer partition) {
            incrementRequestCount(partition, Request.GET);
        }

        public void recordPut(Integer partition) {
            incrementRequestCount(partition, Request.PUT);
        }

        public void recordDelete(Integer partition) {
            incrementRequestCount(partition, Request.DELETE);
        }

        private Map<Integer, Map<Request, Integer>> partition_histograms;

        private void incrementRequestCount(Integer partition, Request request) {
            // Find or init the partition's request histogram
            Map<Request, Integer> partition_counts = partition_histograms.get(partition);
            if(partition_counts == null) {
                // new map with counters at 0
                HashMap<Request, Integer> new_req_counter = new HashMap<Request, Integer>();
                new_req_counter.put(Request.GET, 0);
                new_req_counter.put(Request.PUT, 0);
                new_req_counter.put(Request.DELETE, 0);
                // put new partition histogram into
                partition_histograms.put(partition, new_req_counter);
                partition_counts = new_req_counter;
            }

            // n = n + 1
            int next_n = partition_counts.get(request) + 1;
            if((next_n % 5000) == 0) {
                logger.info("Received " + next_n + " " + request + " requests for partition "
                            + partition);
            }
            partition_counts.put(request, next_n);
        }
    }
}
