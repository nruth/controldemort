/*
 * Copyright 2010 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.performance.benchmark;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import voldemort.client.DefaultStoreClient;
import voldemort.client.LazyStoreClient;
import voldemort.client.StoreClient;
import voldemort.client.UpdateAction;
import voldemort.routing.RoutingStrategy;
import voldemort.serialization.Serializer;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.utils.Time;
import voldemort.versioning.Versioned;

public class VoldemortWrapper {

    public enum ReturnCode {
        Ok,
        Error
    }

    private StoreClient<Object, Object> voldemortStore;
    private Metrics measurement;
    private boolean verifyReads;
    private boolean ignoreNulls;
    private static final Logger logger = Logger.getLogger(VoldemortWrapper.class);

    // Elastore extension; modified nruth for Controldemort
    static Socket clientSocket = null;
    // only one for all client threads
    static private DescriptiveStatistics statsR;
    static private DescriptiveStatistics statsM; // Mixed read/write transaction
    // //

    // singleton sockets and histogram for all client threads (synchronised
    // updates via lock)
    static private PartitionAccessHistogram partition_usage = new PartitionAccessHistogram();
    static DataInputStream in;
    static DataOutputStream out;

    public enum Operations {
        Read("reads"),
        Delete("deletes"),
        Write("writes"),
        Mixed("transactions");

        private String opString;

        public String getOpString() {
            return this.opString;
        }

        Operations(String opString) {
            this.opString = opString;
        }
    }

    public VoldemortWrapper(StoreClient<Object, Object> storeClient,
                            boolean verifyReads,
                            boolean ignoreNulls) {
        this.voldemortStore = storeClient;
        this.measurement = Metrics.getInstance();
        this.verifyReads = verifyReads;
        this.ignoreNulls = ignoreNulls;

        // Elastore extension
        this.statsR = new DescriptiveStatistics();
        this.statsM = new DescriptiveStatistics();
        // Controldemort extension
        rawStoreClient = extract_raw_store_for_routing_info((LazyStoreClient<Object, Object>) voldemortStore);
    }

    public void read(Object key, Object expectedValue, Object transforms) {
        long startNs = System.nanoTime();
        Versioned<Object> returnedValue = voldemortStore.get(key, transforms);
        long endNs = System.nanoTime();
        measurement.recordLatency(Operations.Read.getOpString(),
                                  (int) ((endNs - startNs) / Time.NS_PER_MS));

        // Elastore extension
        synchronized(statsR) {
            statsR.addValue(endNs - startNs);
        }

        ReturnCode res = ReturnCode.Ok;
        if(returnedValue == null && !this.ignoreNulls) {
            res = ReturnCode.Error;
        }

        if(verifyReads && !expectedValue.equals(returnedValue.getValue())) {
            res = ReturnCode.Error;
        }

        measurement.recordReturnCode(Operations.Read.getOpString(), res.ordinal());

        // Controldemort
        partition_usage.recordGet(partition_for_key(key));
    }

    public void mixed(final Object key, final Object newValue, final Object transforms) {

        boolean updated = voldemortStore.applyUpdate(new UpdateAction<Object, Object>() {

            @Override
            public void update(StoreClient<Object, Object> storeClient) {
                long startNs = System.nanoTime();
                Versioned<Object> vs = storeClient.get(key);
                boolean write_executed = false;
                if(vs != null) {
                    // logger.info("updating value of existing entry");
                    write_executed = true;
                    storeClient.put(key, newValue, transforms);
                } else {
                    // logger.info("key has no value, not updating");
                }
                long endNs = System.nanoTime();
                measurement.recordLatency(Operations.Mixed.getOpString(),
                                          (int) ((endNs - startNs) / Time.NS_PER_MS));
                // Elastore extension
                synchronized(statsM) {
                    statsM.addValue(endNs - startNs);
                }

                // Controldemort
                partition_usage.recordGet(partition_for_key(key));
                if(write_executed) {
                    partition_usage.recordPut(partition_for_key(key));
                }
            }
        });

        ReturnCode res = ReturnCode.Error;
        if(updated) {
            res = ReturnCode.Ok;
        }

        measurement.recordReturnCode(Operations.Mixed.getOpString(), res.ordinal());
    }

    public void write(final Object key, final Object value, final Object transforms) {
        // logger.info("starting write");
        boolean written = voldemortStore.applyUpdate(new UpdateAction<Object, Object>() {

            @Override
            public void update(StoreClient<Object, Object> storeClient) {
                // logger.info("making write");
                long startNs = System.nanoTime();
                storeClient.put(key, value, transforms);
                long endNs = System.nanoTime();
                measurement.recordLatency(Operations.Write.getOpString(),
                                          (int) ((endNs - startNs) / Time.NS_PER_MS));
                // Controldemort
                // logger.info("recording write");
                partition_usage.recordPut(partition_for_key(key));
            }
        });

        ReturnCode res = ReturnCode.Error;
        if(written) {
            res = ReturnCode.Ok;
        }

        measurement.recordReturnCode(Operations.Write.getOpString(), res.ordinal());
    }

    public void delete(Object key) {
        long startNs = System.nanoTime();
        boolean deleted = voldemortStore.delete(key);
        long endNs = System.nanoTime();

        ReturnCode res = ReturnCode.Error;
        if(deleted) {
            res = ReturnCode.Ok;
        }
        measurement.recordLatency(Operations.Delete.getOpString(),
                                  (int) ((endNs - startNs) / Time.NS_PER_MS));
        measurement.recordReturnCode(Operations.Delete.getOpString(), res.ordinal());

        // Controldemort
        partition_usage.recordDelete(partition_for_key(key));
    }

    private Store<Object, Object, Object> rawStoreClient;

    public static Store<Object, Object, Object> extract_raw_store_for_routing_info(LazyStoreClient<Object, Object> client) {
        // grab the real store client from this lazy-loader and recurse
        return extract_raw_store_for_routing_info((DefaultStoreClient<Object, Object>) client.getStoreClient());
    }

    @SuppressWarnings("unchecked")
    public static Store<Object, Object, Object> extract_raw_store_for_routing_info(DefaultStoreClient<Object, Object> client) {
        // make the default client's raw store accessible, for routing info
        try {
            Field client_store_field = DefaultStoreClient.class.getDeclaredField("store");
            client_store_field.setAccessible(true);
            return (Store<Object, Object, Object>) client_store_field.get(client);
        } catch(Exception e) {
            throw new UnsupportedOperationException(e.getStackTrace().toString());
        }
    }

    public static RoutingStrategy extract_routing_from_store(Store<Object, Object, Object> rawStoreClient) {
        // assumes DefaultClientStore is modified so that #store is public
        // access by reflection-hack
        return (RoutingStrategy) rawStoreClient.getCapability(StoreCapabilityType.ROUTING_STRATEGY);
    }

    private int partition_for_key(Object key) {
        RoutingStrategy routing = extract_routing_from_store(rawStoreClient);

        @SuppressWarnings("unchecked")
        Serializer<Object> keySerializer = (Serializer<Object>) rawStoreClient.getCapability(StoreCapabilityType.KEY_SERIALIZER);
        // key -> bytes, and look-up its partitions
        // assuming 1 replica as in experiment
        return routing.getPartitionList(keySerializer.toBytes(key)).get(0);
    }

    // Connect to controller, and spin off a thread to handle communicating
    // results to it
    // one connection per load generation node
    static public void startMeasurementListener() {
        // dump any warmup values
        partition_usage.reset();

        // open TCP connection to the controller
        try {
            // TODO: get ip port from property file
            clientSocket = new Socket("lakka-6.it.kth.se", 27960);
            in = new DataInputStream(clientSocket.getInputStream());
            out = new DataOutputStream(clientSocket.getOutputStream());
        } catch(UnknownHostException e) {
            System.err.println("Elastore server not available!");
            return;
        } catch(IOException e) {
            System.err.println("Elastore server IO error!");
            return;
        }

        // create a thread to receive/reply to controller measurement pull
        // requests
        new Thread() {

            @Override
            public void run() {
                // loop until quit
                while(!VoldemortWrapper.halt_measurement) {
                    // catch io exceptions
                    try {
                        // await pull request from controller
                        // which will be a single byte
                        if(in.available() == 0) {
                            try {
                                Thread.sleep(2000);
                            } catch(InterruptedException e) {
                                logger.error(e);
                            }
                        } else {
                            // read data from socket
                            int read = in.readUnsignedByte();
                            if(read != 42) {
                                logger.error("Unexpected ping value: " + read);
                            }

                            // sync-free since updates are threadsafe atomic +
                            // struct isn't changed otherwise
                            String measurement = dumpPartitionAccessHistogram();
                            partition_usage.reset();
                            out.writeUTF(measurement + "\n");
                            logger.info("Sent histogram: " + measurement);

                            HashMap<String, String> readStats = new HashMap<String, String>(7);

                            synchronized(statsR) {
                                readStats.put("n", ((Long) statsR.getN()).toString());
                                readStats.put("mean",
                                              ((Double) ns_to_ms(statsR.getMean())).toString());
                                readStats.put("s.d.",
                                              ((Double) ns_to_ms(statsR.getStandardDeviation())).toString());
                                readStats.put("min",
                                              ((Double) ns_to_ms(statsR.getMin())).toString());
                                readStats.put("max",
                                              ((Double) ns_to_ms(statsR.getMax())).toString());
                                readStats.put("95th%",
                                              ((Double) ns_to_ms(statsR.getPercentile(95))).toString());
                                readStats.put("99th%",
                                              ((Double) ns_to_ms(statsR.getPercentile(99))).toString());
                                statsR.clear();
                            }

                            ObjectMapper mapper = new ObjectMapper();
                            try {
                                String timing = mapper.writeValueAsString(readStats);
                                out.writeUTF(timing + "\n");
                                logger.info("Sent timing: " + timing);
                            } catch(JsonGenerationException e) {
                                logger.error(e.getStackTrace());
                            } catch(JsonMappingException e) {
                                logger.error(e.getStackTrace());
                            } catch(IOException e) {
                                logger.error(e.getStackTrace());
                            }
                        }
                    } catch(IOException e) {
                        System.out.println("Measurer disconnected!!: " + e);
                        break;
                    }
                }
                System.out.println("Halting pull listener");
            }
        }.start();
    }

    private static double ns_to_ms(double nanoseconds) {
        return nanoseconds / (1000 * 1000);
    }

    private volatile static boolean halt_measurement = false;

    public static void halt_measurement() {
        logger.info("flagging halt measurement");
        halt_measurement = true;
    }

    // partition histogram as JSON string
    static public String dumpPartitionAccessHistogram() {
        return partition_usage.toJson();
    }

    private enum Request {
        GET,
        PUT,
        DELETE
    }

    // holds {partition: {request: count}}
    private static class PartitionAccessHistogram {

        private volatile ConcurrentHashMap<Integer, ConcurrentHashMap<Request, AtomicInteger>> partition_histograms;

        public PartitionAccessHistogram() {
            reset();
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

        public String toJson() {
            ObjectMapper mapper = new ObjectMapper();
            try {
                return mapper.writeValueAsString(partition_histograms);
            } catch(JsonGenerationException e) {
                logger.error(e.getStackTrace());
            } catch(JsonMappingException e) {
                logger.error(e.getStackTrace());
            } catch(IOException e) {
                logger.error(e.getStackTrace());
            }
            return "{'error':'json generation error'}";
        }

        public void reset() {
            // we may lose some request counts due to threads updating the old
            // store, but that's not important
            // they'll figure it out eventually, and we're not doing
            // tiny-latency control so the error is acceptably small
            // the performance cost of making all this stuff synchronised
            // cripples the load generator

            // TODO: size by number of partitions? Here just assume 20 is enough
            partition_histograms = new ConcurrentHashMap<Integer, ConcurrentHashMap<Request, AtomicInteger>>(20);
        }

        private void incrementRequestCount(Integer partition, Request request) {
            // Find or init the partition's request histogram
            Map<Request, AtomicInteger> partition_counts = partition_histograms.get(partition);
            if(partition_counts == null) {
                // new map with counters at 0
                ConcurrentHashMap<Request, AtomicInteger> new_req_counter = new ConcurrentHashMap<Request, AtomicInteger>(Request.values().length);
                new_req_counter.put(Request.GET, new AtomicInteger(0));
                new_req_counter.put(Request.PUT, new AtomicInteger(0));
                new_req_counter.put(Request.DELETE, new AtomicInteger(0));
                // put new partition, unless another thread already did
                partition_counts = partition_histograms.putIfAbsent(partition, new_req_counter);
                if(partition_counts == null) {
                    // no previous value, so use our successfully inserted map
                    partition_counts = new_req_counter;
                }
            }

            int count = partition_counts.get(request).incrementAndGet();
            // if((count % 5000) == 0) {
            // logger.info("Received " + count + " " + request +
            // " requests for partition "
            // + partition);
            // }
        }
    }
}
