# run from project root
cd voldemort-0.90.1-nruth; JVM_SIZE="-Xms1g -Xmx1g" bin/voldemort-performance-tool.sh \
--url tcp://lakka-1.it.kth.se:6666 --store-name trickystore2 \
--threads 10 --value-size 1024 --record-count 150000  \
--ops-count 5000000 \
-r 100 -w 0 -m 0 -d 0 --record-selection uniform
