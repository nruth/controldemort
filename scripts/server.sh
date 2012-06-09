# run from project root
(
JVM_SIZE="-server -Xms4g -Xmx4g"
JVM_SIZE_NEW="-XX:NewSize=512m -XX:MaxNewSize=512m"
JVM_GC_TYPE="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC"
JVM_GC_OPTS="-XX:CMSInitiatingOccupancyFraction=70 -XX:SurvivorRatio=2"
cd voldemort-0.90.1-nruth
bin/voldemort-server.sh config/lakka/ > voldemort.log &
)