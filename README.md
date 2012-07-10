Prototype partition-load aware elastic storage controller & 
instrumented Voldemort & YCSB from my masters thesis project. 

Feel free to contact me if you can't figure out how to get started, as the
documentation and scripts here are unlikely to be improved to a useful state.

## Revision history

has been squashed. It was mostly junk messages such as '.' or 'update' used
for sending small updates to deployed servers during experimentation, rather
than meaningful version updates. These files are original & first releases, so
the history is not important.

A separate commit has been retained which shows the changes made to Voldemort 
and its YCSB client [8b3049495950ec4a51ad069b92b59fee753ec76d](https://github.com/nruth/controldemort/commit/8b3049495950ec4a51ad069b92b59fee753ec76d)

## Deployment

 1. create or modify a cluster configuration using your servers
 2. launch the initial voldemort cluster and check it works (client demo from getting-started guide on voldemort website)
 3. launch the controller with controller/launch.rb optionally preceded by LOAD_ASSUMPTION=uniform to change rebalancing strategy
 4. start the voldemort-performance-tool (YCSB) load generators to begin experimentation.

N.B. you will need to modify and rebuild Voldemort for your load generators with your controller's hostname, by modifying the VoldemortWrapper class's hard-coded server connection. 
Patch / pull request with YAML or JSON config file welcome. 
https://github.com/nruth/controldemort/blob/master/voldemort-0.90.1-nruth/test/integration/voldemort/performance/benchmark/VoldemortWrapper.java#L260

Useful commands:

## Remote load generator launching

Scripted multiple generators

`PASSWORD='something something' START_NODE=1804 GEN=uniform scripts/remote-measure.rb 5000`
`PASSWORD='something something' START_NODE=1801 GEN=samekey scripts/remote-measure.rb 4000`
`PASSWORD='something something' START_NODE=1801 GEN=3min_rand_then_key4 scripts/remote-measure.rb 6000`

Or launch one while logged in by ssh
``
cd voldemort-0.90.1-nruth; JVM_SIZE="-Xms1g -Xmx1g" bin/voldemort-performance-tool.sh --url tcp://lakka-1.it.kth.se:6666 --store-name trickystore2 --threads 6 --value-size 1024 --record-count 150000 -r 100 -w 0 -m 0 -d 0 --record-selection 3min_rand_then_key4 --ops-count 1000000 --target-throughput 4000)
``

## kill stray generator processes

`kill $(ps ux | grep [j]ava | awk '{print $2}')`

## grab measurement.log and plot

``
scp controller-host:controller/path/measurements.log . && ssh controller-host 'rm /controller/path/measurements.log'
~/thesis-code/scripts/convert-log-to-gnuplot.rb measurements.log
~/thesis-code/scripts/gnuplot-hist-cmd.rb | gnuplot
open *.pdf
``

or with the data in pwd
``
~/thesis-code/scripts/convert-log-to-gnuplot.rb measurements.log && ~/thesis-code/scripts/gnuplot-hist-cmd.rb| gnuplot
``

## Build and deploy: naive/lazy ant and rsync

``ant -f build.xml
rsync -r -e ssh ./ hostname:'~/deploy/path'
``

## Notes

BOOTSTRAP URL:
can be any server url
https://groups.google.com/forum/?fromgroups#!topic/project-voldemort/1Kpp4j5WxgI

*set per node*

server.properties file should be created in cluster config for each node.
Do this by symlink to avoid reconfiguring and fighting with rsync / scp

 http://www.project-voldemort.com/configuration.php
