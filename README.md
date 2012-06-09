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
