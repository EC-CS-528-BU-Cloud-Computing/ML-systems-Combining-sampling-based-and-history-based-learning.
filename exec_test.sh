## through this entire file I assume you have a python2.7 version of pypy installed in the above directory
## requirements / libraries
../pypy2.7-v7.3.9-linux64/bin/pypy -m ensurepip
../pypy2.7-v7.3.9-linux64/bin/pypy -mpip install -U pip wheel
../pypy2.7-v7.3.9-linux64/bin/pypy -mpip install -r requirements.txt

## generate topology file
../pypy2.7-v7.3.9-linux64/bin/pypy samples/topos/generate_topo.py 100 > topo_100
# generates a 100 node topology

## generate a trace file
../pypy2.7-v7.3.9-linux64/bin/pypy samples/traces/trace-generator.py 1000 5 5000 exp 1 500 unif 1 200 exp 10 1000 unif 12345 > 1000job.trace
# 1000 jobs, 
# job arrival times: exponential distribution (5,5000 secs)
# tasks/job uniform dist (1,500)
# mem/task: exponential dist (100MB,20GB)
# task duration: uniform dist (10,1000 sec)
# seed 12345

## RUN DSS
../pypy2.7-v7.3.9-linux64/bin/pypy dss.py -r ./1000job.trace ./topo_100 128000 32 1000 1000 1000 1 regular
../pypy2.7-v7.3.9-linux64/bin/pypy dss.py 1000job.trace topo_100 128000 1 100 100 1000 0 patience --oracle 0 --threesigma-features-file test.3SigmaFeatures --threesigma-warmup-file test.3SigmaWarmUp --all-or-none 0 --eviction-policy 0 --split-cluster 1 --thin-node-percentage 0 --sampling-node-percentage 0 --job-arrival-time-stretch-multiplier 27.42 --thin-limit 3 --sampling-percentage 3 --common-queue 1 --auto-tunable-partition 0
# 100 node cluster 128 GB ram 32 cores
# 1000-ms heartbeat interval
