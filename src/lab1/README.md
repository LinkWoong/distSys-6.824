## Lab1

### Files that will be touched:
* `mrsequential.go`: The main function that perfrom MapReduce using data under `./data` directory
* `mr/rpc.go, mr/coordinator.go, mr/worker.go`: Map workers and Reduce workers and Coordinator.

### Functional Requirements:
* One process for running the Coordinator
* Multi-processes for running Workers.
* The workers need to talk to Coordinator through gRPC to retrieve `Task`
* The workers then will read the task's input from one or more files, execute the task and write the output to local disks (one or more files)

### Non-functional Requirements:
* Fault Tolerance: The Coordinator should notice if a worker hasn't completed its task in 10 seconds. Then give the same task to a different worker


### Result
```
f0rest@Aviato [00:07:49] [~/Desktop/go/distSys-6.824/src/lab1] [dev *]
-> % bash test-mr.sh 
*** Cannot find timeout command; proceeding without timeouts.
*** Starting wc test.
2022/07/29 00:08:40 rpc.Register: method "Done" has 1 input parameters; needs exactly three
2022/07/29 00:08:48 dialing:dial unix /var/tmp/824-mr-501: connect: connection refused
2022/07/29 00:08:48 dialing:dial unix /var/tmp/824-mr-501: connect: connection refused
2022/07/29 00:08:48 dialing:dial unix /var/tmp/824-mr-501: connect: connection refused
--- wc test: PASS
*** Starting indexer test.
2022/07/29 00:08:49 rpc.Register: method "Done" has 1 input parameters; needs exactly three
2022/07/29 00:08:53 dialing:dial unix /var/tmp/824-mr-501: connect: connection refused
2022/07/29 00:08:53 dialing:dial unix /var/tmp/824-mr-501: connect: connection refused
mr-indexer-all mr-correct-indexer.txt differ: char 16, line 1
--- indexer output is not the same as mr-correct-indexer.txt
--- indexer test: FAIL
*** Starting map parallelism test.
2022/07/29 00:08:54 rpc.Register: method "Done" has 1 input parameters; needs exactly three
2022/07/29 00:09:02 dialing:dial-http unix /var/tmp/824-mr-501: unexpected EOF
2022/07/29 00:09:02 dialing:dial unix /var/tmp/824-mr-501: connect: connection refused
--- map parallelism test: PASS
*** Starting reduce parallelism test.
2022/07/29 00:09:02 rpc.Register: method "Done" has 1 input parameters; needs exactly three
2022/07/29 00:09:11 dialing:dial unix /var/tmp/824-mr-501: connect: connection refused
2022/07/29 00:09:11 dialing:dial unix /var/tmp/824-mr-501: connect: connection refused
--- reduce parallelism test: PASS
*** Starting job count test.
2022/07/29 00:09:11 rpc.Register: method "Done" has 1 input parameters; needs exactly three
2022/07/29 00:09:28 dialing:dial unix /var/tmp/824-mr-501: connect: connection refused
2022/07/29 00:09:28 dialing:dial unix /var/tmp/824-mr-501: connect: connection refused
2022/07/29 00:09:28 dialing:dial unix /var/tmp/824-mr-501: connect: connection refused
2022/07/29 00:09:28 dialing:dial unix /var/tmp/824-mr-501: connect: connection refused
--- job count test: PASS
*** Starting early exit test.
2022/07/29 00:09:28 rpc.Register: method "Done" has 1 input parameters; needs exactly three
2022/07/29 00:09:37 dialing:dial unix /var/tmp/824-mr-501: connect: connection refused
2022/07/29 00:09:37 dialing:dial unix /var/tmp/824-mr-501: connect: connection refused
2022/07/29 00:09:37 dialing:dial unix /var/tmp/824-mr-501: connect: connection refused
--- early exit test: PASS
*** Starting crash test.
2022/07/29 00:09:38 rpc.Register: method "Done" has 1 input parameters; needs exactly three
2022/07/29 00:10:32 dialing:dial unix /var/tmp/824-mr-501: connect: connection refused
2022/07/29 00:10:32 dialing:dial unix /var/tmp/824-mr-501: connect: connection refused
2022/07/29 00:10:32 dialing:dial unix /var/tmp/824-mr-501: connect: connection refused
mr-crash-all mr-correct-crash.txt differ: char 14, line 1
--- crash output is not the same as mr-correct-crash.txt
--- crash test: FAIL
*** FAILED SOME TESTS
```

### Cleanup
```
rm -rf mr-tmp && rm mrcoordinator && rm mrsequential && rm mrworker && rm mr-* 
```