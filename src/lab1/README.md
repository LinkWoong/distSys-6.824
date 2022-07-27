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