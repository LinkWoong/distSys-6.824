package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "sort"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// define sorting criteria
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	workerId := 1
	for areAllTasksComplete() == false {
		doMapReduce(mapf, reducef, workerId)
		workerId += 1
	}
}

func doMapReduce(mapf func(string, string) []KeyValue, reducef func(string, []string) string, workerId int) {
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	reply := retrieveOneTaskFromCoordinator()
	filename, content, nReduce := reply.Filename, reply.Content, reply.NReduce
	
	// 1. MapWorker
	intermediateValue := mapf(filename, content)
	// fmt.Printf("Map result is %v", intermediate_value) -> {donations 1}
	fmt.Printf("Length of intermediate_value is %d\n", len(intermediateValue))
	
	// 2. Segment the intermediate result into nReduce bucket (obtained from the Coordinator)
	chunck := len(intermediateValue) / nReduce
	fmt.Printf("Filename is %v, content length is %v, bucketSize is %v\n", filename, len(content), chunck)
	
	i := 0
	batchId := 0
	for i < len(intermediateValue) {
		j := i + chunck
		if j >= len(intermediateValue) {
			j = len(intermediateValue)
		}
		// fmt.Printf("i is %v, j is %v\n", i, j)
		segment := intermediateValue[i:j]
		
		sort.Sort(ByKey(segment))
		
		// 3. persist output of ReduceWorker on disk
		persistMapWorkerOutput(workerId, segment, reducef, batchId)
		
		i = j
		batchId += 1
	}
}

func areAllTasksComplete() bool {
	args := MapWorkerTaskArgs{}
	reply := MapWorkerTaskReply{}
	
	isDone := call("Coordinator.Done", &args, &reply)
	if isDone {
		fmt.Println("All jobs are completed!!! Good work")
	} else {
		fmt.Printf("Still some jobs remain uncompleted!\n")
	}
	
	return isDone
}

func retrieveOneTaskFromCoordinator() MapWorkerTaskReply {
	args := MapWorkerTaskArgs{}
	reply := MapWorkerTaskReply{}
	
	// send the RPC request, wait for the reply.
	ok := call("Coordinator.TaskDistribution", &args, &reply)
	if ok {
		fmt.Println("MapWorker is talking to the Coordinator through gRPC")
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

func persistMapWorkerOutput(workerId int, intermediate []KeyValue, reducef func(string, []string) string, batchId int) {
	oname := fmt.Sprintf("mr-out-%v-%v", workerId, batchId)
	ofile, _ := os.Create(oname)
	
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1 // this j here means append all values that has same key into the result
		// acts as a dedup process so that values that appended will contain only unique keys
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
