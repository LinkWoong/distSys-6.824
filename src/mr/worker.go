package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

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
	for {
		task := GetTaskFromCoordinator()
		switch task.TaskType {
		case "wait":
			break
		
		case "map":
			MapHandler(task.Filename, task.Index, task.NReduce, mapf)
			FinishTaskFromCoordinator(task)
			break
		
		case "reduce":
			ReduceHandler(task.Filename, task.Index, task.NReduce, reducef)
			FinishTaskFromCoordinator(task)
			break
		}
		
		time.Sleep(100 * time.Millisecond)
	}
}

func MapHandler(filename string, mapIndex int, nReduce int, mapf func(string, string) []KeyValue) {
	
	// read all raw data
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Cannot open %v ", filename)
	}
	
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Cannot read %v ", filename)
	}
	
	file.Close()
	
	// call mapf to get intermediate value
	intermediateValue := mapf(filename, string(content))
	
	// create a hashList to map the intermediate result to nReduce bucket/partition
	hashList := make([][]KeyValue, nReduce)
	
	for _, kv := range intermediateValue {
		fileIndex := ihash(kv.Key) % nReduce
		hashList[fileIndex] = append(hashList[fileIndex], kv)
	}
	
	for reduceIndex, partition := range hashList {
		intermediateFilename := fmt.Sprintf("mr-%d-%d", mapIndex, reduceIndex)
		tempIntermediateFilename := fmt.Sprintf("%s-%d", filename, time.Now().Unix())
		tempFile, _ := os.Create(tempIntermediateFilename)
		
		encoder := json.NewEncoder(tempFile)
		for _, kv := range partition {
			encoder.Encode(kv)
		}
		
		os.Rename(tempIntermediateFilename, intermediateFilename)
	}
}

func ReduceHandler(filename string, index int, nReduce int, reducef func(string, []string) string) {
	// map worker output will be stored as mr-*-index
	
	reduceList := []string{}
	
	for _, name := range readCurrentDirectory() {
		if len(name) >= 6 && 
		name[3:6] != "out" && 
		name[:3] == "mr-" &&
		name[len(name) - 1 - len(strconv.Itoa(index)):] == fmt.Sprintf("-%d", index) {
			reduceList = append(reduceList, name)
		}
	}
	
	res := []KeyValue{}
	
	for _, filename := range reduceList {
		file, err := os.Open(filename)
		decoder := json.NewDecoder(file)
		
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			
			res = append(res, kv)
		}
		
		if err != nil {
			log.Fatalf("Cannot read %v", filename)
		}
		
		file.Close()
	}
	
	sort.Sort(ByKey(res))
	
	output := fmt.Sprintf("mr-out-%d", index)
	temp := fmt.Sprintf("%s-%d", output, time.Now().Unix())
	
	ofile, _ := os.Create(temp)
	
	// perform reducef
	// print result to mt-out-%d
	
	i := 0
	
	for i < len(res) {
		j := i + 1
		for j < len(res) && res[i].Key == res[j].Key {
			j += 1
		}
		
		values := []string{}
		
		for k := i; k < j; k++ {
			values = append(values, res[i].Value)
		}
		
		reduceOutput := reducef(res[i].Key, values)
		
		fmt.Fprintf(ofile, "%v %v\n", res[i].Key, reduceOutput)
		
		i = j
	}
	
	ofile.Close()
	os.Rename(temp, output)
}

func FinishTaskFromCoordinator(task MapWorkerTaskReply) TaskFinishReply {
	args := TaskFinishArgs{
		Index: task.Index,
		TaskType: task.TaskType,
	}
	
	reply := TaskFinishReply{}
	
	// send the RPC request, wait for the reply.
	ok := call("Coordinator.CompleteTask", &args, &reply)
	if ok {
		
	} else {
		fmt.Printf("call failed!\n")
		os.Exit(0)
	}
	
	return reply
}

func GetTaskFromCoordinator() MapWorkerTaskReply {
	args := MapWorkerTaskArgs{}
	reply := MapWorkerTaskReply{}
	
	// send the RPC request, wait for the reply.
	ok := call("Coordinator.TaskDistribution", &args, &reply)
	if ok {
	} else {
		fmt.Printf("call failed!\n")
		os.Exit(0)
	}
	
	return reply
}

func readCurrentDirectory() []string {
	file, err := os.Open(".")
	if err != nil {
		log.Fatalf("Failed to open current directory: %s", err)
	}
	
	defer file.Close()
	list, _ := file.Readdirnames(0)
	
	return list
}


// #########################

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
