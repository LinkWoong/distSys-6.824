package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)


type Coordinator struct {
	// Your definitions here.
	Files []string
	NReduce int
	Completed []bool
}

// Your code here -- RPC handlers for the worker to call.
// give a task to a MapWorker
func (c *Coordinator) TaskDistribution(args *MapWorkerTaskArgs, reply *MapWorkerTaskReply) error {
	if len(c.Files) == 0 { return nil }
	filename := c.Files[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	
	reply.Filename = filename
	reply.Content = string(content)
	reply.NReduce = c.NReduce
	
	c.Files = c.Files[1:] // pop the first element as it is a queue
	return nil
}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if (len(c.Files) == 0) {
		ret = true
	} else {
		fmt.Printf("Task not done yet, current task size remaining: %d\n", len(c.Files))
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// fmt.Println(len(files)) -> 8
	// fmt.Println(files[0]) -> ./data/pg-being_ernest.txt
	// Your code here.

	c.Files = files
	c.NReduce = nReduce
	c.Completed = make([]bool, nReduce)

	c.server()
	return &c
}
