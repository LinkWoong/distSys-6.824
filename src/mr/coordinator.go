package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"io/ioutil"
)


type Coordinator struct {
	// Your definitions here.
	Files []string
	NReduce int
	Completed []bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) TaskDistribution(args *MapWorkerTaskArgs, reply *MapWorkerTaskReply) error {
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
	args.Filename = filename
	args.Content = string(content)
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
