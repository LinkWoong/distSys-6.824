package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type MapWorkerTaskArgs struct {
	Filename string
	Content string
	NReduce int
	Index int
}

type MapWorkerTaskReply struct {
	Filename string
	Content string
	NReduce int
	TaskType string
	Index int
}

type TaskFinishArgs struct {
	Index int
	TaskType string
}

type TaskFinishReply struct {
	Status string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
