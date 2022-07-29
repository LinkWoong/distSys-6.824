package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	index int
	filename string
	startTime time.Time
}


type Coordinator struct {
	// Your definitions here.
	TaskUnfinished []Task
	TaskInQueue []Task
	Stage  string
	Files []string
	NReduce int
	Completed []bool
}

// declare a ReadWriteMutex to prevent multiple reads
var mutex = sync.RWMutex{}

// Your code here -- RPC handlers for the worker to call.
// give a task to a MapWorker
func (c *Coordinator) TaskDistribution(args *MapWorkerTaskArgs, reply *MapWorkerTaskReply) error {
	mutex.Lock()
	
	if (len(c.TaskUnfinished) == 0) {
		reply.TaskType = "wait"
	} else {
		
		// get a task from the coordinator
		// [0] is due to queue structure
		currentTask := c.TaskUnfinished[0]
		currentTask.startTime = time.Now()
		c.TaskInQueue = append(c.TaskInQueue, currentTask)
		
		// pop the task from the queue
		c.TaskUnfinished = c.TaskUnfinished[1:]
		
		// update currentTask properties and send it back to worker through RPC
		reply.Filename = currentTask.filename
		reply.Index = currentTask.index
		reply.NReduce = c.NReduce
		reply.TaskType = c.Stage
	}
	
	mutex.Unlock()
	return nil
}

// Check task status
func (c *Coordinator) CompleteTask(args *TaskFinishArgs, reply *TaskFinishReply) error {
	if (c.Stage != args.TaskType) {
		reply.Status = "Job type inconsistent"
	}
	
	mutex.Lock()
	
	index := -1
	
	for idx, task := range c.TaskInQueue {
		if task.index == args.Index {
			index = idx
			break
		}
	}
	
	if index != -1 {
		c.TaskInQueue = append(c.TaskInQueue[:index], c.TaskInQueue[index + 1:]...) // get rid of this completed task
		reply.Status = fmt.Sprintf("Success.")
	} else {
		reply.Status = fmt.Sprintf("Cannot find the job with id %d", args.Index)
	}
	
	mutex.Unlock()
	
	return nil
}

// a thread dedicated for checking worker status
func (c *Coordinator) checkWorkerStatus() {
	for {
		mutex.Lock()
		
		timeoutTasks := []Task{}
		inprogressTasks := []Task{}
		
		for _, task := range c.TaskInQueue {
			if time.Since(task.startTime).Seconds() > 10.0 { // sees a timeout
				timeoutTasks = append(timeoutTasks, task)
			} else {
				inprogressTasks = append(inprogressTasks, task)
			}
		}
		
		// for timeout tasks, need to be put back into coordinator
		// for inprogress tasks, just keep them as is
		c.TaskUnfinished = append(c.TaskUnfinished, timeoutTasks...)
		c.TaskInQueue = inprogressTasks
		
		// check if the entire process is finished
		
		if len(c.TaskInQueue) == 0 && len(c.TaskUnfinished) == 0 {
			if (c.Stage == "map") { // edge case. So the last map output can be consumed by reduce worker
				c.Stage = "reduce"
				for i := 0; i < c.NReduce; i++ {
					task := Task {
						index: i,
						filename: fmt.Sprintf("mr-*-%d", i),
						startTime: time.Now(),
					}
					
					c.TaskUnfinished = append(c.TaskUnfinished, task)
				}
			} else {
				mutex.Unlock()
				return
			}
		}
		
		mutex.Unlock()
		time.Sleep(100 * time.Millisecond) // check status at a rate of 1 second
	}
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
	mutex.RLock()
	ret = len(c.TaskUnfinished) == 0 && len(c.TaskInQueue) == 0 && c.Stage == "reduce"
	mutex.RUnlock()
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
	
	c.NReduce = nReduce
	c.Stage = "map" // stars with map worker
	
	for i := 0; i < len(files); i++ {
		task := Task {
			index: i,
			filename: files[i],
			startTime: time.Now(),
		}
		
		c.TaskUnfinished = append(c.TaskUnfinished, task)
	}
	
	go c.checkWorkerStatus()

	c.server()
	return &c
}
