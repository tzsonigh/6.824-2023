package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	id       int
	filename string
	time     time.Time
}

type Coordinator struct {
	// Your definitions here.
	files           []string
	index           int
	tasks           []Task
	nMap            int
	nReduce         int
	currentTaskType int
	mu              sync.Mutex
}

const (
	MapTask int = iota
	ReduceTask
	Wait
	Finished
)

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskTask(args *Args, reply *Reply) error {
	c.mu.Lock()

	//delete finished task
	if args.TaskType == c.currentTaskType {
		for i := 0; i < len(c.tasks); i++ {
			if args.TaskId == c.tasks[i].id {
				c.tasks = append(c.tasks[:i], c.tasks[i+1:]...)
				break
			}
		}
	}

	if c.currentTaskType == MapTask && c.index == c.nMap && len(c.tasks) == 0 {
		c.currentTaskType = ReduceTask
		c.index = MapTask
	}

	if c.currentTaskType == ReduceTask && c.index == c.nReduce && len(c.tasks) == 0 {
		c.currentTaskType = Finished
	}

	if c.currentTaskType == Finished {
		reply.TaskType = Finished
		c.mu.Unlock()
		return nil
	}

	//allocate task
	taskType, nTask := ReduceTask, c.nReduce
	filename := ""
	if c.currentTaskType == MapTask {
		taskType = MapTask
		nTask = c.nMap
		if c.index < c.nMap {
			filename = c.files[c.index]
		}
	}

	if c.index < nTask {
		*reply = Reply{c.index, taskType, filename, c.nMap, c.nReduce}
		c.tasks = append(c.tasks, Task{c.index, filename, time.Now()})
		c.index++
	} else {
		var flag bool
		for i := MapTask; i < len(c.tasks); i++ {
			if time.Since(c.tasks[i].time) > 10*time.Second {
				*reply = Reply{c.tasks[i].id, taskType, c.tasks[i].filename, c.nMap, c.nReduce}
				flag = true
				break
			}
		}

		if !flag {
			reply.TaskType = Wait
		}
	}

	c.mu.Unlock()
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	ret = c.currentTaskType == 3

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.index = 0
	c.tasks = make([]Task, 0)
	c.nMap = len(files)
	c.nReduce = nReduce
	c.currentTaskType = 0
	c.server()
	return &c
}
