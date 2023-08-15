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
	filename string
	time     time.Time
}

type Coordinator struct {
	// Your definitions here.
	files           []string
	index           int
	tasks           map[int]Task
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

	if args.TaskType == c.currentTaskType {
		delete(c.tasks, args.TaskId)
	}

	if len(c.tasks) == 0 && c.currentTaskType == MapTask && c.index == c.nMap {
		c.currentTaskType = ReduceTask
		c.index = MapTask
	}

	if len(c.tasks) == 0 && c.currentTaskType == ReduceTask && c.index == c.nReduce {
		c.currentTaskType = Finished
	}

	if c.currentTaskType == Finished {
		reply.TaskType = Finished
		c.mu.Unlock()
		return nil
	}

	filename, nTask := "", c.nReduce
	if c.currentTaskType == MapTask {
		filename, nTask = c.files[c.index%c.nMap], c.nMap
	}

	if c.index < nTask {
		*reply = Reply{c.index, c.currentTaskType, filename, c.nMap, c.nReduce}
		c.tasks[c.index] = Task{filename, time.Now()}
		c.index++
		c.mu.Unlock()
		return nil
	}

	reply.TaskType = Wait
	for id, task := range c.tasks {
		if time.Since(task.time) > 10*time.Second {
			*reply = Reply{id, c.currentTaskType, task.filename, c.nMap, c.nReduce}
			break
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
	c.tasks = map[int]Task{}
	c.nMap = len(files)
	c.nReduce = nReduce
	c.currentTaskType = 0
	c.server()
	return &c
}
