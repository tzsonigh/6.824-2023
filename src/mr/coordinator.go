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
    files            []string
    index            int
    tasks            []Task
    nMap             int
    nReduce          int
    isMapFinished    bool
    isRedeceFinished bool
    mu               sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskTask(args *Args, reply *Reply) error {
    c.mu.Lock()

    //delete finished task
    if reply.id != -1 {
        for i := 0; i < len(c.tasks); i++ {
            if reply.id == c.tasks[i].id {
                c.tasks = append(c.tasks[:i], c.tasks[i:]...)
                break
            }
        }
    }

    if !c.isMapFinished && c.index == c.nMap && len(c.tasks) == 0 {
        c.isMapFinished = true
        c.index = 0
    }

    if c.isRedeceFinished && c.index == c.nReduce && len(c.tasks) == 0 {
        c.isRedeceFinished = true
    }

    if c.isRedeceFinished {
        reply.TaskType = 3
        c.mu.Unlock()
        return nil
    }

    //allocate task
    taskType, nTask := 1, c.nReduce
    filename := ""
    if !c.isMapFinished {
        taskType = 0
        nTask = c.nMap
        filename = c.files[c.index]
    }

    if c.index < nTask {
        reply = &Reply{c.index, taskType, filename}
        c.tasks = append(c.tasks, Task{c.index, filename, time.Now().Add(time.Second * 10)})
        c.index++
    } else {
        var flag bool
        for i := 0; i < len(c.tasks); i++ {
            if time.Now().After(c.tasks[i].time) {
                reply = &Reply{c.tasks[i].id, taskType, c.tasks[i].filename}
                flag = true
                break
            }
        }

        if flag {
            reply.TaskType = 2
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
    ret = c.isRedeceFinished

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
    c.isMapFinished = false
    c.isRedeceFinished = false
    c.server()
    return &c
}