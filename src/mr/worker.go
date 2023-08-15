package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	taskId, taskType := -1, -1
	for {
		reply := CallTask(Args{taskId, taskType})
		switch reply.TaskType {
		case MapTask:
			doMapTask(mapf, reply)
		case ReduceTask:
			doReduceTask(reducef, reply)
		case Wait:
			time.Sleep(1 * time.Second)
		case Finished:
			return
		default:
			log.Fatalf("reply error")
		}
		taskId, taskType = reply.Id, reply.TaskType
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doMapTask(mapf func(string, string) []KeyValue, reply Reply) {
	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("reply: %v\ncannot open %v", reply, reply.Filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}
	file.Close()

	intermediate := []KeyValue{}
	kva := mapf(reply.Filename, string(content))
	intermediate = append(intermediate, kva...)

	buckets := make([][]KeyValue, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		buckets[i] = []KeyValue{}
	}
	for _, kva := range intermediate {
		buckets[ihash(kva.Key)%reply.NReduce] = append(buckets[ihash(kva.Key)%reply.NReduce], kva)
	}

	for i, bucket := range buckets {
		oname := fmt.Sprint("mr-", reply.Id, "-", i)
		ofile, _ := os.CreateTemp("", oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range bucket {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("cannot write %v", oname)
			}
		}
		os.Rename(ofile.Name(), oname)
	}
}

func doReduceTask(reducef func(string, []string) string, reply Reply) {
	intermediate := []KeyValue{}
	for i := 0; i < reply.NMap; i++ {
		oname := fmt.Sprint("mr-", i, "-", reply.Id)
		ofile, err := os.Open(oname)
		if err != nil {
			log.Fatalf("cannot open %v", oname)
		}

		dec := json.NewDecoder(ofile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		ofile.Close()
	}

	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	oname := fmt.Sprint("mr-out-", reply.Id)
	ofile, _ := os.CreateTemp("", oname)

	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	for i, j := 0, 0; i < len(intermediate); i = j {
		values := []string{}
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			values = append(values, intermediate[j].Value)
			j++
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
	}

	os.Rename(ofile.Name(), oname)
	ofile.Close()
}

func CallTask(args Args) (reply Reply) {
	call("Coordinator.AskTask", &args, &reply)
	return
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
