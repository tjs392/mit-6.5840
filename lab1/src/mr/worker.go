package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// uncomment to send the Example RPC to the coordinator.
	for {
		reply, ok := CallGetTask()
		if !ok {
			return
		}

		switch reply.TaskType {
		case MapTask:
			content, err := os.ReadFile(reply.Filename)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Filename)
			}
			kvs := mapf(reply.Filename, string(content))
			buckets := make([][]KeyValue, reply.NReduce)
			for _, kv := range kvs {
				y := ihash(kv.Key) % reply.NReduce
				buckets[y] = append(buckets[y], kv)
			}

			for y := 0; y < reply.NReduce; y++ {
				filename := fmt.Sprintf("mr-%d-%d", reply.TaskId, y)
				file, _ := os.Create(filename)
				enc := json.NewEncoder(file)

				for _, kv := range buckets[y] {
					enc.Encode(&kv)
				}

				file.Close()
			}
			CallReportTaskDone(MapTask, reply.TaskId)

		case ReduceTask:
			intermediate := []KeyValue{}
			for x := range reply.NMap {
				filename := fmt.Sprintf("mr-%d-%d", x, reply.TaskId)
				file, _ := os.Open(filename)
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}

			sort.Sort(ByKey(intermediate))
			oname := fmt.Sprintf("mr-out-%d", reply.TaskId)
			ofile, _ := os.Create(oname)

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}

				output := reducef(intermediate[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}

			ofile.Close()

			CallReportTaskDone(ReduceTask, reply.TaskId)

		case ExitTask:
			return
		case WaitTask:
			time.Sleep(time.Second)
		}
	}
}

func CallReportTaskDone(taskType TaskType, taskId int) {
	args := ReportTaskArgs{TaskType: taskType, TaskId: taskId}
	reply := ReportTaskReply{}
	call("Coordinator.ReportTaskDone", &args, &reply)
}

func CallGetTask() (GetTaskReply, bool) {
	args := GetTaskArgs{}

	reply := GetTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	return reply, ok
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
