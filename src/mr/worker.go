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
	"time"
)

const (
	KMap = 0
	KReduce = 1
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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

func doMapping(resp *TaskReponse, mapf func(string, string) []KeyValue) {
	task := resp.Task
	filename := task.Filename
	file, err := os.Open(filename)
	if err != nil {
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return
	}
	file.Close()

	kva := mapf(filename, string(content))
	kv := make(map[int][]KeyValue)
	for _, keyValue := range kva {
		hash := ihash(keyValue.Key) % resp.NReducing
		kv[hash] = append(kv[hash], keyValue) 
	}

	for key, value := range kv {
		ifilename := fmt.Sprintf("mr-%d-%d", task.Id, key)
		ifile, _ := os.Create(ifilename)
		data, _ := json.Marshal(value)
		ifile.Write(data)
		ifile.Close()
	}

	CallFinishTask(task.Id, task.TaskType)
}

func doReducing(resp *TaskReponse, reducef func(string, []string) string) {
	task := resp.Task
	oname := fmt.Sprintf("mr-out-%d", task.Id)
	ofile, _ := os.Create(oname)
	intermediate := []KeyValue{}
	for i := 0; i < resp.NMapping; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, task.Id)
		data, err := ioutil.ReadFile(filename)
		if err != nil {
			continue
		} else {
			var tmp []KeyValue
			json.Unmarshal(data, &tmp)
			intermediate = append(intermediate, tmp...)
		}
	}

	sort.Sort(ByKey(intermediate))

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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()	
	CallFinishTask(task.Id, task.TaskType)	
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.

	for {
		req := TaskRequest{}
		resp := TaskReponse{}
		err := CallTask(&req, &resp)
		if err != nil {
			fmt.Printf("err calltask: %v\n", err)
			continue
		}

		if resp.Stat == 0 {
			doMapping(&resp, mapf)
		} else if resp.Stat == 1 {
			doReducing(&resp, reducef)
		} else if resp.Stat == 2 {
			break
		} else if resp.Stat == 3 {
			time.Sleep(500)
			continue
		}

		time.Sleep(time.Millisecond * 100)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func CallFinishTask(TaskId int, TaskType int) error {
	req := FinishRequest{
		TaskId: TaskId,
		TaskType: TaskType,
	}
	resp := FinishResponse{}
	err := call("Coordinator.FinishTask", &req, &resp)
	return err
}

func CallTask(req *TaskRequest, resp *TaskReponse) error {

	// declare an argument structure.

	// fill in the argument(s).
	// declare a reply structure.

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	err := call("Coordinator.GetTask", req, resp)
	return err
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
		os.Exit(-1)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err
}
