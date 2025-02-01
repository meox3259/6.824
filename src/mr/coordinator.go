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
	Filename string
	Id int
	TaskType int
	Stat int
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	MapTask []Task
	ReduceTask []Task
	NMap int
	NReduce int
	MappingDone bool
	ReducingDone bool
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) GetTask(req *TaskRequest, resp *TaskReponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	resp.NMapping = c.NMap
	resp.NReducing = c.NReduce
	done := true
	if !c.MappingDone {
		for id, task := range c.MapTask {
			if task.Stat == 0 {
				c.MapTask[id].Stat = 1
				c.MapTask[id].StartTime = time.Now()
				resp.Task = task 
				resp.Stat = 0
				return nil
			}
			if task.Stat != 2 {
				done = false
			}
		}
		if !done {
			resp.Stat = 3
		} else {
			resp.Stat = 2
		}
		return nil
	} else if !c.ReducingDone {
		for id, task := range c.ReduceTask {
			if task.Stat == 0 {
				c.ReduceTask[id].Stat = 1
				c.ReduceTask[id].StartTime = time.Now()
				resp.Task = task
				resp.Stat = 1
				return nil
			}
			if task.Stat != 2 {
				done = false
			}
		}
		if !done {
			resp.Stat = 3
		} else {
			resp.Stat = 2
		}
		return nil
	} else {
		resp.Stat = 2
		return nil
	}
	return fmt.Errorf("error: not find free task of type %d", req.TaskType)
}

func (c *Coordinator) FinishTask(req *FinishRequest, resp *FinishResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if req.TaskType == 0 {
		c.MapTask[req.TaskId].Stat = 2
		return nil
	} else if req.TaskType == 1 {
		c.ReduceTask[req.TaskId].Stat = 2
		return nil
	}
	return fmt.Errorf("invalid task type")
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

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.MappingDone && c.ReducingDone
}

func (c *Coordinator) initMapping(files []string) {
	c.mu.Lock()
	for id, file := range files {
		c.MapTask = append(c.MapTask, Task{
			Filename: file,
			Id: id,
			TaskType: 0,
			Stat: 0,
			StartTime: time.Now(),
		})
	}
	c.mu.Unlock()
	go c.refreshMapping()
}

func (c *Coordinator) initReducing(nReduce int) {
	for i := 0; i < nReduce; i++ {
		c.ReduceTask = append(c.ReduceTask, Task{
			Filename: "",
			Id: i,
			TaskType: 1,
			Stat: 0,
			StartTime: time.Now(),
		})
	}
	go c.refreshReducing()
}

func (c *Coordinator) refreshMapping() {
	for {
		c.mu.Lock()
		done := true
		for id, task := range c.MapTask {
			if task.Stat == 1 && time.Since(task.StartTime) / time.Second > 10 {
				c.MapTask[id].Stat = 0
			}
			if task.Stat != 2 {
				done = false
			}
		}
		if done {
			c.MappingDone = true
			time.Sleep(time.Millisecond * 300)
			c.initReducing(c.NReduce)
			c.mu.Unlock()
			break
		}
		c.mu.Unlock()
		time.Sleep(500)
	}
}

func (c *Coordinator) refreshReducing() {
	for {
		c.mu.Lock()
		done := true
		for id, task := range c.ReduceTask {
			if task.Stat == 1 && time.Since(task.StartTime) / time.Second > 10 {
				c.ReduceTask[id].Stat = 0
			}
			if task.Stat != 2 {
				done = false
			}
		}
		if done {
			c.ReducingDone = true			
			time.Sleep(time.Millisecond * 300)
			c.mu.Unlock()
			break
		}
		c.mu.Unlock()
		time.Sleep(500)
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MappingDone: false,
		ReducingDone: false,
		NMap: len(files),
		NReduce: nReduce,
	}
	c.server()

	// Your code here.

	c.initMapping(files)

	return &c
}
