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

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Done
)

type Task struct {
	Type      TaskType
	Id        int
	Filename  string
	Status    TaskStatus
	StartTime time.Time
}

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	DonePhase
)

type Coordinator struct {
	mu          sync.Mutex
	mapTasks    []Task
	reduceTasks []Task
	nMap        int
	nReduce     int
	phase       Phase
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase == DonePhase {
		reply.TaskType = ExitTask
		return nil
	}

	var tasks []Task
	var taskType TaskType
	if c.phase == MapPhase {
		tasks = c.mapTasks
		taskType = MapTask
	} else {
		tasks = c.reduceTasks
		taskType = ReduceTask
	}

	allDone := true
	for i := range tasks {
		task := &tasks[i]
		if task.Status != Done {
			allDone = false
		}

		switch task.Status {
		case Idle:
			task.Status = InProgress
			task.StartTime = time.Now()
			reply.Filename = task.Filename
			reply.NMap = c.nMap
			reply.NReduce = c.nReduce
			reply.TaskId = task.Id
			reply.TaskType = taskType
			return nil

		case InProgress:
			if time.Since(task.StartTime) > 10*time.Second {
				task.StartTime = time.Now()
				reply.Filename = task.Filename
				reply.NMap = c.nMap
				reply.NReduce = c.nReduce
				reply.TaskId = task.Id
				reply.TaskType = taskType
				return nil
			}
			continue

		case Done:
			continue
		}
	}

	if allDone {
		if taskType == MapTask {
			c.phase = ReducePhase
		} else {
			c.phase = DonePhase
			reply.TaskType = ExitTask
			return nil
		}
	}

	reply.TaskType = WaitTask
	return nil
}

func (c *Coordinator) ReportTaskDone(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == MapTask {
		c.mapTasks[args.TaskId].Status = Done
	} else if args.TaskType == ReduceTask {
		c.reduceTasks[args.TaskId].Status = Done
	}

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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == DonePhase
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapTasks = make([]Task, len(files))
	for i, f := range files {
		c.mapTasks[i] = Task{
			Type:     MapTask,
			Id:       i,
			Filename: f,
			Status:   Idle,
		}
	}
	c.nMap = len(files)
	c.nReduce = nReduce
	c.phase = MapPhase

	c.reduceTasks = make([]Task, nReduce)
	for i := range nReduce {
		c.reduceTasks[i] = Task{
			Type:   ReduceTask,
			Id:     i,
			Status: Idle,
		}
	}

	c.server()

	return &c
}
