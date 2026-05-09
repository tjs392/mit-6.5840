package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// My Implementation

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

type GetTaskArgs struct {
}

type GetTaskReply struct {
	Filename string
	TaskId   int
	NReduce  int
	NMap     int
	TaskType TaskType
}

type ReportTaskArgs struct {
	TaskType TaskType
	TaskId   int
}

type ReportTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
