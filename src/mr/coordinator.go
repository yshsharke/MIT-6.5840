package mr

import "log"
import "sync"
import "time"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	nMap    int
	nReduce int
	tasks   []Task

	mutex sync.Mutex
}

type Task struct {
	number   int
	workType WorkType
	filename string
	status   TaskStatus
	start    time.Time
}

type TaskStatus int

const (
	Idle TaskStatus = iota
	Inprogress
	Completed
)

//
// RPC handlers for the worker to call.
//
func (c *Coordinator) ScheduleHandler(args *RequestArgs, reply *RequestReply) error {
	isMapCompleted := true
	reply.Type = Exit

	c.mutex.Lock()

	for i, task := range c.tasks {
		// when a process has exited, the output should be finalized
		if task.status != Completed {
			reply.Type = Wait
		}

		// reduces can't start until the last map has finished
		if i < c.nMap && task.status != Completed {
			isMapCompleted = false
		}
		if i >= c.nMap && !isMapCompleted {
			reply.Type = Wait
			break
		}

		// assign a task
		if task.status == Idle || (task.status == Inprogress && time.Now().Sub(task.start) >= 10*time.Second) {
			reply.Type = Run
			reply.WorkType = task.workType
			reply.Number = task.number
			reply.Filename = task.filename
			reply.NReduce = c.nReduce
			c.tasks[i].status = Inprogress
			c.tasks[i].start = time.Now()
			c.mutex.Unlock()
			return nil
		}
	}

	c.mutex.Unlock()
	return nil
}

func (c *Coordinator) ReportHandler(args *RequestArgs, reply *RequestReply) error {
	c.mutex.Lock()

	if args.WorkType == Map {
		c.tasks[args.Number].status = Completed
	} else {
		c.tasks[args.Number+c.nMap].status = Completed
	}

	c.mutex.Unlock()
	return nil
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
	ret := true

	for _, task := range c.tasks {
		if task.status != Completed {
			ret = false
		}
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	nMap := len(files)
	tasks := []Task{}
	for i, filename := range files {
		tasks = append(tasks, Task{number: i, workType: Map, filename: filename, status: Idle})
	}
	for i := 0; i < nReduce; i++ {
		tasks = append(tasks, Task{number: i, workType: Reduce, status: Idle})
	}

	c := Coordinator{nMap: nMap, nReduce: nReduce, tasks: tasks}

	c.server()
	return &c
}
