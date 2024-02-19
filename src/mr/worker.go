package mr

import "encoding/json"
import "fmt"
import "io/ioutil"
import "os"
import "path/filepath"
import "sort"
import "strconv"
import "time"
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type WorkType int

const (
	Map WorkType = iota
	Reduce
)

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func mapWork(mapf func(string, string) []KeyValue, number int, filename string, nReduce int) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate := mapf(filename, string(content))

	tmpfiles := []*os.File{}
	encs := []*json.Encoder{}
	for i := 0; i < nReduce; i++ {
		tmpfile, err := ioutil.TempFile("", "mr-tmp-")
		if err != nil {
			log.Fatalf("cannot create tempfile")
		}
		tmpfiles = append(tmpfiles, tmpfile)
		enc := json.NewEncoder(tmpfile)
		encs = append(encs, enc)
	}

	for _, kv := range intermediate {
		err := encs[ihash(kv.Key)%nReduce].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode %v", kv)
		}
	}

	for i := 0; i < nReduce; i++ {
		ifilename := "mr-" + strconv.Itoa(number) + "-" + strconv.Itoa(i)
		err := os.Rename(tmpfiles[i].Name(), ifilename)
		if err != nil {
			log.Fatalf("cannot rename %v", tmpfiles[i].Name())
		}
		tmpfiles[i].Close()
	}
}

func reduceWork(reducef func(string, []string) string, number int) {
	matches := "mr-*-" + strconv.Itoa(number)
	ifilenames, _ := filepath.Glob(matches)
	if len(ifilenames) == 0 {
		// fmt.Printf("No matched ifile for reduce task %d\n", number)
		return
	}

	intermediate := []KeyValue{}
	for _, ifilename := range ifilenames {
		ifile, err := os.Open(ifilename)
		if err != nil {
			log.Fatalf("cannot open %v", ifilename)
		}
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		ifile.Close()
	}

	sort.Sort(ByKey(intermediate))

	tmpfile, _ := ioutil.TempFile("", "mr-tmp-")
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

		fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofilename := "mr-out-" + strconv.Itoa(number)
	err := os.Rename(tmpfile.Name(), ofilename)
	if err != nil {
		log.Fatalf("cannot rename %v", tmpfile.Name())
	}
	tmpfile.Close()
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		reply := schedule()
		switch reply.Type {
		case Wait:
			time.Sleep(time.Second)
		case Run:
			if reply.WorkType == Map {
				mapWork(mapf, reply.Number, reply.Filename, reply.NReduce)
			} else {
				reduceWork(reducef, reply.Number)
			}
			report(reply.WorkType, reply.Number)
		case Exit:
			return
		default:
			return
		}
	}
}

func schedule() RequestReply {
	args := RequestArgs{Type: Schedule}
	reply := RequestReply{}

	ok := call("Coordinator.ScheduleHandler", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}

	return reply
}

func report(workType WorkType, number int) RequestReply {
	args := RequestArgs{Type: Report, Number: number, WorkType: workType}
	reply := RequestReply{}

	ok := call("Coordinator.ReportHandler", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}

	return reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
