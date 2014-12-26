package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
  address string
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    fmt.Println("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) DispatchWorker(jobNum int) {
  workerAddr := <- mr.registerChannel
  mr.Workers[workerAddr] = &WorkerInfo { address: workerAddr }
  args := &DoJobArgs { 
    File: mr.file, 
    Operation: mr.currentPhase, 
    JobNumber: jobNum,
  }
  if (mr.currentPhase == Map) {
    args.NumOtherPhase = mr.nReduce
  } else {
    args.NumOtherPhase = mr.nMap
  }
  var reply DoJobReply;
  ok := call(workerAddr, "Worker.DoJob", args, &reply)
  if ok == true {
    go func(mr *MapReduce, jobNum int) {
      mr.successChannel <- jobNum
    } (mr, jobNum)
    go Register(mr.MasterAddress, workerAddr)
  } else {
    go func(mr *MapReduce, jobNum int) {
      mr.requestChannel <- jobNum
    } (mr, jobNum)
  }
}

func (mr *MapReduce) RequestJobs() {
  var numJobs int;
  if mr.currentPhase == Map { 
    numJobs = mr.nMap 
  } else { 
    numJobs = mr.nReduce 
  }
  for i := 0; i < numJobs; i++ { 
    go func(i int) { mr.requestChannel <- i }(i) 
  }
}

func (mr *MapReduce) RunMaster() *list.List {
  numJobsDone := 0
  mr.currentPhase = Map
  mr.successChannel = make(chan int)
  mr.requestChannel = make(chan int)
  mr.RequestJobs()
  for {
    select {
      case reqJobId := <- mr.requestChannel:
        go mr.DispatchWorker(reqJobId)
      case successJobId := <- mr.successChannel:
        numJobsDone++
        fmt.Printf("Job %d complete", successJobId)
        if (mr.currentPhase == Map) {
          if (numJobsDone == mr.nMap) {
            numJobsDone = 0
            mr.currentPhase = Reduce
            mr.RequestJobs()  // could make a go routine?
          }
        } else {
          if (numJobsDone == mr.nReduce) {
            return mr.KillWorkers()
          }
        }
    }
  }
}
