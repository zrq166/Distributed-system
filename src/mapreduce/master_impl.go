package mapreduce




//
// any additional state that you want to add to type WorkerInfo
//
type WorkerInfoImpl struct {
	//status bool // 1 means busy, 0 means available
}

//
// run the MapReduce job across all the workers
//
func (mr *MapReduce) RunMasterImpl() {

	mr.Workers = make(map[string]*WorkerInfo)

	Mapjob := func(job_num int, worker_temp string) bool {
		args := &DoJobArgs{}
		args.File = mr.file
		args.Operation = Map
		args.JobNumber = job_num
		args.NumOtherPhase = mr.nReduce
		var reply DoJobReply

		success := call(worker_temp, "Worker.DoJob", args, &reply)
		if success {
			mr.impl.lock1.Lock()
			mr.impl.countmap++
			mr.impl.lock1.Unlock()
			mr.registerChannel <- worker_temp
			return true
		} else {
			mr.impl.jobnum1 <- job_num
			mr.registerChannel <- worker_temp
			return false
		}
	}
	Reducejob := func(job_num int, worker_temp string) bool {
		args := &DoJobArgs{}
		args.File = mr.file
		args.Operation = Reduce
		args.JobNumber = job_num
		args.NumOtherPhase = mr.nMap
		var reply DoJobReply

		success := call(worker_temp, "Worker.DoJob", args, &reply)
		if success {
			mr.impl.lock2.Lock()
			mr.impl.countreduce++
			mr.impl.lock2.Unlock()
			mr.registerChannel <- worker_temp
			return true
		} else {
			mr.impl.jobnum2 <- job_num
			mr.registerChannel <- worker_temp
			return false
		}

	}

	for i := 0; i < mr.nMap; i++ {
		mr.impl.jobnum1 <- i
	}

	for {
		mr.impl.lock1.Lock()
		//fmt.Println(mr.impl.countmap)
		if (mr.impl.countmap >= mr.nMap) {
			mr.impl.lock1.Unlock()
			break
		}
		mr.impl.lock1.Unlock()
		select {
		case x := <-mr.impl.jobnum1:
			worker_temp := <-mr.registerChannel
			go Mapjob(x, worker_temp)
		default:
			continue
		}

	}

	for i := 0; i < mr.nReduce; i++ {
		mr.impl.jobnum2 <- i
	}

	for {
		mr.impl.lock2.Lock()
		if (mr.impl.countreduce >= mr.nReduce) {
			mr.impl.lock2.Unlock()
			break
		}
		mr.impl.lock2.Unlock()
		select {
		case x := <-mr.impl.jobnum2:
			worker_temp := <-mr.registerChannel
			go Reducejob(x, worker_temp)
		default:
			continue
		}

	}

	return

}
