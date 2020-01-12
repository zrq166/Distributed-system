package mapreduce

import "sync"

//
// any additional state that you want to add to type MapReduce
//
type MapReduceImpl struct {
	jobnum1           chan int
	jobnum2           chan int
	lock1             sync.Mutex
	lock2             sync.Mutex
	countmap int
	countreduce int
}

//
// additional initialization of mr.* state beyond that in InitMapReduce
//
func (mr *MapReduce) InitMapReduceImpl(nmap int, nreduce int,
	file string, master string) {
	mr.impl.jobnum1 = make(chan int, nmap)
	mr.impl.jobnum2 = make(chan int, nreduce)
	mr.impl.lock1 = sync.Mutex{}
	mr.impl.countmap=0
	mr.impl.countreduce=0


}
