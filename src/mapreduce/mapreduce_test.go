package mapreduce

import (
	"testing"
	"time"
)
import "fmt"
import "container/list"
import "os"
import "os/exec"
import "bufio"
import "log"
import "strconv"

const (
	nMap    = 100
	nReduce = 50
)

// Checks whether tail of sorted output file matches reference output
func check(t *testing.T, file string) {
	cmd := exec.Command("sh", "-c", "sort -k2n -k1 ../main/mr-testout.txt > ../main/mr-testout.sorted")
	err := cmd.Run()
	if err != nil {
		log.Fatal("check: ", err)
	}

	cmd = exec.Command("sh", "-c", "sort -k2n -k1 " + OutputDir + "mrtmp.kjv12.txt | tail -1005 | diff - ../main/mr-testout.sorted > diff.out")
	err = cmd.Run()
	if err != nil {
		log.Fatal("check: ", err)
	}

	input, err := os.Open("diff.out")
	if err != nil {
		log.Fatal("check: ", err)
	}
	defer input.Close()

	inputScanner := bufio.NewScanner(input)
	for inputScanner.Scan() {
		t.Fatalf("Output differs from reference output\n")
	}
}

// Workers report back how many RPCs they have processed in the Shutdown reply.
// Check that they processed at least 1 RPC.
func checkWorker(t *testing.T, l *list.List) {
	for e := l.Front(); e != nil; e = e.Next() {
		if e.Value == 0 {
			t.Fatalf("Some worker didn't do any work\n")
		}
	}
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp. can't use current directory since
// AFS doesn't support UNIX-domain sockets.
func port(suffix string) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "mr"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += suffix
	return s
}

func setup() *MapReduce {
	file := "kjv12.txt"
	master := port("master")
	mr := MakeMapReduce(nMap, nReduce, file, master)
	return mr
}

func cleanup(mr *MapReduce) {
	mr.CleanupFiles()
}

func TestBasic(t *testing.T) {
	fmt.Printf("Test: Basic mapreduce ...\n")
	mr := setup()
	for i := 0; i < 2; i++ {
		go RunWorker(mr.MasterAddress, port("worker"+strconv.Itoa(i)),
			WCMap, WCReduce, -1, 0)
	}
	// Wait until MR is done
	<-mr.DoneChannel
	check(t, mr.file)
	checkWorker(t, mr.stats)
	cleanup(mr)
	fmt.Printf("  ... Basic Passed\n")
}

func TestOneFailure(t *testing.T) {
	fmt.Printf("Test: One Failure mapreduce ...\n")
	mr := setup()
	// Start 2 workers, one of which fails after 10 jobs
	go RunWorker(mr.MasterAddress, port("worker"+strconv.Itoa(0)),
		WCMap, WCReduce, 10, 0)
	go RunWorker(mr.MasterAddress, port("worker"+strconv.Itoa(1)),
		WCMap, WCReduce, -1, 0)
	// Wait until MR is done
	<-mr.DoneChannel
	check(t, mr.file)
	checkWorker(t, mr.stats)
	cleanup(mr)
	fmt.Printf("  ... One Failure Passed\n")
}

func RunUnreliable(t *testing.T, fprob float64) {
	mr := setup()
	// Start 2 workers, with unreliable network connectivity
	go RunWorker(mr.MasterAddress, port("worker"+strconv.Itoa(0)),
		WCMap, WCReduce, -1, fprob)
	go RunWorker(mr.MasterAddress, port("worker"+strconv.Itoa(1)),
		WCMap, WCReduce, -1, fprob)
	// Wait until MR is done
	<-mr.DoneChannel
	check(t, mr.file)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

func TestUnreliableLow(t *testing.T) {
	fmt.Printf("Test: Intermittently Unreliable Network mapreduce ...\n")
	RunUnreliable(t, 0.1)
	fmt.Printf("  ... Intermittently Unreliable Network Passed\n")
}

func TestUnreliableHigh(t *testing.T) {
	fmt.Printf("Test: Highly Unreliable Network mapreduce ...\n")
	RunUnreliable(t, 0.9)
	fmt.Printf("  ... Highly Unreliable Network Passed\n")
}

func TestManyFailures(t *testing.T) {
	fmt.Printf("Test: Many Failures mapreduce ...\n")
	mr := setup()
	i := 0
	done := false
	for !done {
		select {
		case done = <-mr.DoneChannel:
			check(t, mr.file)
			cleanup(mr)
			break
		default:
			// Start 2 workers each sec. The workers fail after 10 jobs
			w := port("worker" + strconv.Itoa(i))
			go RunWorker(mr.MasterAddress, w, WCMap, WCReduce, 10, 0)
			i++
			w = port("worker" + strconv.Itoa(i))
			go RunWorker(mr.MasterAddress, w, WCMap, WCReduce, 10, 0)
			i++
			time.Sleep(1 * time.Second)
		}
	}

	fmt.Printf("  ... Many Failures Passed\n")
}


