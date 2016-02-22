package dlock

import (
	"testing"
	"time"
	"strconv"
	_ "strings"
	"sync"
	_	"fmt"
)

var (
	clusterStrings []string
)

func TestMain(m *testing.M){

	go RunServer("localhost", "8422")
	go RunServer("localhost", "8423")
	go RunServer("localhost", "8424")
	go RunServer("localhost", "8425")

	clusterStrings = []string{"localhost:8422",
		"localhost:8423",
		"localhost:8424",
		"localhost:8425"}

	//Slight delay so that server gets up and running
	time.Sleep(250 * time.Millisecond)

	m.Run()
}

// Have n threads try to write their index to result
// Each thread tries to acquire the lock, and waits a second once it has it.
// Ensure that at t = n, result = n
func TestLocking(t *testing.T){
	conn, err := Connect("localhost", "8422")
	if err != nil {
		t.Error("Connection error "+err.Error())
	}

	result := 0
	n := 10
	delay := 100 * time.Millisecond

	for i := 0; i < n; i++ {
		_i := i //capture
		go func(){
			entity := "thread_"+strconv.Itoa(_i)
			s := ClientAcquireLock(conn, "lock_test_locking", entity)
			if s != "lock_acquired" {
				t.Error("Acquire lock failed: "+s)
			}

			result = _i
			time.Sleep(delay)

			_, s = ClientReleaseLock(conn, "lock_test_locking", entity)
			if s != "lock_released" {
				t.Error("Release lock failed: "+s)
			}
		}()
	}

	//Give the locks one third interval to get acquired
	time.Sleep(delay / 3)

	for i := 0; i < n; i++ {
		//At t = n, result should equal n
		if result != i {
			str := strconv.Itoa(i)
			t.Error("t = "+str+", result should be "+str+", is "+strconv.Itoa(result))
		}
		time.Sleep(delay)
	}
}

//Have 1000 threads request the lock "lock1" in order of their indices
// When they complete, they push to finish_order so that we can ensure
// they executed in the correct order.
func TestLockOrdering(t *testing.T){
	conn1, err := Connect("localhost", "8422")
	if err != nil {
		t.Error("Connection error "+err.Error())
	}
	conn2, err := Connect("localhost", "8422")
	if err != nil {
		t.Error("Connection error")
	}


	limit := 200
	finish_order := make(chan int, limit)
	for i := 0; i < limit; i++ {
		_i := i // We want the current i, not the final state of i (limit)

		//Sleep between requests to ensure they're received in the correct order
		time.Sleep(2 * time.Millisecond)

		go func(){
			conn := conn1
			if _i % 2 == 0 {
				conn = conn2
			}
			entity := "entity_"+strconv.Itoa(_i)
			s := ClientAcquireLock(conn, "lock_test_order", entity)
			if s != "lock_acquired" {
				t.Error("Acquire lock failed: "+s)
			}
			finish_order <- _i
			_, s = ClientReleaseLock(conn, "lock_test_order", entity)
			if s != "lock_released" {
				t.Error("Release lock failed: "+s)
			}
		}()
	}

	for i := 0; i < limit; i++ {
		result := <- finish_order
		if result != i {
			t.Error("Out of order. Got "+strconv.Itoa(result)+", Expected "+strconv.Itoa(i))
		}
	}
}


//Ensure TryAcquire works
func TestTryAcquire(t *testing.T){
	conn, err := Connect("localhost", "8422")
	if err != nil {
		t.Error("Connection error "+err.Error())
	}

	//Acquiring a nonexistent lock should work
	s := ClientTryAcquireLock(conn, "lock_test_acquire", "")
	if s != "lock_acquired" {
		t.Error("Couldn't acquire lock: "+s)
	}

	//Attempting to acquire again shouldn't
	s = ClientTryAcquireLock(conn, "lock_test_acquire", "")
	if s != "owner=_" {
		t.Error("Incorrectly acquired lock: "+s)
	}

	//After releasing, it should be available again
	ClientReleaseLock(conn, "lock_test_acquire", "")
	s = ClientTryAcquireLock(conn, "lock_test_acquire", "")
	if s != "lock_acquired" {
		t.Error("Couldn't acquire lock: "+s)
	}
}

func TestCluster(t *testing.T){
	c, err := ClusterInitialize(clusterStrings)
	if err != nil {
		t.Fatal("Cluster initialization error: "+err.Error())
	}

	// Ensure that we fail to acquire a lock if it's already acquired
	statuses := c.AcquireLock("lock0", "entity")
	success, statuses := c.TryAcquireLock("lock0", "entity")
	if true == success {
		t.Fatal("Incorrectly acquired lock")
	}
	t.Log(statuses)

	// Ensure that other entities can't release the lock
	success, statuses = c.ReleaseLock("lock0", "entity2")
	if true == success {
		t.Fatal("Wrong entity capable of releasing lock")
	}

	// Test releasing and reacquiring the lock
	success, statuses = c.ReleaseLock("lock0", "entity")
	if false == success {
		t.Fatal("Failed to release lock")
	}

	success, statuses = c.TryAcquireLock("lock0", "entity")
	if false == success {
		t.Fatal("Failed to reacquire lock")
	}

	//Test two clients accessing the same lock
	c2, err := ClusterInitialize(clusterStrings)
	if err != nil {
		t.Fatal("Cluster initialization error: "+err.Error())
	}
	success2, _ := c2.TryAcquireLock("lock0", "entity")
	if true == success2 {
		t.Fatal("Incorrectly acquired lock")
	}

	c.ClusterShutdown()
	c2.ClusterShutdown()
}

func TestClusterCompetition(t *testing.T){
	clusters := make([]Cluster, 0)

	for i := 0; i < 10; i++ {
		c, err := ClusterInitialize(clusterStrings)
		if err != nil {
			t.Fatal("Cluster initialization error: "+err.Error())
		}
		clusters = append(clusters, c)
	}


	var wg sync.WaitGroup

	successes := make([]bool, 0)
	statuses := make([][]string, 0)

	wg.Add(len(clusters))
	for i, cluster := range clusters {
		t.Log(strconv.Itoa(i))
		_cluster := cluster //Capture
		_i := i //Capture
		go func(){
			time.Sleep(10 * time.Millisecond)
			success, status := _cluster.TryAcquireLock("lock_competition", "cordial_entity"+strconv.Itoa(_i))
			successes = append(successes, success)
			statuses = append(statuses, status)
			wg.Done()
		}()
	}
	wg.Wait()

	t.Log(statuses)
	n_acquired := 0
	for _, success := range successes {
		if success == true {
			n_acquired += 1
		}
	}
	if n_acquired != 1 {
		t.Fatal("Too many locks acquired")
	}
}
