package main

import (
	"testing"
	"github.com/mmcdermo/dlock/server"
	"github.com/mmcdermo/dlock/client"
	"time"
	"strconv"
//	"fmt"
)

func TestMain(m *testing.M){
	go dlock_server.RunServer("localhost", "8422")

	//Slight delay so that server gets up and running
	time.Sleep(250 * time.Millisecond)

	m.Run()
}

// Have n threads try to write their index to result
// Each thread tries to acquire the lock, and waits a second once it has it.
// Ensure that at t = n, result = n
func TestLocking(t *testing.T){
	conn, err := dlock_client.Connect("localhost", "8422")
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
			b := dlock_client.AcquireLock(conn, "lock_test_locking", entity)
			if b != true {
				t.Error("Acquire lock failed")
			}

			result = _i
			time.Sleep(delay)

			b = dlock_client.ReleaseLock(conn, "lock_test_locking", entity)
			if b != true {
				t.Error("Release lock failed")
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
	conn1, err := dlock_client.Connect("localhost", "8422")
	if err != nil {
		t.Error("Connection error "+err.Error())
	}
	conn2, err := dlock_client.Connect("localhost", "8422")
	if err != nil {
		t.Error("Connection error")
	}


	limit := 1000
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
			b := dlock_client.AcquireLock(conn, "lock_test_order", entity)
			if b != true {
				t.Error("Acquire lock failed")
			}
			finish_order <- _i
			b = dlock_client.ReleaseLock(conn, "lock_test_order", entity)
			if b != true {
				t.Error("Release lock failed")
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
