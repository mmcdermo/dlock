package dlock

import (
//	"errors"
//	"sync"
	"fmt"
)

type request struct {
	name       string
	owner      string
	resp_chan  chan string
}

type lock_state struct {
	requests  []request
	owner     string
}

type dlock_state struct {
	locks map[string]lock_state
	acquire_requests chan request
	try_acquire_requests chan request
	release_requests chan request
}

func Initialize() dlock_state{
	state := dlock_state{
		locks: make(map[string] lock_state),
		acquire_requests: make(chan request),
		try_acquire_requests: make(chan request),
		release_requests: make(chan request),
	}
	go manageMap(state)
	return state
}

//Internal function: Give the lock to the owner of req
func giveLock(state dlock_state, req request){
	state.locks[req.name] = lock_state{state.locks[req.name].requests, req.owner}
	req.resp_chan <- "success"
}

//Fail to acquire a lock and push false on the channel
func failAcquireLock(req request, status string){
	req.resp_chan <- status
}

func manageMap(state dlock_state){
	for {
		select {
		case req := <- state.acquire_requests:
			//Process acquire requests
			if _, ok := state.locks[req.name]; !ok {
				//Create lock entry
				state.locks[req.name] = lock_state{ requests: make([]request,0,100),
					owner: "" }
				//Give away lock like it's candy
				giveLock(state, req)
			} else {
				//Otherwise, queue the request for this lock
				state.locks[req.name] = lock_state{ append(state.locks[req.name].requests, req),
					state.locks[req.name].owner }
			}
		case req := <- state.try_acquire_requests:
			//Process acquire requests
			if lock, ok := state.locks[req.name]; !ok {
				//Create lock entry
				state.locks[req.name] = lock_state{ requests: make([]request,0,100),
					owner: "" }
				//Give away lock like it's candy
				giveLock(state, req)
			} else {
				//Otherwise, push failure on the acquisition channel
				failAcquireLock(req, "owner="+lock.owner)
			}
		case req := <- state.release_requests:
			//Process release requests
			lockstate, exists := state.locks[req.name]

			if !exists || lockstate.owner != req.owner {
				//Lock doesn't exist or requester isn't owner
				fmt.Println("ManageMap: Release Request Error ("+req.owner+","+lockstate.owner+")" )
				req.resp_chan <- "release_error"
			} else {
				if len(lockstate.requests) < 1 {
					//No pending acquire requests, delete the entry.
					delete(state.locks, req.name)
				} else {
					//Give the lock to the first request
					acquire_request := state.locks[req.name].requests[0]
					state.locks[req.name] = lock_state{state.locks[req.name].requests[1:],
						state.locks[req.name].owner }
					giveLock(state, acquire_request)
				}
				//Release request successful, return true.
				req.resp_chan <- "success"
			}
		}
	}
}

func AcquireLock(state dlock_state, lock_name string, entity string) {
	//Create a request with a new channel that we'll listen on
	req := request{lock_name, entity, make(chan string) }

	//Queue the request
	state.acquire_requests <- req

	//Wait for the lock to be acquired
	_ = <- req.resp_chan

	return
}

func TryAcquireLock(state dlock_state, lock_name string, entity string) string{
	//Create a request with a new channel that we'll listen on
	req := request{lock_name, entity, make(chan string) }

	//Queue the request
	state.try_acquire_requests <- req

	//Wait for the lock to be acquired
	s := <- req.resp_chan

	return s
}

func ReleaseLock(state dlock_state, lock_name string, entity string) string{
	//Create a request with a new channel that we'll listen on
	req := request{lock_name, entity, make(chan string) }

	//Queue the request
	state.release_requests <- req

	//Wait for the lock to be released
	s := <- req.resp_chan

	return s
}
