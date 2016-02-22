package dlock

import (
//	"errors"
//	"sync"
	"fmt"
)

type request struct {
	name       string
	owner      string
	resp_chan  chan bool
}

type lock_state struct {
	requests  []request
	owner     string
}

var (
	locks map[string]lock_state
	acquire_requests chan request
	try_acquire_requests chan request
	release_requests chan request
)

func Initialize(){
	locks = make(map[string] lock_state)
	acquire_requests = make(chan request)
	try_acquire_requests = make(chan request)
	release_requests = make(chan request)
	go manageMap()
}

//Internal function: Give the lock to the owner of req
func giveLock(req request){
	locks[req.name] = lock_state{locks[req.name].requests, req.owner}
	req.resp_chan <- true
}

//Fail to acquire a lock and push false on the channel
func failAcquireLock(req request){
	req.resp_chan <- false
}

func manageMap(){
	for {
		select {
		case req := <- acquire_requests:
			//Process acquire requests
			if _, ok := locks[req.name]; !ok {
				//Create lock entry
				locks[req.name] = lock_state{ requests: make([]request,0,100),
					owner: "" }
				//Give away lock like it's candy
				giveLock(req)
			} else {
				//Otherwise, queue the request for this lock
				locks[req.name] = lock_state{ append(locks[req.name].requests, req),
					locks[req.name].owner }
			}
		case req := <- try_acquire_requests:
			//Process acquire requests
			if _, ok := locks[req.name]; !ok {
				//Create lock entry
				locks[req.name] = lock_state{ requests: make([]request,0,100),
					owner: "" }
				//Give away lock like it's candy
				giveLock(req)
			} else {
				//Otherwise, push failure on the acquisition channel
				failAcquireLock(req)
			}
		case req := <- release_requests:
			//Process release requests
			state, exists := locks[req.name]

			if !exists || state.owner != req.owner {
				//Lock doesn't exist or requester isn't owner
				fmt.Println("ManageMap: Release Request Error ("+req.owner+","+state.owner+")" )
				req.resp_chan <- false
			} else {
				if len(state.requests) < 1 {
					//No pending acquire requests, delete the entry.
					delete(locks, req.name)
				} else {
					//Give the lock to the first request
					acquire_request := locks[req.name].requests[0]
					locks[req.name] = lock_state {locks[req.name].requests[1:],
						locks[req.name].owner }
					giveLock(acquire_request)
				}
				//Release request successful, return true.
				req.resp_chan <- true
			}
		}
	}
}

func AcquireLock(lock_name string, entity string){
	//Create a request with a new channel that we'll listen on
	req := request {lock_name, entity, make(chan bool) }

	//Queue the request
	acquire_requests <- req

	//Wait for the lock to be acquired
	_ = <- req.resp_chan

	return
}

func TryAcquireLock(lock_name string, entity string) bool{
	//Create a request with a new channel that we'll listen on
	req := request {lock_name, entity, make(chan bool) }

	//Queue the request
	try_acquire_requests <- req

	//Wait for the lock to be acquired
	b := <- req.resp_chan

	return b
}

func ReleaseLock(lock_name string, entity string) (bool){
	//Create a request with a new channel that we'll listen on
	req := request {lock_name, entity, make(chan bool) }

	//Queue the request
	release_requests <- req

	//Wait for the lock to be released
	b := <- req.resp_chan

	return b
}
