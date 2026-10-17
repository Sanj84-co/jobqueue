package main

import (
	"errors"
	"sync"
)

type Status string
type Job struct {
	ID       int
	Payload  string
	Status   Status
	Attempts int
	Error    string
	Result   chan *Job //channel that carries *Job values
}
type Queue struct {
	jobs    map[int]*Job //same jib sits in the map and travels through the channels
	mu      sync.Mutex
	queue   chan *Job
	counter int
}

const (
	Pending Status = "pending"
	Running Status = "running"
	Done    Status = "done"
	Failed  Status = "failed"
)

func NewQueue() *Queue {
	return &Queue{
		jobs:  make(map[int]*Job),
		queue: make(chan *Job, 100), //when buffer is full, we send blocks and wait for a task to be completed.
	}
}
func (q *Queue) Submit(payload string) (int, <-chan *Job, error) {
	if payload == "" {
		return 0, nil, errors.New("payload cannot be empty")
	}
	q.mu.Lock()
	q.counter++
	job := &Job{ //make is only for maps and slices.
		ID:      q.counter,
		Payload: payload,
		Status:  Pending,
		Result:  make(chan *Job, 1),
	}
	q.jobs[job.ID] = job //m[key] = value for maps ch<-value for channels
	q.mu.Unlock()        //if you held the lock ans buffer is full send would wait and everything elsr has to wait for the lock
	q.queue <- job
	return job.ID, job.Result, nil

}
func (q *Queue) Get(id int) (*Job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock() //lock wsits till the function ends
	job, ok := q.jobs[id]
	return job, ok
}

//we need a name here. you are just getting a copy of the pointer
