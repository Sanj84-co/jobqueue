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
	counter int //only lists data fields.
	closed  bool
	done    chan struct{} //done is closed when our queue is done
}

const (
	Pending Status = "pending"
	Running Status = "running"
	Done    Status = "done"
	Failed  Status = "failed"
)

func NewQueue(handler func(string) error) *Queue {
	q := &Queue{
		jobs:  make(map[int]*Job),
		queue: make(chan *Job, 100), //when buffer is full, we send block and wait for a task to be completed.
		done:  make(chan struct{}),
	}
	go q.work(handler)
	return q

}
func (q *Queue) Close() {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.closed = true
	close(q.queue)
	q.mu.Unlock()
	<-q.done

} // closes thr queue channel so new jobs cant come in . Workers finidhes remaining jobs in and ends
// .then we close done, the moment it is closed we finish
func (q *Queue) Submit(payload string) (int, <-chan *Job, error) {
	if payload == "" {
		return 0, nil, errors.New("payload cannot be empty")
	}
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return 0, nil, errors.New("queue is closed")
	}
	q.counter++
	job := &Job{ //make is only for maps and slices and channels.
		ID:      q.counter,
		Payload: payload,
		Status:  Pending,
		Result:  make(chan *Job, 1),
	}
	q.jobs[job.ID] = job //m[key] = value for maps ch<-value for channels
	//if you held the lock ans buffer is full send would wait and everything elsr has to wait for the lock
	//a send that cant wait
	select {
	case q.queue <- job:

	default:
		delete(q.jobs, job.ID)
		q.mu.Unlock()
		return 0, nil, errors.New("queue is full!")
	}
	q.mu.Unlock()

	return job.ID, job.Result, nil

}
func (q *Queue) Get(id int) (*Job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock() //lock wsits till the function ends
	job, ok := q.jobs[id]
	return job, ok
}
func (q *Queue) work(handler func(string) error) {
	defer close(q.done)
	for job := range q.queue {
		for job.Attempts < 3 {
			q.mu.Lock()
			job.Status = Running
			job.Attempts++
			q.mu.Unlock()
			err := handler(job.Payload)
			if err == nil {
				q.mu.Lock()
				job.Status = Done
				q.mu.Unlock()
				break
			} else {
				q.mu.Lock()
				job.Error = err.Error()
				q.mu.Unlock()
			}

		}
		if job.Status != Done {
			q.mu.Lock()
			job.Status = Failed
			q.mu.Unlock()
		}
		job.Result <- job
	}
}

//Sart launches the worker loop in. goroutine. the worker takes each job from the channl marks i running
//increases its attempts unlocks it. call the handler if the handler is nil, it locks the job done returns the job to resukt channl
//we need a name here. you are just getting a copy of the pointer
