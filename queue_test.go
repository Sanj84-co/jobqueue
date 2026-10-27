package main

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// make the worker get stuck on the first job using release. submit two jobs.
// check jobs 2 and make sure it pending trhen close
func TestSubmitCreatesPendingJob(t *testing.T) {
	release := make(chan struct{})
	//deadlock: gorotuines wwait for racg ther or a lock that is never released everything freezes
	q := NewQueue(
		(func(u string) error { //we dont want to start a goroutine right now.
			<-release
			return nil
		})) // we will write the condtructor
	//we cant do just normally because they need to be made with make first
	_, _, err := q.Submit("send email")
	if err != nil {
		t.Fatalf("Submit returned an error: %v", err)
	}
	id2, _, err2 := q.Submit("Job 2")
	if err2 != nil {
		t.Fatalf("Submit returned an error: %v", err2)
	}
	job2, ok := q.Get(id2)
	if !ok {
		t.Fatalf("job %d not found", id2)
	}

	if job2.Status != Pending {
		t.Errorf("status = %v, want %v", job2.Status, Pending)
	}
	close(release)
}
func TestSubmitRejectEmptyPayload(t *testing.T) {
	q := NewQueue((func(u string) error { //we dont want to start a goroutine right now.
		return nil
	}))
	_, _, err := q.Submit("")
	if err == nil {
		t.Fatalf("Submit returned an error: %v", err)
	}
}
func TestWorkerCompletesJob(t *testing.T) {
	q := NewQueue(
		(func(u string) error { //we dont want to start a goroutine right now.
			return nil
		}))
	_, ch, err := q.Submit("Create email")
	if err != nil {
		t.Fatalf("Submit retuned an error %v", err)
	}
	select { //make sure to take care of when the goroutine itself is hanging
	case msg := <-ch:
		if msg.Status == Done {
			fmt.Println("Success")
		} else {
			t.Fatalf("Retuned an error %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout reached")
	}

}
func TestWorkerFailsAfterThreeAttempts(t *testing.T) {
	q := NewQueue(
		(func(u string) error {
			return errors.New("Simulated failure")
		}))
	_, ch, err := q.Submit("Success")
	if err != nil {
		t.Fatalf("Submit returned an error %v", err)
	}
	select {
	case msg := <-ch:
		if msg.Status != Failed || msg.Attempts != 3 {
			if msg.Status != Failed {
				t.Fatalf("Wrong Status ")
			} else {
				t.Fatalf("Test did not fail after 3 attempts")
			}

		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout reached")
	}

}
func TestWorkerRetriesThenSucceeds(t *testing.T) {

	counter := 0
	q := NewQueue(
		(func(u string) error {
			counter++
			if counter <= 2 {
				return errors.New("Failed")
			}
			return nil
		}))
	_, ch, err := q.Submit("Success")
	if err != nil {
		t.Fatalf("Submit returned an error %v", err)
	}
	select {
	case msg := <-ch:
		if msg.Status == Done && msg.Attempts == 3 {
			//passing test is silent
		} else {
			if msg.Status != Done {
				t.Fatalf("Wrong Status ")
			} else {
				t.Fatalf("Succeed in attemps %d", msg.Attempts)
			}
		}
	case <-time.After(time.Second * 1):
		t.Fatalf("Timeout Reached")
	}
} //handler is locked by one goroutine so test are onlly called by one routine
func TestCloseFinishesPendingJob(t *testing.T) {
	q := NewQueue((func(u string) error {
		return nil
	}))
	var results []<-chan *Job
	for i := 0; i < 5; i++ {
		_, ch, err := q.Submit("Job")
		if err != nil {
			t.Fatalf("Submit returned an error %v", err)
		}
		results = append(results, ch)
	}
	q.Close() //use defer only when close is cleanup
	for _, ch := range results {
		select {
		case msg := <-ch:
			if msg.Status == Done {
				//passing test is silent
			} else {
				if msg.Status != Done {
					t.Fatalf("Wrong Status ")
				} else {
					t.Fatalf("Succeed in attemps %d", msg.Attempts)
				}
			}
		}
	}
}
func TestSubmitAfterCloseReturnsError(t *testing.T) {
	q := NewQueue((func(u string) error {
		return nil
	}))
	q.Close()
	_, _, err := q.Submit("Job")
	if err == nil {
		t.Fatalf("Submit accepted a job after close")
	}
}
