package main

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestSubmitCreatesPendingJob(t *testing.T) {
	q := NewQueue() // we will write the condtructor
	//we cant do just normally because they need to be made with make first
	id, _, err := q.Submit("send email")
	if err != nil {
		t.Fatalf("Submit returned an error: %v", err)
	}
	job, ok := q.Get(id)
	if !ok {
		t.Fatalf("job %d not found", id)
	}

	if job.Status != Pending {
		t.Errorf("status = %v, want %v", job.Status, Pending)
	}
}
func TestSubmitRejectEmptyPayload(t *testing.T) {
	q := NewQueue()
	_, _, err := q.Submit("")
	if err == nil {
		t.Fatalf("Submit returned an error: %v", err)
	}
}
func TestWorkerCompletesJob(t *testing.T) {
	q := NewQueue()
	q.Start(func(u string) error { //we dont want to start a goroutine right now.
		return nil
	})
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
	q := NewQueue()
	q.Start(func(u string) error {
		return errors.New("Simulated failure")
	})
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
