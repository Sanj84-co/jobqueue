package main

import "testing"

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
