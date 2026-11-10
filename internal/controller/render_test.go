package controller

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDeadlineSeconds(t *testing.T) {
	if deadlineSeconds(nil) != nil {
		t.Fatal("nil deadline should leave the Job unbounded")
	}
	for d, want := range map[time.Duration]int64{4 * time.Hour: 14400, 38 * time.Second: 38, 90*time.Second + 500*time.Millisecond: 91} {
		if got := *deadlineSeconds(&metav1.Duration{Duration: d}); got != want {
			t.Errorf("%s: got %d want %d", d, got, want)
		}
	}
}
