package destination

import (
	"errors"
	"time"

	"github.com/atzoum/event-delivery/internal/common"
)

// DestinationHandler is responsible to deliver an event to a destination
type Handler interface {
	Handle(event *common.Event) error
	GetSuccessCounter() int
	GetFailureCounter() int
}

// Creates a mock destination handler
func MockHandler(delay time.Duration, fail bool) Handler {
	res := mockHandler{fail: fail, delay: delay}
	return &res
}

type mockHandler struct {
	fail           bool
	delay          time.Duration
	successCounter int
	failureCounter int
}

func (h *mockHandler) Handle(event *common.Event) error {
	time.Sleep(h.delay)
	if h.fail {
		h.failureCounter += 1
		return errors.New("Handler failed")
	}
	h.successCounter += 1
	return nil
}

func (h *mockHandler) GetSuccessCounter() int {
	return h.successCounter
}

func (h *mockHandler) GetFailureCounter() int {
	return h.failureCounter
}
