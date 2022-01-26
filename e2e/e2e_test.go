package e2e

import (
	"encoding/json"
	"os"
	"time"

	"github.com/atzoum/event-delivery/internal/common"
	"github.com/atzoum/event-delivery/internal/destination"
	"github.com/atzoum/event-delivery/internal/receiver"
	"github.com/go-resty/resty/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func sendEvent(event common.Event) {
	body, _ := json.Marshal(event)
	resp, err := resty.New().NewRequest().
		SetBody(body).
		Post("http://localhost:8080")
	Expect(resp.StatusCode()).To(Equal(200))
	Expect(err).To(BeNil())
}

var _ = Describe("A receiver and two destinations are setup", Label("e2e"), func() {
	var topic string
	var r receiver.HttpServer
	var c1 destination.Controller
	var c2 destination.Controller

	BeforeEach(func() {
		os.Setenv("RETRY_COUNT", "1")
		topic = common.RandomString(10)
		fwd, err := receiver.KafkaForwarder(topic)
		Expect(err).To(BeNil())
		r = receiver.HttpServer{Fwd: fwd}
		c1 = destination.KafkaController(topic, "destination1", destination.MockHandler(1*time.Millisecond, false))
		c2 = destination.KafkaController(topic, "destination2", destination.MockHandler(1*time.Millisecond, false))
		r.Start()
		err = c1.Start()
		Expect(err).To(BeNil())
		err = c2.Start()
		Expect(err).To(BeNil())
		DeferCleanup(func() {
			c1.Stop()
			c2.Stop()
			r.Stop()
		})
	})

	When("both destination handlers operate normally", func() {

		Context("and an event has been sent through HTTP", func() {
			BeforeEach(func() {
				sendEvent(common.Event{UserID: "one", Payload: "payload"})
			})

			It("both destination handlers should eventually handle the event successfully", func() {
				Eventually(func() int { return c1.GetHandler().GetSuccessCounter() }, "10s").Should(Equal(1))
				Eventually(func() int { return c2.GetHandler().GetSuccessCounter() }, "10s").Should(Equal(1))
			})
		})
	})

	When("destination1 handler is failing", func() {
		BeforeEach(func() {
			c1.SetHandler(destination.MockHandler(1*time.Millisecond, true))
		})

		Context("and an event has been sent through HTTP", func() {
			BeforeEach(func() {
				sendEvent(common.Event{UserID: "two", Payload: "payload"})
			})
			It("destination2 handler should eventually handle the event successfully and destination1 handler should not", func() {
				Eventually(func() int { return c2.GetHandler().GetSuccessCounter() }, "10s").Should(Equal(1))
				Eventually(func() int { return c1.GetHandler().GetSuccessCounter() }, "10s").Should(Equal(0))
				Eventually(func() int { return c1.GetHandler().GetFailureCounter() }, "10s").Should(BeNumerically(">", 1))
			})
		})
	})

	When("destination1 handler is experiencing delays", func() {
		BeforeEach(func() {
			c1.SetHandler(destination.MockHandler(10*time.Second, false))
		})

		Context("and two events have been sent through HTTP", func() {
			BeforeEach(func() {
				sendEvent(common.Event{UserID: "one", Payload: "payload"})
				sendEvent(common.Event{UserID: "two", Payload: "payload"})
			})
			It("destination1 handler is left behind, whereas destination2 handler proceeds normally", func() {
				Eventually(func() int { return c2.GetHandler().GetSuccessCounter() }, "10s").Should(Equal(2))
				Expect(c1.GetHandler().GetFailureCounter()).To(Equal(0))
				Expect(c1.GetHandler().GetSuccessCounter()).To(BeNumerically("<", 2))
			})
		})
	})

	When("more than one destination1 handlers exist", func() {
		var c1b destination.Controller
		BeforeEach(func() {
			c1b = destination.KafkaController(topic, "destination1", destination.MockHandler(1*time.Millisecond, false))
			err := c1b.Start()
			Expect(err).To(BeNil())
			DeferCleanup(func() {
				c1b.Stop()
			})
		})

		Context("and ten events have been sent through HTTP", func() {
			BeforeEach(func() {
				sendEvent(common.Event{UserID: "one", Payload: "payload"})
				sendEvent(common.Event{UserID: "two", Payload: "payload"})
				sendEvent(common.Event{UserID: "three", Payload: "payload"})
				sendEvent(common.Event{UserID: "four", Payload: "payload"})
				sendEvent(common.Event{UserID: "five", Payload: "payload"})
				sendEvent(common.Event{UserID: "six", Payload: "payload"})
				sendEvent(common.Event{UserID: "seven", Payload: "payload"})
				sendEvent(common.Event{UserID: "eight", Payload: "payload"})
				sendEvent(common.Event{UserID: "none", Payload: "payload"})
				sendEvent(common.Event{UserID: "ten", Payload: "payload"})
			})
			It("both destination1 handlers handle events", func() {
				// by the time destination2 is done
				Eventually(func() int { return c2.GetHandler().GetSuccessCounter() }, "10s").Should(Equal(10))

				// destination1 is also done
				Eventually(func() int { return c1.GetHandler().GetSuccessCounter() + c1b.GetHandler().GetSuccessCounter() }, "10s").Should(Equal(10))

				// and both handlers have processed at least one message
				Expect(c1.GetHandler().GetSuccessCounter()).To(BeNumerically(">", 0))
				Expect(c1b.GetHandler().GetSuccessCounter()).To(BeNumerically(">", 0))
			})
		})
	})

	When("ten events have been sent through HTTP", func() {
		BeforeEach(func() {
			sendEvent(common.Event{UserID: "one", Payload: "payload"})
			sendEvent(common.Event{UserID: "two", Payload: "payload"})
			sendEvent(common.Event{UserID: "three", Payload: "payload"})
			sendEvent(common.Event{UserID: "four", Payload: "payload"})
			sendEvent(common.Event{UserID: "five", Payload: "payload"})
			sendEvent(common.Event{UserID: "six", Payload: "payload"})
			sendEvent(common.Event{UserID: "seven", Payload: "payload"})
			sendEvent(common.Event{UserID: "eight", Payload: "payload"})
			sendEvent(common.Event{UserID: "none", Payload: "payload"})
			sendEvent(common.Event{UserID: "ten", Payload: "payload"})
		})

		Context("and all current destination handlers are done with processing", func() {
			BeforeEach(func() {
				Eventually(func() int { return c1.GetHandler().GetSuccessCounter() }, "10s").Should(Equal(10))
				Eventually(func() int { return c2.GetHandler().GetSuccessCounter() }, "10s").Should(Equal(10))
			})

			Context("and a new destination3 handler is added", func() {
				var c3 destination.Controller
				BeforeEach(func() {
					c3 = destination.KafkaController(topic, "destination3", destination.MockHandler(1*time.Millisecond, false))
					err := c3.Start()
					Expect(err).To(BeNil())
					DeferCleanup(func() {
						c3.Stop()
					})
				})

				It("the new destination3 handler processes all previous events", func() {
					// by the time destination2 is done
					Eventually(func() int { return c3.GetHandler().GetSuccessCounter() }, "10s").Should(Equal(10))
				})
			})

		})
	})
})
