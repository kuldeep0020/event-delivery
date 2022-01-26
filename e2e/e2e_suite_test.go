package e2e

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestEventDelivery(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "EventDelivery Suite")
}
