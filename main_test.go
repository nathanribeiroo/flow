package flow_test

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain garante que nenhum teste vaza goroutine: o pacote é dono das
// goroutines que cria e precisa esperar todas.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
