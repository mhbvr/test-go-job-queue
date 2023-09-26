package goring

import (
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	//"strings"
	"sync"
	"testing"
	"time"
)

// Helper functions

func source(n int) chan *Message {
	output := make(chan *Message, n)

	for i := 0; i < n; i++ {
		msg := Message{i, "AAAAUUUUAAUUUAAAUUAAA"}
		output <- &msg
	}
	close(output)

	return output
}

func sincChecker(input chan *Message, expected int, t *testing.T) {
	id := 0
	for msg := range input {
		if msg.id != id {
			t.Fatalf("Expected msg id %v, received %v", id, msg.id)
		}
		id++
	}
	if id != expected {
		t.Fatalf("Expected %v messages, received %v", expected, id)
	}
}

type actionChecker struct {
	mu        sync.Mutex
	processed map[int]struct{}
	t         *testing.T
}

func (a *actionChecker) action(msg *Message) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, ok := a.processed[msg.id]; ok {
		a.t.Fatalf("Message id %v already processed", msg.id)
	}
	a.processed[msg.id] = struct{}{}
}

func (a *actionChecker) Total() int {
	return len(a.processed)
}

func newActionChecker(t *testing.T) *actionChecker {
	return &actionChecker{
		processed: make(map[int]struct{}),
		t:         t,
	}
}

type Processor func(chan *Message, func(*Message), int) chan *Message

//Tests

func processorTest(t *testing.T, proc Processor) {
	name := runtime.FuncForPC(reflect.ValueOf(proc).Pointer()).Name()
	testFunc := func(t *testing.T) {
		numMessages := 1000000
		numWorkers := 16
		input := source(numMessages)
		checker := newActionChecker(t)
		output := proc(input, checker.action, numWorkers)
		sincChecker(output, numMessages, t)
		if checker.Total() != numMessages {
			t.Fatalf("Expected %v messages, processed %v", numMessages, checker.Total())
		}
	}
	t.Run(name, testFunc)
}

func TestCorrectness(t *testing.T) {
	processorTest(t, simpleProcessor)
	processorTest(t, locklessProcessor)
	processorTest(t, queueTwoProcessor)
	processorTest(t, chainProcessor)
}

// Benchmarks

func sink(input chan *Message) {
	var count int
	for msg := range input {
		count += msg.id
	}
}

func do(msg *Message) {
	time.Sleep(time.Duration(100+rand.Intn(50)) * time.Microsecond)
	//time.Sleep(time.Microsecond)
}

func processorBenchmark(b *testing.B, proc Processor, workers int) {
	procName := runtime.FuncForPC(reflect.ValueOf(proc).Pointer()).Name()
	benchFunc := func(b *testing.B) {
		input := source(b.N)
		b.ResetTimer()
		output := proc(input, do, workers)
		sink(output)
	}
	b.Run(fmt.Sprintf("%s_%v_workers", procName, workers), benchFunc)
}

func BenchmarkProcessors(b *testing.B) {
	processorBenchmark(b, simpleProcessor, 1)

	for _, proc := range []Processor{locklessProcessor, queueTwoProcessor, chainProcessor} {
		for _, i := range []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024} {
			processorBenchmark(b, proc, i)
		}
	}
}
