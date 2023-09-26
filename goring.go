package goring

import (
	"sync"
)

// Implementations of job execution engine.
//
// Messages should be received and send in order.
// Every message should be processed, but
// order of processing is not important.
//
// The main difficulty is to preserve an order of
// messages after processing

type Message struct {
	id   int
	data string
}

// Sequential execution without parallelization
func simpleProcessor(input chan *Message, action func(*Message), _ int) chan *Message {
	output := make(chan *Message)

	go func() {
		defer close(output)
		for msg := range input {
			action(msg)
			output <- msg
		}
	}()

	return output
}

// Messages are stored in the ring buffer. One goroutine use select to
// send jobs to workers and get results back. Several workers run in parallel.
type Slot struct {
	msg       *Message
	processed bool
}

func locklessProcessor(input chan *Message, action func(*Message), workers int) chan *Message {
	output := make(chan *Message)

	size := 2 * workers

	workersOut := make(chan *Slot, size)

	ring := make([]Slot, size)
	var head, tail int
	length := 0

	source := input

	go func() {
		defer func() {
			close(output)
		}()

		for length > 0 || input != nil {
			select {
			case msg, ok := <-source:
				if !ok {
					source = nil
					input = nil
					continue
				}

				ring[tail].msg = msg
				ring[tail].processed = false
				go doJob(&ring[tail], workersOut, action)
				tail = (tail + 1) % size
				length++
			case slot := <-workersOut:
				slot.processed = true
				for length > 0 && ring[head].processed {
					output <- ring[head].msg
					head = (head + 1) % size
					length--
				}
			}

			if length >= workers {
				source = nil
			} else {
				source = input
			}
		}
	}()

	return output
}

func doJob(slot *Slot, output chan *Slot, action func(*Message)) {
	action(slot.msg)
	output <- slot
}

// One goroutine send messages to workers, another collects results.
// Order of messages preserved by fixed size ring buffer protected by lock/

type Task struct {
	index int
	msg   *Message
}

func queueTwoProcessor(input chan *Message, action func(*Message), workers int) chan *Message {
	output := make(chan *Message)

	size := 2 * workers

	workersIn := make(chan *Task, size)
	workersOut := make(chan int, size)

	var wg sync.WaitGroup

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		worker(workersIn, workersOut, action, &wg)
	}

	ring := make([]Slot, size)
	var head, tail int
	length := 0
	tokens := make(chan struct{}, size)
	var mu sync.Mutex

	go func() {
		defer func() {
			close(workersIn)
			wg.Wait()
			close(workersOut)
		}()
		for msg := range input {
			//fmt.Println(msg.id)
			tokens <- struct{}{}
			mu.Lock()
			index := tail
			ring[tail].msg = msg
			tail = (tail + 1) % size
			length++
			mu.Unlock()

			workersIn <- &Task{index, msg}
		}
	}()

	flush := func() {
		mu.Lock()
		defer mu.Unlock()

		for length > 0 && ring[head].processed {
			//fmt.Println("flushed", ring[head].msg.id)
			output <- ring[head].msg
			head = (head + 1) % size
			length--
			<-tokens
		}
	}

	go func() {
		defer func() {
			close(output)
		}()
		for index := range workersOut {
			//fmt.Println("processed", slot.msg.id)
			ring[index].processed = true
			if index == head {
				flush()
			}
		}
	}()

	return output
}

func worker(input chan *Task, output chan int, action func(*Message), wg *sync.WaitGroup) {
	go func() {
		for task := range input {
			action(task.msg)
			output <- task.index
		}
		wg.Done()
	}()
}

// Every message to worker has two channels.
// One of them is used to wait for the previous worker
// Another to notify next worker.

// with this approach workers can execute jobs in parallel,
// but every worker need to wait for the previous one to
// send the message. So order of messages is preserved.

type Segment struct {
	msg  *Message
	prev <-chan struct{}
	next chan<- struct{}
}

func chainProcessor(input chan *Message, action func(*Message), workers int) chan *Message {
	output := make(chan *Message)

	size := workers

	workersIn := make(chan *Segment, size)

	prev := make(chan struct{}, 1)
	next := make(chan struct{}, 1)
	prev <- struct{}{}

	var wg sync.WaitGroup

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		segmentWorker(workersIn, output, action, &wg)
	}

	go func() {
		defer func() {
			close(workersIn)
			wg.Wait()
			close(output)
		}()
		for msg := range input {
			workersIn <- &Segment{msg, prev, next}
			prev = next
			next = make(chan struct{}, 1)
		}
	}()

	return output
}

func segmentWorker(input <-chan *Segment, output chan *Message, action func(*Message), wg *sync.WaitGroup) {
	go func() {
		for segment := range input {
			action(segment.msg)

			// Wait for prev worker
			<-segment.prev

			// Send message
			output <- segment.msg

			// Notify next
			segment.next <- struct{}{}
		}
		wg.Done()
	}()
}
