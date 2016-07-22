// Package workwork provides a Worker instance which will perform work that is
// queued up to it while managing a worker pool to help keep operations
// asynchronous.
//
// Components of this code have been sourced directly from SegmentIO's
// analytics-go available:
//
// https://github.com/segmentio/analytics-go/tree/2d840d861c322bdf5346ba7917af1c2285e653d3
//
// analytics-go is licensed under a MIT license.
package workwork

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"
)

const (
	// internalSize is the maximum size of the msg channel.
	internalSize = 100

	// internalGoRoutineAsyncMax is the maximum amount of processing jobs allowed
	// at a time.
	internalGoRoutineAsyncMax = 500
)

// WorkFunc describes a function that is executed with the msg for the worker.
type WorkFunc func(msg []interface{}) error

// Worker is the instance that does the actual queuing and processing.
type Worker struct {
	Interval time.Duration
	Size     int
	Verbose  bool
	Logger   *log.Logger

	work     WorkFunc
	msgs     chan interface{}
	quit     chan struct{}
	shutdown chan struct{}
	once     sync.Once
	wg       sync.WaitGroup

	// These synchronization primitives are used to control how many goroutines
	// are spawned by the client for uploads.
	upmtx   sync.Mutex
	upcond  sync.Cond
	upcount int
}

// WorkerOpts is the options that are needed to create a new Worker.
type WorkerOpts struct {
	Interval time.Duration
	Size     int
	Verbose  bool
	Logger   *log.Logger
}

// NewWorker creates a new worker based on the spec provided. It will allow the
// queing of work which will flush when either the Size is reached in the buffer
// or the interval is reached, which ever occurs first.
func NewWorker(work WorkFunc, opts WorkerOpts) *Worker {
	w := &Worker{
		Interval: opts.Interval,
		Size:     opts.Size,
		Verbose:  opts.Verbose,
		Logger:   opts.Logger,
		work:     work,
		msgs:     make(chan interface{}),
		quit:     make(chan struct{}),
		shutdown: make(chan struct{}),
	}

	// setup the locker for the condition
	w.upcond.L = &w.upmtx

	return w
}

// Verbose log.
func (w *Worker) verbose(function, msg string, args ...interface{}) {
	if w.Verbose {
		w.Logger.Printf("workwork : worker : %s : %s", function, fmt.Sprintf(msg, args...))
	}
}

// Error log.
func (w *Worker) error(function string, err error, msg string, args ...interface{}) {
	w.Logger.Printf("workwork : worker : %s : %s : %s", function, err.Error(), fmt.Sprintf(msg, args...))
}

// startLoop starts the main looping routine that will consume the queue
// elements.
func (w *Worker) startLoop() {
	go w.loop()
}

// Queue adds the msg to the worker to be processed as well as verifying that
// the msg passed in is not a pointer and that the processing queue is started.
func (w *Worker) Queue(msg interface{}) error {
	if rv := reflect.ValueOf(msg); rv.Kind() == reflect.Ptr {
		return errors.New("can't queue a pointer")
	}

	w.once.Do(w.startLoop)
	w.msgs <- msg

	return nil
}

// sendAsync sends a batch of messages to the worker function asynchronously.
func (w *Worker) sendAsync(msgs []interface{}) {
	w.upmtx.Lock()
	for w.upcount >= internalGoRoutineAsyncMax {
		w.upcond.Wait()
	}
	w.upcount++
	w.upmtx.Unlock()
	w.wg.Add(1)

	go func() {
		err := w.work(msgs)
		if err != nil {
			w.error("sendAsync", err, "couldn't work, an error occured executing the work")
		}

		w.upmtx.Lock()
		w.upcount--
		w.upcond.Signal()
		w.upmtx.Unlock()
		w.wg.Done()
	}()
}

// Batch loop.
func (w *Worker) loop() {
	var msgs []interface{}
	tick := time.NewTicker(w.Interval)

	for {
		select {
		case msg := <-w.msgs:
			w.verbose("loop", "buffer (%d/%d) %v", len(msgs), w.Size, msg)
			msgs = append(msgs, msg)
			if len(msgs) == w.Size {
				w.verbose("loop", "exceeded %d messages – flushing", w.Size)
				w.sendAsync(msgs)
				msgs = make([]interface{}, 0, w.Size)
			}
		case <-tick.C:
			if len(msgs) > 0 {
				w.verbose("loop", "interval reached - flushing %d", len(msgs))
				w.sendAsync(msgs)
				msgs = make([]interface{}, 0, w.Size)
			}
		case <-w.quit:
			w.verbose("loop", "exit requested – draining msgs")
			tick.Stop()
			// drain the msg channel.
			for msg := range w.msgs {
				w.verbose("loop", "buffer (%d/%d) %v", len(msgs), w.Size, msg)
				msgs = append(msgs, msg)
			}
			w.verbose("loop", "exit requested – flushing %d", len(msgs))
			w.sendAsync(msgs)
			w.wg.Wait()
			w.verbose("loop", "exit")
			w.shutdown <- struct{}{}
			return
		}
	}
}

// Close and flush cache requests.
func (w *Worker) Close() error {
	w.once.Do(w.startLoop)
	w.quit <- struct{}{}
	close(w.msgs)
	<-w.shutdown
	return nil
}
