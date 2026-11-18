package camunda

import (
	"context"
	"sync"
)

// ItemFailure is one item of a ForEachBounded run that failed.
type ItemFailure struct {
	Label string
	Err   error
}

// ForEachBounded runs fn for each item with at most workers at a time, and
// stops handing out items once ctx is done or any call has failed.
//
// Callers use it for listings made of many small requests (one per day, or per
// day and prefix) where the whole listing is only usable if every request
// succeeded. After the first failure the remaining requests could change
// nothing, so they are not made: a down or slow service is not asked another
// few hundred times until the scan's deadline. Requests already in flight finish.
//
// It returns how many items were started and the failures, in item order. label
// names an item in those failures. A run that stopped on ctx reports it as a
// failure of the first item it did not start, so the caller cannot mistake a
// partial run for a complete one.
func ForEachBounded[T any](ctx context.Context, items []T, workers int, label func(T) string, fn func(context.Context, T) error) (started int, failures []ItemFailure) {
	if len(items) == 0 {
		return 0, nil
	}
	if workers < 1 {
		workers = 1
	}
	if workers > len(items) {
		workers = len(items)
	}

	var (
		mu     sync.Mutex
		failed bool
		errs   = make([]error, len(items))
		ran    = make([]bool, len(items))
		next   int
		wg     sync.WaitGroup
	)

	// take hands out the next item, or reports there is none to hand out.
	take := func() (int, bool) {
		mu.Lock()
		defer mu.Unlock()
		if failed || ctx.Err() != nil || next >= len(items) {
			return 0, false
		}
		i := next
		next++
		ran[i] = true
		return i, true
	}

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i, ok := take()
				if !ok {
					return
				}
				if err := fn(ctx, items[i]); err != nil {
					mu.Lock()
					errs[i] = err
					failed = true
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			failures = append(failures, ItemFailure{Label: label(items[i]), Err: err})
		}
	}
	for _, r := range ran {
		if r {
			started++
		}
	}
	if started < len(items) && len(failures) == 0 {
		// Stopped by the context without any request failing.
		failures = append(failures, ItemFailure{Label: label(items[started]), Err: ctx.Err()})
	}
	return started, failures
}
