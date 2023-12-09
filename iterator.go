package iterator

import (
	"errors"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"
)

type Iterable[V any] func(func(V) bool) error

func Empty[V any]() Iterable[V] {
	return func(yield func(V) bool) error {
		return nil
	}
}

func Single[V any](v V) Iterable[V] {
	return func(yield func(V) bool) error {
		yield(v)
		return nil
	}
}

// Slice create an Iterable from a slice
func Slice[V any](items []V) Iterable[V] {
	return func(yield func(V) bool) error {
		for _, i := range items {
			if !yield(i) {
				return nil
			}
		}
		return nil
	}
}

// Append appends to Iterables
func Append[V any](iterables ...Iterable[V]) Iterable[V] {
	return func(yield func(V) bool) error {
		for _, it := range iterables {
			var run bool
			err := it(func(v V) bool {
				run = yield(v)
				return run
			})
			if err != nil {
				return err
			}
			if !run {
				return nil
			}
		}
		return nil
	}
}

func Generate[V any](n int, gen func(i int) (V, error)) Iterable[V] {
	return func(yield func(V) bool) error {
		for i := 0; i < n; i++ {
			v, err := gen(i)
			if err != nil {
				return err
			}
			if !yield(v) {
				return nil
			}
		}
		return nil
	}
}

func toError(rec any) error {
	if err, ok := rec.(error); ok {
		return err
	} else {
		return fmt.Errorf("%v", rec)
	}
}

// ToChan writes elements to a channel
func ToChan[V any](it Iterable[V]) (<-chan V, chan struct{}, chan error) {
	c := make(chan V)
	stop := make(chan struct{})
	panicChan := make(chan error)
	innerChannel := c
	go func() {
		defer func() {
			rec := recover()
			if rec != nil {
				panicChan <- toError(rec)
			}
			close(innerChannel)
			innerChannel = nil
		}()
		err := it(func(v V) bool {
			select {
			case innerChannel <- v:
				return true
			case <-stop:
				return false
			}
		})
		if err != nil {
			panicChan <- err
		}
	}()
	return c, stop, panicChan
}

// FromChan reads items from a channel
func FromChan[V any](c <-chan V, stop chan<- struct{}, panicReadeAndFire <-chan error) Iterable[V] {
	return func(yield func(V) bool) error {
		defer close(stop)
		for {
			select {
			case v, ok := <-c:
				if ok {
					if !yield(v) {
						return nil
					}
				} else {
					return nil
				}
			case p := <-panicReadeAndFire:
				return p
			}
		}
	}
}

// ToSlice reads all items from the Iterator and stores them in a slice.
func ToSlice[V any](it Iterable[V]) ([]V, error) {
	var sl []V
	err := it(func(v V) bool {
		sl = append(sl, v)
		return true
	})
	return sl, err
}

// Equals checks if the two Iterators are equal.
func Equals[V any](i1, i2 Iterable[V], equals func(V, V) (bool, error)) (bool, error) {
	cMain1, stop1, p1 := ToChan(i1)
	cMain2, stop2, p2 := ToChan(i2)
	defer func() {
		close(stop1)
		close(stop2)
	}()

	var item1, item2 V
	var ok1, ok2 bool
	c1 := cMain1
	c2 := cMain2
	for {
		select {
		case item1, ok1 = <-c1:
			c1 = nil
			if c2 == nil {
				if !ok1 && !ok2 {
					return true, nil
				} else if ok1 != ok2 {
					return false, nil
				} else if ok1 && ok2 {
					eq, err := equals(item1, item2)
					if err != nil {
						return false, err
					}
					if !eq {
						return false, nil
					}
					c1 = cMain1
					c2 = cMain2
				}
			}
		case item2, ok2 = <-c2:
			c2 = nil
			if c1 == nil {
				if !ok1 && !ok2 {
					return true, nil
				} else if ok1 != ok2 {
					return false, nil
				} else if ok1 && ok2 {
					eq, err := equals(item1, item2)
					if err != nil {
						return false, err
					}
					if !eq {
						return false, nil
					}
					c1 = cMain1
					c2 = cMain2
				}
			}
		case p := <-p1:
			return false, p
		case p := <-p2:
			return false, p
		}
	}
}

// Map maps the elements to new element created by the given mapFunc function
func Map[I, O any](in Iterable[I], mapFunc func(int, I) (O, error)) Iterable[O] {
	return mapIterator[I, O](in, mapFunc, 0)
}

func mapIterator[I, O any](iter Iterable[I], mapFunc func(int, I) (O, error), num int) Iterable[O] {
	return func(yield func(O) bool) error {
		var innerErr error
		err := iter(func(item I) bool {
			o, err := mapFunc(num, item)
			if err != nil {
				innerErr = err
				return false
			}
			if !yield(o) {
				return false
			}
			num++
			return true
		})
		if err != nil {
			return err
		}
		return innerErr
	}
}

type container[V any] struct {
	num int
	val V
}

func splitWork[I, O any](jobs <-chan container[I], closed <-chan struct{}, panicChan chan<- error, mapFuncFac func() func(n int, i I) (O, error)) <-chan container[O] {
	result := make(chan container[O])
	wg := sync.WaitGroup{}
	for n := 0; n < runtime.NumCPU(); n++ {
		wg.Add(1)
		mapFunc := mapFuncFac()
		go func() {
			defer wg.Done()

			for ci := range jobs {
				o, err := mapFunc(ci.num, ci.val)
				if err != nil {
					panicChan <- err
					return
				}
				select {
				case result <- container[O]{ci.num, o}:
					if err != nil {
						return
					}
				case <-closed:
					return
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(result)
	}()
	return result
}

func collectResults[O any](result <-chan container[O], closed chan<- struct{}, panicReadAndFire <-chan error, ack chan<- struct{}, yield func(o O) bool, num int) error {
	if ack != nil {
		defer close(ack)
	}

	store := map[int]O{}

	sendAvail := func() bool {
		for {
			if o, ok := store[num]; ok {
				if !yield(o) {
					close(closed)
					return false
				}
				delete(store, num)
				num++
			} else {
				return true
			}
		}
	}

	for {
		select {
		case co, ok := <-result:
			if ok {
				if co.num == num {
					if !yield(co.val) {
						close(closed)
						return nil
					}
					num++
					if !sendAvail() {
						return nil
					}
				} else {
					store[co.num] = co.val
				}
			} else {
				sendAvail()
				return nil
			}
		case p := <-panicReadAndFire:
			return p
		}
	}
}

// MapParallel behaves the same as the Map Iterable.
// The mapping is distributed over all available cores.
func MapParallel[I, O any](iter Iterable[I], mapFuncFac func() func(int, I) (O, error)) Iterable[O] {
	return func(yield func(O) bool) error {
		jobs, closed, panicChan := ToChan(mapIterator(iter, func(n int, item I) (container[I], error) {
			return container[I]{num: n, val: item}, nil
		}, 0))

		result := splitWork(jobs, closed, panicChan, mapFuncFac)

		return collectResults(result, closed, panicChan, nil, yield, 0)
	}
}

// MapAuto behaves the same as the Map Iterable.
// It is measured how long the map function takes. If the map function requires so much
// computing time that it is worth distributing it over several cores, the map function
// is distributed over all available cores (reported by runtime.NumCPU()).
func MapAuto[I, O any](iter Iterable[I], mapFuncFac func() func(int, I) (O, error)) Iterable[O] {
	if runtime.NumCPU() == 1 {
		return Map(iter, mapFuncFac())
	}

	return func(yield func(O) bool) error {
		next := measure[I, O](yield, mapFuncFac)
		var innerErr error
		var cleanUp func()
		err := iter(func(item I) bool {
			next, cleanUp, innerErr = next(item)
			return next != nil && innerErr == nil
		})
		if cleanUp != nil {
			cleanUp()
		}
		if innerErr != nil {
			return innerErr
		}
		return err
	}
}

type nextType[V any] func(v V) (nextType[V], func(), error)

const (
	itemProcessingTimeMicroSec = 200
	itemsToMeasure             = 11
)

func measure[I, O any](yield func(O) bool, mapFuncFac func() func(int, I) (O, error)) nextType[I] {
	var meas nextType[I]
	var dur time.Duration
	var count int
	mapFunc := mapFuncFac()
	meas = func(v I) (nextType[I], func(), error) {
		start := time.Now()
		o, err := mapFunc(count, v)
		if err != nil {
			return nil, nil, err
		}
		if count > 0 {
			// ignore first call to avoid errors due to setup costs
			dur += time.Now().Sub(start)
		}
		if !yield(o) {
			return nil, nil, nil
		}
		count++
		if count < itemsToMeasure {
			return meas, nil, nil
		} else {
			if dur.Microseconds() > itemProcessingTimeMicroSec*int64(count-1) {
				log.Printf("switched to parallel map: time spend in measured mapFunc calls: %v", dur/(itemsToMeasure-1))
				return parallel(yield, mapFuncFac, count)
			} else {
				return serial(yield, mapFunc, count)
			}
		}
	}
	return meas
}

func serial[I, O any](yield func(O) bool, mapFunc func(int, I) (O, error), count int) (nextType[I], func(), error) {
	var ser nextType[I]
	ser = func(v I) (nextType[I], func(), error) {
		o, err := mapFunc(count, v)
		if err != nil {
			return nil, nil, err
		}
		if !yield(o) {
			return nil, nil, nil
		} else {
			count++
			return ser, nil, nil
		}
	}
	return ser, nil, nil
}

func parallel[I, O any](yield func(O) bool, mapFuncFac func() func(int, I) (O, error), num int) (nextType[I], func(), error) {
	jobs := make(chan container[I])
	stop := make(chan struct{})
	panicChan := make(chan error)
	ack := make(chan struct{})

	i := num
	cleanUp := func() {
		close(jobs)
		<-ack
		log.Printf("items mapped in parallel: %d", i-itemsToMeasure)
	}

	var par nextType[I]
	par = func(v I) (nextType[I], func(), error) {
		select {
		case jobs <- container[I]{i, v}:
			i++
			return par, cleanUp, nil
		case <-stop:
			return nil, cleanUp, nil
		case p := <-panicChan:
			return nil, cleanUp, p
		}
	}

	result := splitWork(jobs, stop, panicChan, mapFuncFac)

	go func() {
		defer func() {
			rec := recover()
			if rec != nil {
				panicChan <- toError(rec)
			}
		}()
		err := collectResults(result, stop, nil, ack, yield, num)
		if err != nil {
			panicChan <- err
		}
	}()

	return par, cleanUp, nil
}

// Filter filters the given Iterable by the given accept function
func Filter[V any](iter Iterable[V], accept func(V) (bool, error)) Iterable[V] {
	return func(yield func(V) bool) error {
		var innerErr error
		err := iter(func(v V) bool {
			b, err := accept(v)
			if err != nil {
				innerErr = err
				return false
			}
			if b {
				if !yield(v) {
					return false
				}
			}
			return true
		})
		if innerErr != nil {
			return innerErr
		}
		return err
	}
}

// FilterAuto behaves the same as the Filter Iterable.
// It is measured how long the accept function takes. If the accept function requires so much
// computing time that it is worth distributing it over several cores, the filtering
// is distributed over several cores.
func FilterAuto[V any](in Iterable[V], acceptFac func() func(V) (bool, error)) Iterable[V] {
	if runtime.NumCPU() == 1 {
		return Filter(in, acceptFac())
	}

	return func(yield func(V) bool) error {
		return MapAuto(in, func() func(i int, val V) (filterContainer[V], error) {
			accept := acceptFac()
			return func(i int, val V) (filterContainer[V], error) {
				b, err := accept(val)
				if err != nil {
					return filterContainer[V]{}, err
				}
				return filterContainer[V]{val, b}, nil
			}
		})(func(v filterContainer[V]) bool {
			if v.accept {
				return yield(v.val)
			} else {
				return true
			}
		})
	}
}

type filterContainer[V any] struct {
	val    V
	accept bool
}

// FilterParallel behaves the same as the Filter Iterable.
// The filtering is distributed over all available cores.
func FilterParallel[V any](in Iterable[V], acceptFac func() func(V) (bool, error)) Iterable[V] {
	return func(yield func(V) bool) error {
		return MapParallel[V, filterContainer[V]](in, func() func(i int, val V) (filterContainer[V], error) {
			accept := acceptFac()
			return func(i int, val V) (filterContainer[V], error) {
				b, err := accept(val)
				if err != nil {
					return filterContainer[V]{}, err
				}
				return filterContainer[V]{val, b}, nil
			}
		})(func(v filterContainer[V]) bool {
			if v.accept {
				return yield(v.val)
			} else {
				return true
			}
		})
	}
}

// Compact returns an iterable which contains no consecutive duplicates.
func Compact[V any](iter Iterable[V], equal func(V, V) (bool, error)) Iterable[V] {
	return func(yield func(V) bool) error {
		isLast := false
		var last V
		var innerErr error
		err := iter(func(v V) bool {
			if isLast {
				eq, err := equal(last, v)
				if err != nil {
					innerErr = err
					return false
				}
				if !eq {
					if !yield(v) {
						return false
					}
				}
			} else {
				isLast = true
				if !yield(v) {
					return false
				}
			}
			last = v
			return true
		})
		if innerErr != nil {
			return innerErr
		}
		return err
	}
}

// Group returns an iterable which contains iterables of equal values
func Group[V any](iter Iterable[V], equal func(V, V) (bool, error)) Iterable[Iterable[V]] {
	return func(yield func(Iterable[V]) bool) error {
		var list []V
		var innerErr error
		err := iter(func(v V) bool {
			if len(list) > 0 {
				eq, err := equal(list[len(list)-1], v)
				if err != nil {
					innerErr = err
					return false
				}
				if eq {
					list = append(list, v)
				} else {
					if !yield(Slice(list)) {
						return false
					}
					list = []V{v}
				}
			} else {
				list = []V{v}
			}
			return true
		})
		if innerErr != nil {
			return innerErr
		}
		if len(list) > 0 {
			yield(Slice(list))
			return nil
		}
		return err
	}
}

// Combine maps two consecutive elements to a new element.
// The generated iterable has one element less than the original iterable.
func Combine[I, O any](in Iterable[I], combine func(I, I) (O, error)) Iterable[O] {
	return func(yield func(O) bool) error {
		isValue := false
		var innerErr error
		var last I
		err := in(func(i I) bool {
			if isValue {
				o, err := combine(last, i)
				if err != nil {
					innerErr = err
					return false
				}
				if !yield(o) {
					return false
				}
			} else {
				isValue = true
			}
			last = i
			return true
		})
		if innerErr != nil {
			return innerErr
		}
		return err
	}
}

// Combine3 maps three consecutive elements to a new element.
// The generated iterable has two elements less than the original iterable.
func Combine3[I, O any](iter Iterable[I], combine func(I, I, I) (O, error)) Iterable[O] {
	return func(yield func(O) bool) error {
		valuesPresent := 0
		var lastLast, last I
		var innerErr error
		err := iter(func(i I) bool {
			switch valuesPresent {
			case 0:
				valuesPresent = 1
				lastLast = i
				return true
			case 1:
				valuesPresent = 2
				last = i
				return true
			default:
				o, err := combine(lastLast, last, i)
				if err != nil {
					innerErr = err
					return false
				}
				lastLast = last
				last = i
				return yield(o)
			}
		})
		if innerErr != nil {
			return innerErr
		}
		return err
	}
}

// CombineN maps N consecutive elements to a new element.
// The generated iterable has (N-1) elements less than the original iterable.
func CombineN[I, O any](iter Iterable[I], n int, combine func(i0 int, v []I) (O, error)) Iterable[O] {
	return func(yield func(O) bool) error {
		valuesPresent := 0
		pos := 0
		vals := make([]I, n, n)
		var innerErr error
		err := iter(func(i I) bool {
			vals[pos] = i
			pos++
			if pos == n {
				pos = 0
			}
			if valuesPresent < n {
				valuesPresent++
			}
			if valuesPresent == n {
				o, err := combine(pos, vals)
				if err != nil {
					innerErr = err
					return false
				}
				return yield(o)
			}
			return true
		})
		if innerErr != nil {
			return innerErr
		}
		return err
	}
}

// Merge is used to merge two iterables.
// The less function determines which element to take first
// Makes sens only if the provided iterables are ordered.
func Merge[V any](ai, bi Iterable[V], less func(V, V) (bool, error)) Iterable[V] {
	return func(yield func(V) bool) error {
		aMain, aStop, aPanic := ToChan(ai)
		bMain, bStop, bPanic := ToChan(bi)
		defer func() {
			close(aStop)
			close(bStop)
		}()
		ar := aMain
		br := bMain
		var remainder <-chan V
		var a V
		var b V
		var ok bool
		for {
			select {
			case a, ok = <-ar:
				ar = nil
				if ok {
					if br == nil {
						le, err := less(a, b)
						if err != nil {
							return err
						}
						if le {
							if !yield(a) {
								return nil
							}
							ar = aMain
						} else {
							if !yield(b) {
								return nil
							}
							br = bMain
						}
					}
				} else {
					if br == nil {
						if !yield(b) {
							return nil
						}
					} else {
						br = nil
					}
					remainder = bMain
				}
			case b, ok = <-br:
				br = nil
				if ok {
					if ar == nil {
						le, err := less(a, b)
						if err != nil {
							return err
						}
						if le {
							if !yield(a) {
								return nil
							}
							ar = aMain
						} else {
							if !yield(b) {
								return nil
							}
							br = bMain
						}
					}
				} else {
					if ar == nil {
						if !yield(a) {
							return nil
						}
					} else {
						ar = nil
					}
					remainder = aMain
				}
			case r, ok := <-remainder:
				if ok {
					if !yield(r) {
						return nil
					}
				} else {
					return nil
				}
			case p := <-aPanic:
				return p
			case p := <-bPanic:
				return p
			}
		}
	}
}

// MergeElements is used to merge two iterables.
// The combine function creates the result of combining the two elements.
// The iterables must have the same size.
func MergeElements[A, B, C any](ai Iterable[A], bi Iterable[B], combine func(A, B) (C, error)) Iterable[C] {
	return func(yield func(C) bool) error {
		aMain, aStop, aPanic := ToChan(ai)
		bMain, bStop, bPanic := ToChan(bi)
		defer func() {
			close(aStop)
			close(bStop)
		}()
		for {
			var a A
			var aOk bool
			select {
			case a, aOk = <-aMain:
			case p := <-aPanic:
				return p
			case p := <-bPanic:
				return p
			}
			var b B
			var bOk bool
			select {
			case b, bOk = <-bMain:
			case p := <-aPanic:
				return p
			case p := <-bPanic:
				return p
			}

			if aOk && bOk {
				cb, err := combine(a, b)
				if err != nil {
					return err
				}
				if !yield(cb) {
					return nil
				}
			} else if aOk || bOk {
				return errors.New("iterables in mergeElements dont have the same size")
			} else {
				return nil
			}
		}
	}
}

// Cross is used to cross two iterables.
func Cross[A, B, C any](a Iterable[A], b Iterable[B], cross func(A, B) (C, error)) Iterable[C] {
	return func(yield func(C) bool) error {
		var innerErr error
		err := a(func(a A) bool {
			ierr := b(func(b B) bool {
				c, cerr := cross(a, b)
				if cerr != nil {
					innerErr = cerr
					return false
				}
				if !yield(c) {
					return false
				}
				return true
			})
			if ierr != nil {
				innerErr = ierr
				return false
			}
			return ierr == nil
		})
		if innerErr != nil {
			return innerErr
		}
		return err
	}
}

// IirMap maps a value, the last value and the last created value to a new element.
// Can be used to implement iir filters like a low-pass. The last item is provided
// to allow handling of non-equidistant values.
func IirMap[I any, R any](items Iterable[I], initial func(item I) (R, error), iir func(item I, lastItem I, last R) (R, error)) Iterable[R] {
	return func(yield func(R) bool) error {
		isLast := false
		var lastItem I
		var last R
		var innerErr error
		err := items(func(i I) bool {
			var err error
			if isLast {
				last, err = iir(i, lastItem, last)
				if err != nil {
					innerErr = err
					return false
				}
			} else {
				last, err = initial(i)
				if err != nil {
					innerErr = err
					return false
				}
				isLast = true
			}
			lastItem = i
			return yield(last)
		})
		if innerErr != nil {
			return innerErr
		}
		return err
	}
}

// FirstN returns the first element of an Iterable
func FirstN[V any](items Iterable[V], n int) Iterable[V] {
	return func(yield func(V) bool) error {
		i := 0
		return items(func(v V) bool {
			if i < n {
				i++
				return yield(v)
			} else {
				return false
			}
		})
	}
}

// Skip skips the first elements.
// The number of elements to skip is given in skip.
func Skip[V any](items Iterable[V], n int) Iterable[V] {
	return func(yield func(V) bool) error {
		i := 0
		return items(func(v V) bool {
			if i < n {
				i++
				return true
			} else {
				return yield(v)
			}
		})
	}
}

// Thinning returns an iterable which skips a certain amount of elements
// from the parent iterable. If skip is set to 1, every second element is skipped.
// The first and the last item are always returned.
func Thinning[V any](iter Iterable[V], n int) Iterable[V] {
	return func(yield func(V) bool) error {
		i := 0
		var skipped V
		err := iter(func(v V) bool {
			if i == 0 {
				i = n
				return yield(v)
			} else {
				skipped = v
				i--
				return true
			}
		})
		if err == nil {
			if i < n {
				yield(skipped)
				return nil
			} else {
				return nil
			}
		}
		return err
	}
}

// Reduce reduces the items of the iterable to a single value by calling the reduce function.
func Reduce[V any](it Iterable[V], reduceFunc func(V, V) (V, error)) (V, error) {
	var sum V
	isValue := false
	var innerErr error
	err := it(func(v V) bool {
		var err error
		if isValue {
			sum, err = reduceFunc(sum, v)
			if err != nil {
				innerErr = err
				return false
			}
		} else {
			sum = v
			isValue = true
		}
		return true
	})
	if innerErr != nil {
		return sum, innerErr
	}
	if err != nil {
		return sum, err
	}
	if !isValue {
		return sum, errors.New("reduce on empty iterable")
	}
	return sum, nil
}

// ReduceParallel usage makes sens only if reduce operation is costly.
// In cases like a+b, a*b or "if a>b then a else b" it makes no sense at all
// because the synchronization is more expensive than the operation itself.
// It should always be possible to do the heavy lifting in a map operation and
// make the reduce operation low cost.
func ReduceParallel[V any](it Iterable[V], reduceFuncFac func() func(V, V) (V, error)) (V, error) {
	valChan, stop, wasPanic := ToChan(it)

	result := make(chan V)
	var wg sync.WaitGroup
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		reduceFunc := reduceFuncFac()
		go func() {
			defer func() {
				rec := recover()
				if rec != nil {
					wasPanic <- toError(rec)
				}
				wg.Done()
			}()
			var sum V
			isSum := false
			for v := range valChan {
				if isSum {
					var err error
					sum, err = reduceFunc(sum, v)
					if err != nil {
						wasPanic <- err
						return
					}
				} else {
					sum = v
					isSum = true
				}
			}
			if isSum {
				result <- sum
			}
		}()
	}
	go func() {
		wg.Wait()
		close(result)
	}()
	var sum V
	isSum := false
	reduceFunc := reduceFuncFac()
	for {
		select {
		case r, ok := <-result:
			if ok {
				if isSum {
					var err error
					sum, err = reduceFunc(sum, r)
					if err != nil {
						return sum, err
					}
				} else {
					sum = r
					isSum = true
				}
			} else {
				if !isSum {
					return sum, errors.New("reduce on empty iterable")
				}
				return sum, nil
			}
		case p := <-wasPanic:
			close(stop)
			return sum, p
		}
	}
}

// MapReduce combines a map and reduce step in one go.
// Avoids generating intermediate map results.
// Instead of map(n->n^2).reduce((a,b)->a+b) one
// can write  mapReduce(0, (s,n)->s+n^2)
// Useful if map and reduce are both low cost operations.
func MapReduce[S, V any](it Iterable[V], initial S, reduceFunc func(S, V) (S, error)) (S, error) {
	var innerErr error
	err := it(func(v V) bool {
		var err error
		initial, err = reduceFunc(initial, v)
		if err != nil {
			innerErr = err
			return false
		}
		return true
	})
	if innerErr != nil {
		return initial, innerErr
	}
	if err != nil {
		return initial, err
	}
	return initial, nil
}

// First returns the first item of the iterator
// The returned bool is false if there is no item because iterator is empty.
func First[V any](in Iterable[V]) (V, error) {
	var first V
	isFirst := false
	err := in(func(v V) bool {
		first = v
		isFirst = true
		return false
	})
	if err != nil {
		return first, err
	}
	if isFirst {
		return first, nil
	} else {
		return first, errors.New("iterator is empty")
	}
}

// Peek takes an iterator, gives a new iterator and the first item of the given iterator.
// The returned iterator still iterates all items including the first one.
// Expensive because all items have to go through a channel.
// Use only if the creation of the original iterator is even more expensive.
func Peek[V any](it Iterable[V]) (Iterable[V], V, error) {
	c, stop, panicChan := ToChan[V](it)
	var zero V
	var first V
	var ok bool
	select {
	case first, ok = <-c:
		if !ok {
			return nil, zero, errors.New("iterator is empty")
		}
	case p := <-panicChan:
		return nil, zero, p
	}

	iter := func(yield func(V) bool) error {
		if !yield(first) {
			return nil
		}
		return FromChan[V](c, stop, panicChan)(yield)
	}

	return iter, first, nil
}
