package iterator

import (
	"errors"
	"log"
	"runtime"
	"sync"
	"time"
)

// SBC is returned by the Consumer to stop the producer
// A producer should see this error as normal termination by the consumer.
// The producer should return this error unwrapped to its caller.
var SBC = errors.New("stopped by consumer")

// Consumer consumes elements of type V
type Consumer[V any] func(V) error

// Producer produces elements of type V
// The Context C is used to pass data to the producer which is required to produce the
// items to be consumed.
// The Producer calls the Consumer for each item produced.
// The Producer returns an error if the production of items or the consumption fails.
// If the Consumer returns an error, the Producer stops producing items.
// The error is returned to the caller of the Producer.
// Also, the StopByConsumer error needs to be passed to the caller of the producer.
type Producer[V, C any] func(C, Consumer[V]) error

func Empty[V, C any]() Producer[V, C] {
	return func(c C, yield Consumer[V]) error {
		return nil
	}
}

func Single[V, C any](v V) Producer[V, C] {
	return func(c C, yield Consumer[V]) error {
		return yield(v)
	}
}

// Slice create an Iterable from a slice
func Slice[V, C any](items []V) Producer[V, C] {
	return func(c C, yield Consumer[V]) error {
		for _, i := range items {
			if e := yield(i); e != nil {
				return e
			}
		}
		return nil
	}
}

// Append appends to Iterables
func Append[V, C any](iterables ...Producer[V, C]) Producer[V, C] {
	return func(c C, yield Consumer[V]) error {
		for _, it := range iterables {
			if err := it(c, yield); err != nil {
				return err
			}
		}
		return nil
	}
}

func Generate[V, C any](n int, gen func(i int) (V, error)) Producer[V, C] {
	return func(c C, yield Consumer[V]) error {
		for i := 0; i < n; i++ {
			v, err := gen(i)
			if err != nil {
				return err
			}
			if e := yield(v); e != nil {
				return e
			}
		}
		return nil
	}
}

type container[V any] struct {
	num int
	val V
	err error
}

// ToChan writes elements to a channel
func ToChan[V, C any](co C, it Producer[V, C]) (<-chan container[V], chan struct{}) {
	c := make(chan container[V])
	done := make(chan struct{})
	go func() {
		i := 0
		err := it(co, func(v V) error {
			select {
			case c <- container[V]{num: i, val: v}:
				i++
				return nil
			case <-done:
				return SBC
			}
		})
		if err != nil && err != SBC {
			c <- container[V]{num: i, err: err}
		}
		close(c)
	}()
	return c, done
}

// ToSlice reads all items from the Producer and stores them in a slice.
func ToSlice[V, C any](c C, it Producer[V, C]) ([]V, error) {
	var sl []V
	err := it(c, func(v V) error {
		sl = append(sl, v)
		return nil
	})
	return sl, err
}

// Equals checks if the two Iterators are equal.
func Equals[V, C any](c1, c2 C, i1, i2 Producer[V, C], equals func(V, V) (bool, error)) (bool, error) {
	ch1, done1 := ToChan(c1, i1)
	ch2, done2 := ToChan(c2, i2)
	defer func() {
		close(done1)
		close(done2)
	}()

	for {
		c1, ok1 := <-ch1
		c2, ok2 := <-ch2
		if ok1 && ok2 {
			if c1.err != nil {
				return false, c1.err
			}
			if c2.err != nil {
				return false, c2.err
			}
			eq, err := equals(c1.val, c2.val)
			if err != nil {
				return false, err
			}
			if !eq {
				return false, nil
			}
		} else {
			return ok1 == ok2, nil
		}
	}
}

// Map maps the elements to new element created by the given mapFunc function
func Map[I, O, C any](in Producer[I, C], mapFunc func(int, I) (O, error)) Producer[O, C] {
	return func(c C, yield Consumer[O]) error {
		num := 0
		return in(c, func(item I) error {
			o, err := mapFunc(num, item)
			if err != nil {
				return err
			}
			if e := yield(o); e != nil {
				return e
			}
			num++
			return nil
		})
	}
}

// MapParallel behaves the same as the Map Iterable.
// The mapping is distributed over all available cores.
func MapParallel[I, O, C any](in Producer[I, C], mapFuncFac func() func(int, I) (O, error)) Producer[O, C] {
	if runtime.NumCPU() == 1 {
		return Map(in, mapFuncFac())
	}
	return func(c C, yield Consumer[O]) error {
		pc := createParallelConsumer(mapFuncFac, yield, 0)
		num := 0
		err := in(c, func(item I) error {
			err, _ := pc.doMap(num, item)
			num++
			if err != nil {
				return err
			}
			return nil
		})
		return pc.done(err)
	}
}

type mapConsumer[I, O any] interface {
	doMap(num int, item I) (error, mapConsumer[I, O])
	done(err error) error
}

const (
	itemProcessingTimeMicroSec = 200
	itemsToMeasure             = 11
)

type measureConsumer[I, O any] struct {
	mapFuncFac func() func(int, I) (O, error)
	mapFunc    func(int, I) (O, error)
	yield      Consumer[O]
	dur        time.Duration
	count      int
}

func (mc *measureConsumer[I, O]) doMap(num int, item I) (error, mapConsumer[I, O]) {
	start := time.Now()
	o, err := mc.mapFunc(num, item)
	dur := time.Since(start)

	if num > 0 {
		mc.dur += dur
		mc.count++
	}

	var next mapConsumer[I, O] = mc

	if num == itemsToMeasure {
		durPerItem := mc.dur.Microseconds() / int64(mc.count)
		//log.Printf("duration per item: %d\n", durPerItem)
		if durPerItem < itemProcessingTimeMicroSec {
			log.Println("sequential mapping")
			next = &sequentialConsumer[I, O]{mapFunc: mc.mapFunc, yield: mc.yield}
		} else {
			log.Println("parallel mapping")
			next = createParallelConsumer[I, O](mc.mapFuncFac, mc.yield, num+1)
		}
	}

	if err != nil {
		return err, next
	}
	if e := mc.yield(o); e != nil {
		return e, next
	}
	return nil, next
}

func (mc *measureConsumer[I, O]) done(err error) error {
	return err
}

type sequentialConsumer[I, O any] struct {
	mapFunc func(int, I) (O, error)
	yield   Consumer[O]
}

func (sc *sequentialConsumer[I, O]) doMap(num int, item I) (error, mapConsumer[I, O]) {
	o, err := sc.mapFunc(num, item)
	if err != nil {
		return err, sc
	}
	if e := sc.yield(o); e != nil {
		return e, sc
	}
	return nil, sc
}

func (sc *sequentialConsumer[I, O]) done(err error) error {
	return err
}

func createParallelConsumer[I, O any](mapFuncFac func() func(int, I) (O, error), yield Consumer[O], num int) mapConsumer[I, O] {
	c := make(chan container[I])
	r := make(chan container[O])
	errOccurred := make(chan struct{})
	errChan := make(chan error)

	var wg sync.WaitGroup
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func(ma func(int, I) (O, error)) {
			defer wg.Done()
			for ci := range c {
				o, err := ma(ci.num, ci.val)
				select {
				case r <- container[O]{num: ci.num, val: o, err: err}:
				case <-errOccurred:
					return
				}
			}
		}(mapFuncFac())
	}
	go func() {
		wg.Wait()
		close(r)
	}()
	go func() {
		cache := make(map[int]O)
		for co := range r {
			if co.err != nil {
				close(errOccurred)
				errChan <- co.err
				return
			}
			if co.num == num {
				if e := yield(co.val); e != nil {
					close(errOccurred)
					errChan <- co.err
					return
				}
				num++
				for {
					if o, ok := cache[num]; ok {
						if e := yield(o); e != nil {
							close(errOccurred)
							errChan <- co.err
							return
						}
						delete(cache, num)
						num++
					} else {
						break
					}
				}
			} else {
				cache[co.num] = co.val
			}
		}
		errChan <- nil
	}()

	return &parallelConsumer[I, O]{c: c, errChan: errChan, errOccurred: errOccurred}
}

type parallelConsumer[I, O any] struct {
	c           chan<- container[I]
	errChan     chan error
	errOccurred chan struct{}
}

func (pc *parallelConsumer[I, O]) doMap(num int, item I) (error, mapConsumer[I, O]) {
	select {
	case pc.c <- container[I]{num: num, val: item}:
	case <-pc.errOccurred:
		return SBC, pc
	}
	return nil, pc
}

func (pc *parallelConsumer[I, O]) done(err error) error {
	close(pc.c)

	e := <-pc.errChan
	if e != nil {
		return e
	}
	return err
}

// MapAuto behaves the same as the Map Iterable.
// It is measured how long the map function takes. If the map function requires so much
// computing time that it is worth distributing it over several cores, the map function
// is distributed over all available cores (reported by runtime.NumCPU()).
func MapAuto[I, O, C any](in Producer[I, C], mapFuncFac func() func(int, I) (O, error)) Producer[O, C] {
	if runtime.NumCPU() == 1 {
		return Map(in, mapFuncFac())
	}
	return func(c C, yield Consumer[O]) error {
		var con mapConsumer[I, O] = &measureConsumer[I, O]{mapFuncFac: mapFuncFac, mapFunc: mapFuncFac(), yield: yield}
		num := 0
		err := in(c, func(item I) error {
			var err error
			err, con = con.doMap(num, item)
			num++
			if err != nil {
				return err
			}
			return nil
		})
		return con.done(err)
	}
}

// Filter filters the given Iterable by the given accept function
func Filter[V, C any](in Producer[V, C], accept func(V) (bool, error)) Producer[V, C] {
	return func(c C, yield Consumer[V]) error {
		return in(c, func(v V) error {
			b, err := accept(v)
			if err != nil {
				return err
			}
			if b {
				if e := yield(v); e != nil {
					return e
				}
			}
			return nil
		})
	}
}

// FilterAuto behaves the same as the Filter Iterable.
// It is measured how long the accept function takes. If the accept function requires so much
// computing time that it is worth distributing it over several cores, the filtering
// is distributed over several cores.
func FilterAuto[V, C any](in Producer[V, C], acceptFac func() func(V) (bool, error)) Producer[V, C] {
	if runtime.NumCPU() == 1 {
		return Filter(in, acceptFac())
	}

	return func(c C, yield Consumer[V]) error {
		return MapAuto(in, func() func(i int, val V) (filterContainer[V], error) {
			accept := acceptFac()
			return func(i int, val V) (filterContainer[V], error) {
				b, err := accept(val)
				if err != nil {
					return filterContainer[V]{}, err
				}
				return filterContainer[V]{val, b}, nil
			}
		})(c, func(v filterContainer[V]) error {
			if v.accept {
				return yield(v.val)
			} else {
				return nil
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
func FilterParallel[V, C any](in Producer[V, C], acceptFac func() func(V) (bool, error)) Producer[V, C] {
	return func(c C, yield Consumer[V]) error {
		return MapParallel[V, filterContainer[V]](in, func() func(i int, val V) (filterContainer[V], error) {
			accept := acceptFac()
			return func(i int, val V) (filterContainer[V], error) {
				b, err := accept(val)
				if err != nil {
					return filterContainer[V]{}, err
				}
				return filterContainer[V]{val, b}, nil
			}
		})(c, func(v filterContainer[V]) error {
			if v.accept {
				return yield(v.val)
			} else {
				return nil
			}
		})
	}
}

// Compact returns an iterable which contains no consecutive duplicates.
func Compact[V, M, C any](items Producer[V, C], convert func(C, V) (M, error), equal func(C, M, M) (bool, error)) Producer[V, C] {
	return func(c C, yield Consumer[V]) error {
		isLast := false
		var last M
		return items(c, func(v V) error {
			val, err := convert(c, v)
			if err != nil {
				return err
			}
			if isLast {
				eq, err := equal(c, last, val)
				if err != nil {
					return err
				}
				if !eq {
					if e := yield(v); e != nil {
						return e
					}
				}
			} else {
				isLast = true
				if e := yield(v); e != nil {
					return e
				}
			}
			last = val
			return nil
		})
	}
}

// Group returns an iterable which contains iterables of equal values
func Group[V, C any](items Producer[V, C], equal func(C, V, V) (bool, error)) Producer[Producer[V, C], C] {
	return func(c C, yield Consumer[Producer[V, C]]) error {
		var list []V
		err := items(c, func(v V) error {
			if len(list) > 0 {
				eq, err := equal(c, list[len(list)-1], v)
				if err != nil {
					return err
				}
				if eq {
					list = append(list, v)
				} else {
					if e := yield(Slice[V, C](list)); e != nil {
						return e
					}
					list = []V{v}
				}
			} else {
				list = []V{v}
			}
			return nil
		})
		if err != nil {
			return err
		}
		if len(list) > 0 {
			return yield(Slice[V, C](list))
		}
		return nil
	}
}

// Combine maps two consecutive elements to a new element.
// The generated iterable has one element less than the original iterable.
func Combine[I, O, C any](in Producer[I, C], combine func(C, I, I) (O, error)) Producer[O, C] {
	return func(c C, yield Consumer[O]) error {
		isValue := false
		var last I
		return in(c, func(i I) error {
			if isValue {
				o, err := combine(c, last, i)
				if err != nil {
					return err
				}
				if e := yield(o); e != nil {
					return e
				}
			} else {
				isValue = true
			}
			last = i
			return nil
		})
	}
}

// Combine3 maps three consecutive elements to a new element.
// The generated iterable has two elements less than the original iterable.
func Combine3[I, O, C any](in Producer[I, C], combine func(C, I, I, I) (O, error)) Producer[O, C] {
	return func(c C, yield Consumer[O]) error {
		valuesPresent := 0
		var lastLast, last I
		return in(c, func(i I) error {
			switch valuesPresent {
			case 0:
				valuesPresent = 1
				lastLast = i
				return nil
			case 1:
				valuesPresent = 2
				last = i
				return nil
			default:
				o, err := combine(c, lastLast, last, i)
				if err != nil {
					return err
				}
				lastLast = last
				last = i
				return yield(o)
			}
		})
	}
}

// CombineN maps N consecutive elements to a new element.
// The generated iterable has (N-1) elements less than the original iterable.
func CombineN[I, O, C any](in Producer[I, C], n int, combine func(C, int, []I) (O, error)) Producer[O, C] {
	return func(c C, yield Consumer[O]) error {
		valuesPresent := 0
		pos := 0
		vals := make([]I, n, n)
		return in(c, func(i I) error {
			vals[pos] = i
			pos++
			if pos == n {
				pos = 0
			}
			if valuesPresent < n {
				valuesPresent++
			}
			if valuesPresent == n {
				o, err := combine(c, pos, vals)
				if err != nil {
					return err
				}
				return yield(o)
			}
			return nil
		})
	}

}

// Merge is used to merge two iterables.
// The less function determines which element to take first
// Makes sens only if the provided iterables are ordered.
func Merge[V, C any](ai, bi Producer[V, C], less func(C, V, V) (bool, error), cFac func() C) Producer[V, C] {
	return func(c C, yield Consumer[V]) error {
		aMain, aStop := ToChan(c, ai)
		bMain, bStop := ToChan(cFac(), bi)
		defer func() {
			close(aStop)
			close(bStop)
		}()
		isA := false
		var a container[V]
		isB := false
		var b container[V]
		for {
			if !isA {
				a, isA = <-aMain
				if !isA {
					if isB {
						if e := yield(b.val); e != nil {
							return e
						}
					}
					return copyValues(bMain, yield)
				}
				if a.err != nil {
					return a.err
				}
			}
			if !isB {
				b, isB = <-bMain
				if !isB {
					if isA {
						if e := yield(a.val); e != nil {
							return e
						}
					}
					return copyValues(aMain, yield)
				}
				if b.err != nil {
					return b.err
				}
			}
			lessA, err := less(c, a.val, b.val)
			if err != nil {
				return err
			}
			if lessA {
				if e := yield(a.val); e != nil {
					return e
				}
				isA = false
			} else {
				if e := yield(b.val); e != nil {
					return e
				}
				isB = false
			}
		}
	}
}

func copyValues[V any](main <-chan container[V], yield func(V) error) error {
	for c := range main {
		if c.err != nil {
			return c.err
		}
		if e := yield(c.val); e != nil {
			return e
		}
	}
	return nil
}

// MergeElements is used to merge two iterables.
// The combine function creates the result of combining the two elements.
// The iterables must have the same size.
func MergeElements[A, B, C, Co any](ai Producer[A, Co], bi Producer[B, Co], combine func(Co, A, B) (C, error), cFac func() Co) Producer[C, Co] {
	return func(c Co, yield Consumer[C]) error {
		aMain, aStop := ToChan(c, ai)
		bMain, bStop := ToChan(cFac(), bi)
		defer func() {
			close(aStop)
			close(bStop)
		}()
		for {
			a, aOk := <-aMain
			b, bOk := <-bMain

			if aOk && bOk {
				if a.err != nil {
					return a.err
				}
				if b.err != nil {
					return b.err
				}
				cb, err := combine(c, a.val, b.val)
				if err != nil {
					return err
				}
				if e := yield(cb); e != nil {
					return e
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
func Cross[A, B, C, Co any](a Producer[A, Co], b Producer[B, Co], cross func(Co, A, B) (C, error)) Producer[C, Co] {
	return func(co Co, yield Consumer[C]) error {
		return a(co, func(a A) error {
			return b(co, func(b B) error {
				c, err := cross(co, a, b)
				if err != nil {
					return err
				}
				return yield(c)
			})
		})
	}
}

// IirMap maps a value, the last value and the last created value to a new element.
// Can be used to implement iir filters like a low-pass. The last item is provided
// to allow handling of non-equidistant values.
func IirMap[I, R, C any](items Producer[I, C], initial func(c C, item I) (R, error), iir func(c C, item I, lastItem I, last R) (R, error)) Producer[R, C] {
	return func(c C, yield Consumer[R]) error {
		isLast := false
		var lastItem I
		var last R
		return items(c, func(i I) error {
			var err error
			if isLast {
				last, err = iir(c, i, lastItem, last)
				if err != nil {
					return err
				}
			} else {
				last, err = initial(c, i)
				if err != nil {
					return err
				}
				isLast = true
			}
			lastItem = i
			return yield(last)
		})
	}
}

// FirstN returns the first n elements of an Iterable
func FirstN[V, C any](items Producer[V, C], n int) Producer[V, C] {
	return func(c C, yield Consumer[V]) error {
		i := 0
		err := items(c, func(v V) error {
			if i < n {
				i++
				return yield(v)
			} else {
				return SBC
			}
		})
		if err != SBC {
			return err
		} else {
			return nil
		}
	}
}

// Skip skips the first elements.
// The number of elements to skip is given in skip.
func Skip[V, C any](items Producer[V, C], n int) Producer[V, C] {
	return func(c C, yield Consumer[V]) error {
		i := 0
		return items(c, func(v V) error {
			if i < n {
				i++
				return nil
			} else {
				return yield(v)
			}
		})
	}
}

// Thinning returns an iterable which skips a certain amount of elements
// from the parent iterable. If skip is set to 1, every second element is skipped.
// The first and the last item are always returned.
func Thinning[V, C any](items Producer[V, C], n int) Producer[V, C] {
	return func(c C, yield Consumer[V]) error {
		i := 0
		var skipped V
		err := items(c, func(v V) error {
			if i == 0 {
				i = n
				return yield(v)
			} else {
				skipped = v
				i--
				return nil
			}
		})
		if err == nil {
			if i < n {
				return yield(skipped)
			} else {
				return nil
			}
		}
		return err
	}
}

// Reduce reduces the items of the iterable to a single value by calling the reduce function.
func Reduce[V, C any](c C, it Producer[V, C], reduceFunc func(C, V, V) (V, error)) (V, error) {
	var sum V
	isValue := false
	err := it(c, func(v V) error {
		var err error
		if isValue {
			sum, err = reduceFunc(c, sum, v)
			if err != nil {
				return err
			}
		} else {
			sum = v
			isValue = true
		}
		return nil
	})
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
func ReduceParallel[V, C any](it Producer[V, C], reduceFuncFac func() func(V, V) (V, error), c C) (V, error) {
	valChan, done := ToChan(c, it)
	defer close(done)

	result := make(chan container[V])
	var wg sync.WaitGroup
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		reduceFunc := reduceFuncFac()
		go func() {
			defer wg.Done()
			var sum V
			isSum := false
			for v := range valChan {
				if v.err != nil {
					select {
					case result <- container[V]{err: v.err}:
					case <-done:
					}
					return
				}
				if isSum {
					var err error
					sum, err = reduceFunc(sum, v.val)
					if err != nil {
						select {
						case result <- container[V]{err: err}:
						case <-done:
						}
						return
					}
				} else {
					sum = v.val
					isSum = true
				}
			}
			if isSum {
				select {
				case result <- container[V]{val: sum}:
				case <-done:
				}
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
	for r := range result {
		if r.err != nil {
			return sum, r.err
		}
		if isSum {
			var err error
			sum, err = reduceFunc(sum, r.val)
			if err != nil {
				return sum, err
			}
		} else {
			sum = r.val
			isSum = true
		}
	}
	if !isSum {
		return sum, errors.New("reduce on empty iterable")
	}
	return sum, nil
}

// MapReduce combines a map and reduce step in one go.
// Avoids generating intermediate map results.
// Instead of map(n->n^2).reduce((a,b)->a+b) one
// can write  mapReduce(0, (s,n)->s+n^2)
// Useful if map and reduce are both low cost operations.
func MapReduce[S, V, C any](c C, it Producer[V, C], initial S, reduceFunc func(C, S, V) (S, error)) (S, error) {
	err := it(c, func(v V) error {
		var err error
		initial, err = reduceFunc(c, initial, v)
		return err
	})
	return initial, err
}

// First returns the first item of the iterator
// The returned bool is false if there is no item because iterator is empty.
func First[V, C any](c C, in Producer[V, C]) (V, bool) {
	var first V
	isFirst := false
	in(c, func(v V) error {
		first = v
		isFirst = true
		return SBC
	})
	return first, isFirst
}

type itHolder[V, C any] struct {
	num   int
	c     chan V
	err   chan error
	done  chan struct{}
	ended bool
}

func (ih *itHolder[V, C]) createIt() Producer[V, C] {
	used := false
	return func(c C, yield Consumer[V]) error {
		if used {
			return errors.New("copied iterator a can only be used once")
		}
		used = true
		for v := range ih.c {
			if e := yield(v); e != nil {
				select {
				case ih.err <- e:
				case <-ih.done:
				}
				return e
			}
		}
		return nil
	}
}

// CopyProducer copies the initial producer into num identical producers.
// The returned producers can be used in parallel to process the input in.
//
// The return values are:
//
//	The prodList contains the copied producers.
//	The run function needs to be called with the consumer context to start reading the given producer.
//	The doneFunc needs to be called once from every created go routine which calls one of the created producers. If no error has occurred, the error is nil.
func CopyProducer[V, C any](in Producer[V, C], num int) (prodList []Producer[V, C], run func(C) error, doneFunc func(error)) {
	innerDone := make(chan struct{})

	var ihl []*itHolder[V, C]
	for i := 0; i < num; i++ {
		ih := &itHolder[V, C]{
			num:  i,
			c:    make(chan V),
			err:  make(chan error),
			done: innerDone,
		}
		ihl = append(ihl, ih)
		prodList = append(prodList, ih.createIt())
	}

	var mainErrMutex sync.Mutex
	mainErrSet := make(chan struct{})
	mainErrSetClosed := false
	var mainErr error = nil

	wg := sync.WaitGroup{}
	wg.Add(num)

	doneFunc = func(e error) {
		mainErrMutex.Lock()
		defer mainErrMutex.Unlock()

		if e != nil && e != SBC {
			if !mainErrSetClosed {
				mainErr = e
				close(mainErrSet)
				mainErrSetClosed = true
			}
		}

		wg.Done()
	}

	run = func(c C) error {
		defer close(innerDone)

		running := num
		err := in(c, func(v V) error {
			for _, ih := range ihl {
				if !ih.ended {
					select {
					case ih.c <- v:
					case <-mainErrSet:
						return mainErr
					case err := <-ih.err:
						if err == SBC {
							ih.ended = true
							running--
							if running == 0 {
								return SBC
							}
						} else {
							mainErrMutex.Lock()
							defer mainErrMutex.Unlock()

							if !mainErrSetClosed {
								mainErr = err
								close(mainErrSet)
								mainErrSetClosed = true
							}
							return err
						}
					case <-time.After(5 * time.Second):
						return errors.New("timeout; Are all copied iterators used?")
					}
				}
			}
			return nil
		})
		for _, ih := range ihl {
			close(ih.c)
		}

		wg.Wait()

		if err == nil && mainErr != nil {
			return mainErr
		}

		if err == SBC {
			return nil
		}
		return err
	}

	return
}
