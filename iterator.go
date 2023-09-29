package iterator

import (
	"log"
	"runtime"
	"sync"
	"time"
)

type Iterator[V any] func(func(V) bool) bool

type Iterable[V any] func() Iterator[V]

func Empty[V any]() Iterable[V] {
	return func() Iterator[V] {
		return func(yield func(V) bool) bool {
			return true
		}
	}
}

func Single[V any](v V) Iterable[V] {
	return func() Iterator[V] {
		return func(yield func(V) bool) bool {
			return yield(v)
		}
	}
}

// Slice create an Iterable from a slice
func Slice[V any](items []V) Iterable[V] {
	return func() Iterator[V] {
		return func(yield func(V) bool) bool {
			for _, i := range items {
				if !yield(i) {
					return false
				}
			}
			return true
		}
	}
}

// Append appends to Iterables
func Append[V any](iterables ...Iterable[V]) Iterable[V] {
	return func() Iterator[V] {
		return func(yield func(V) bool) bool {
			for _, it := range iterables {
				if !it()(yield) {
					return false
				}
			}
			return true
		}
	}
}

func Generate[V any](n int, gen func(i int) V) Iterable[V] {
	return func() Iterator[V] {
		return func(yield func(V) bool) bool {
			for i := 0; i < n; i++ {
				if !yield(gen(i)) {
					return false
				}
			}
			return true
		}
	}
}

// ToChan writes elements to a channel
func ToChan[V any](it Iterator[V]) (<-chan V, chan struct{}, chan any) {
	c := make(chan V)
	stop := make(chan struct{})
	panicChan := make(chan any)
	innerChannel := c
	go func() {
		defer func() {
			rec := recover()
			if rec != nil {
				panicChan <- rec
			}
			close(innerChannel)
			innerChannel = nil
		}()
		it(func(v V) bool {
			select {
			case innerChannel <- v:
				return true
			case <-stop:
				return false
			}
		})
	}()
	return c, stop, panicChan
}

// FromChan reads items from a channel
func FromChan[V any](c <-chan V, stop chan<- struct{}, panicReadeAndFire <-chan any) Iterator[V] {
	return func(yield func(V) bool) bool {
		defer close(stop)
		for {
			select {
			case v, ok := <-c:
				if ok {
					if !yield(v) {
						return false
					}
				} else {
					return true
				}
			case p := <-panicReadeAndFire:
				panic(p)
			}
		}
	}
}

// ToSlice reads all items from the Iterator and stores them in a slice.
func ToSlice[V any](it Iterator[V]) []V {
	var sl []V
	it(func(v V) bool {
		sl = append(sl, v)
		return true
	})
	return sl
}

// Equals checks if the two Iterators are equal.
func Equals[V any](i1, i2 Iterator[V], equals func(V, V) bool) bool {
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
					return true
				} else if ok1 != ok2 {
					return false
				} else if ok1 && ok2 {
					if !equals(item1, item2) {
						return false
					}
					c1 = cMain1
					c2 = cMain2
				}
			}
		case item2, ok2 = <-c2:
			c2 = nil
			if c1 == nil {
				if !ok1 && !ok2 {
					return true
				} else if ok1 != ok2 {
					return false
				} else if ok1 && ok2 {
					if !equals(item1, item2) {
						return false
					}
					c1 = cMain1
					c2 = cMain2
				}
			}
		case p := <-p1:
			panic(p)
		case p := <-p2:
			panic(p)
		}
	}
}

// Map maps the elements to new element created by the given mapFunc function
func Map[I, O any](in Iterable[I], mapFunc func(int, I) O) Iterable[O] {
	return func() Iterator[O] {
		return mapIterator[I, O](in(), mapFunc, 0)
	}
}

func mapIterator[I, O any](iter Iterator[I], mapFunc func(int, I) O, num int) Iterator[O] {
	return func(yield func(O) bool) bool {
		return iter(func(item I) bool {
			if !yield(mapFunc(num, item)) {
				return false
			}
			num++
			return true
		})
	}
}

type container[V any] struct {
	num int
	val V
}

func splitWork[I, O any](jobs <-chan container[I], closed <-chan struct{}, panicChan chan<- any, mapFuncFac func() func(n int, i I) O) <-chan container[O] {
	result := make(chan container[O])
	wg := sync.WaitGroup{}
	for n := 0; n < runtime.NumCPU(); n++ {
		wg.Add(1)
		mapFunc := mapFuncFac()
		go func() {
			defer func() {
				rec := recover()
				if rec != nil {
					panicChan <- rec
				}
				wg.Done()
			}()

			for ci := range jobs {
				o := mapFunc(ci.num, ci.val)
				select {
				case result <- container[O]{ci.num, o}:
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

func collectResults[O any](result <-chan container[O], closed chan<- struct{}, panicReadAndFire <-chan any, ack chan<- struct{}, yield func(o O) bool, num int) bool {
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
						return false
					}
					num++
					if !sendAvail() {
						return false
					}
				} else {
					store[co.num] = co.val
				}
			} else {
				return sendAvail()
			}
		case p := <-panicReadAndFire:
			panic(p)
		}
	}
}

// MapParallel behaves the same as the Map Iterable.
// The mapping is distributed over all available cores.
func MapParallel[I, O any](in Iterable[I], mapFuncFac func() func(int, I) O) Iterable[O] {
	return func() Iterator[O] {
		iter := in()
		return func(yield func(O) bool) bool {
			jobs, closed, panicChan := ToChan(mapIterator(iter, func(n int, item I) container[I] {
				return container[I]{n, item}
			}, 0))

			result := splitWork(jobs, closed, panicChan, mapFuncFac)

			return collectResults(result, closed, panicChan, nil, yield, 0)
		}
	}
}

// MapAuto behaves the same as the Map Iterable.
// It is measured how long the map function takes. If the map function requires so much
// computing time that it is worth distributing it over several cores, the map function
// is distributed over all available cores (reported by runtime.NumCPU()).
func MapAuto[I, O any](in Iterable[I], mapFuncFac func() func(int, I) O) Iterable[O] {
	if runtime.NumCPU() == 1 {
		return Map(in, mapFuncFac())
	}

	return func() Iterator[O] {
		iter := in()
		return func(yield func(O) bool) bool {
			next := measure[I, O](yield, mapFuncFac)
			var cleanUp func()
			ok := iter(func(item I) bool {
				next, cleanUp = next(item)
				return next != nil
			})
			if cleanUp != nil {
				cleanUp()
			}
			return ok
		}
	}
}

type nextType[V any] func(v V) (nextType[V], func())

const (
	itemProcessingTimeMicroSec = 200
	itemsToMeasure             = 11
)

func measure[I, O any](yield func(O) bool, mapFuncFac func() func(int, I) O) nextType[I] {
	var meas nextType[I]
	var dur time.Duration
	var count int
	mapFunc := mapFuncFac()
	meas = func(v I) (nextType[I], func()) {
		start := time.Now()
		o := mapFunc(count, v)
		if count > 0 {
			// ignore first call to avoid errors due to setup costs
			dur += time.Now().Sub(start)
		}
		if !yield(o) {
			return nil, nil
		}
		count++
		if count < itemsToMeasure {
			return meas, nil
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

func serial[I, O any](yield func(O) bool, mapFunc func(int, I) O, count int) (nextType[I], func()) {
	var ser nextType[I]
	ser = func(v I) (nextType[I], func()) {
		if !yield(mapFunc(count, v)) {
			return nil, nil
		} else {
			count++
			return ser, nil
		}
	}
	return ser, nil
}

func parallel[I, O any](yield func(O) bool, mapFuncFac func() func(int, I) O, num int) (nextType[I], func()) {
	jobs := make(chan container[I])
	stop := make(chan struct{})
	panicChan := make(chan any)
	ack := make(chan struct{})

	i := num
	cleanUp := func() {
		close(jobs)
		<-ack
		log.Printf("items mapped in parallel: %d", i-itemsToMeasure)
	}

	var par nextType[I]
	par = func(v I) (nextType[I], func()) {
		select {
		case jobs <- container[I]{i, v}:
			i++
			return par, cleanUp
		case <-stop:
			return nil, cleanUp
		case p := <-panicChan:
			panic(p)
		}
	}

	result := splitWork(jobs, stop, panicChan, mapFuncFac)

	go func() {
		defer func() {
			rec := recover()
			if rec != nil {
				panicChan <- rec
			}
		}()
		collectResults(result, stop, nil, ack, yield, num)
	}()

	return par, cleanUp
}

// Filter filters the given Iterable by the given accept function
func Filter[V any](in Iterable[V], accept func(V) bool) Iterable[V] {
	return func() Iterator[V] {
		return func(yield func(V) bool) bool {
			return in()(func(v V) bool {
				if accept(v) {
					if !yield(v) {
						return false
					}
				}
				return true
			})
		}
	}
}

// FilterAuto behaves the same as the Filter Iterable.
// It is measured how long the accept function takes. If the accept function requires so much
// computing time that it is worth distributing it over several cores, the filtering
// is distributed over several cores.
func FilterAuto[V any](in Iterable[V], acceptFac func() func(V) bool) Iterable[V] {
	if runtime.NumCPU() == 1 {
		return Filter(in, acceptFac())
	}

	return func() Iterator[V] {
		return func(yield func(V) bool) bool {
			return MapAuto(in, func() func(i int, val V) filterContainer[V] {
				accept := acceptFac()
				return func(i int, val V) filterContainer[V] {
					return filterContainer[V]{val, accept(val)}
				}
			})()(func(v filterContainer[V]) bool {
				if v.accept {
					return yield(v.val)
				} else {
					return true
				}
			})
		}
	}
}

type filterContainer[V any] struct {
	val    V
	accept bool
}

// FilterParallel behaves the same as the Filter Iterable.
// The filtering is distributed over all available cores.
func FilterParallel[V any](in Iterable[V], acceptFac func() func(V) bool) Iterable[V] {
	return func() Iterator[V] {
		return func(yield func(V) bool) bool {
			return MapParallel[V, filterContainer[V]](in, func() func(i int, val V) filterContainer[V] {
				accept := acceptFac()
				return func(i int, val V) filterContainer[V] {
					return filterContainer[V]{val, accept(val)}
				}
			})()(func(v filterContainer[V]) bool {
				if v.accept {
					return yield(v.val)
				} else {
					return true
				}
			})
		}
	}
}

// Compact returns an iterable which contains no consecutive duplicates.
func Compact[V any](items Iterable[V], equal func(V, V) bool) Iterable[V] {
	return func() Iterator[V] {
		return func(yield func(V) bool) bool {
			isLast := false
			var last V
			return items()(func(v V) bool {
				if isLast {
					if !equal(last, v) {
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
		}
	}
}

// Group returns an iterable which contains iterables of equal values
func Group[V any](items Iterable[V], equal func(V, V) bool) Iterable[Iterable[V]] {
	return func() Iterator[Iterable[V]] {
		return func(yield func(Iterable[V]) bool) bool {
			var list []V
			ok := items()(func(v V) bool {
				if len(list) > 0 {
					if equal(list[len(list)-1], v) {
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
			if ok && len(list) > 0 {
				return yield(Slice(list))
			}
			return ok
		}
	}
}

// Combine maps two consecutive elements to a new element.
// The generated iterable has one element less than the original iterable.
func Combine[I, O any](in Iterable[I], combine func(I, I) O) Iterable[O] {
	return func() Iterator[O] {
		return func(yield func(O) bool) bool {
			isValue := false
			var last I
			return in()(func(i I) bool {
				if isValue {
					o := combine(last, i)
					if !yield(o) {
						return false
					}
				} else {
					isValue = true
				}
				last = i
				return true
			})
		}
	}
}

// Combine3 maps three consecutive elements to a new element.
// The generated iterable has two elements less than the original iterable.
func Combine3[I, O any](in Iterable[I], combine func(I, I, I) O) Iterable[O] {
	return func() Iterator[O] {
		return func(yield func(O) bool) bool {
			valuesPresent := 0
			var lastLast, last I
			return in()(func(i I) bool {
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
					o := combine(lastLast, last, i)
					lastLast = last
					last = i
					return yield(o)
				}
			})
		}
	}
}

// CombineN maps N consecutive elements to a new element.
// The generated iterable has (N-1) elements less than the original iterable.
func CombineN[I, O any](in Iterable[I], n int, combine func(i0 int, v []I) O) Iterable[O] {
	return func() Iterator[O] {
		return func(yield func(O) bool) bool {
			valuesPresent := 0
			pos := 0
			vals := make([]I, n, n)
			return in()(func(i I) bool {
				vals[pos] = i
				pos++
				if pos == n {
					pos = 0
				}
				if valuesPresent < n {
					valuesPresent++
				}
				if valuesPresent == n {
					o := combine(pos, vals)
					return yield(o)
				}
				return true
			})
		}
	}
}

// Merge is used to merge two iterables.
// The less function determines which element to take first
// Makes sens only if the provided iterables are ordered.
func Merge[V any](ai, bi Iterable[V], less func(V, V) bool) Iterable[V] {
	return func() Iterator[V] {
		return func(yield func(V) bool) bool {
			aMain, aStop, aPanic := ToChan(ai())
			bMain, bStop, bPanic := ToChan(bi())
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
							if less(a, b) {
								if !yield(a) {
									return false
								}
								ar = aMain
							} else {
								if !yield(b) {
									return false
								}
								br = bMain
							}
						}
					} else {
						if br == nil {
							if !yield(b) {
								return false
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
							if less(a, b) {
								if !yield(a) {
									return false
								}
								ar = aMain
							} else {
								if !yield(b) {
									return false
								}
								br = bMain
							}
						}
					} else {
						if ar == nil {
							if !yield(a) {
								return false
							}
						} else {
							ar = nil
						}
						remainder = aMain
					}
				case r, ok := <-remainder:
					if ok {
						if !yield(r) {
							return false
						}
					} else {
						return true
					}
				case p := <-aPanic:
					panic(p)
				case p := <-bPanic:
					panic(p)
				}
			}
		}
	}
}

// MergeElements is used to merge two iterables.
// The combine function creates the result of combining the two elements.
// The iterables must have the same size.
func MergeElements[A, B, C any](ai Iterable[A], bi Iterable[B], combine func(A, B) C) Iterable[C] {
	return func() Iterator[C] {
		return func(yield func(C) bool) bool {
			aMain, aStop, aPanic := ToChan(ai())
			bMain, bStop, bPanic := ToChan(bi())
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
					panic(p)
				case p := <-bPanic:
					panic(p)
				}
				var b B
				var bOk bool
				select {
				case b, bOk = <-bMain:
				case p := <-aPanic:
					panic(p)
				case p := <-bPanic:
					panic(p)
				}

				if aOk && bOk {
					if !yield(combine(a, b)) {
						return false
					}
				} else if aOk || bOk {
					panic("iterables in mergeElements dont have the same size")
				} else {
					return true
				}
			}
		}
	}
}

// Cross is used to cross two iterables.
func Cross[A, B, C any](a Iterable[A], b Iterable[B], cross func(A, B) C) Iterable[C] {
	return func() Iterator[C] {
		return func(yield func(C) bool) bool {
			return a()(func(a A) bool {
				return b()(func(b B) bool {
					c := cross(a, b)
					if !yield(c) {
						return false
					}
					return true
				})
			})
		}
	}
}

// IirMap maps a value, the last value and the last created value to a new element.
// Can be used to implement iir filters like a low-pass. The last item is provided
// to allow handling of non-equidistant values.
func IirMap[I any, R any](items Iterable[I], initial func(item I) R, iir func(item I, lastItem I, last R) R) Iterable[R] {
	return func() Iterator[R] {
		return func(yield func(R) bool) bool {
			isLast := false
			var lastItem I
			var last R
			return items()(func(i I) bool {
				if isLast {
					last = iir(i, lastItem, last)
				} else {
					last = initial(i)
					isLast = true
				}
				lastItem = i
				return yield(last)
			})
		}
	}
}

// FirstN returns the first element of an Iterable
func FirstN[V any](items Iterable[V], n int) Iterable[V] {
	return func() Iterator[V] {
		return func(yield func(V) bool) bool {
			i := 0
			return items()(func(v V) bool {
				if i < n {
					i++
					return yield(v)
				} else {
					return false
				}
			})
		}
	}
}

// Skip skips the first elements.
// The number of elements to skip is given in skip.
func Skip[V any](items Iterable[V], n int) Iterable[V] {
	return func() Iterator[V] {
		return func(yield func(V) bool) bool {
			i := 0
			return items()(func(v V) bool {
				if i < n {
					i++
					return true
				} else {
					return yield(v)
				}
			})
		}
	}
}

// Thinning returns an iterable which skips a certain amount of elements
// from the parent iterable. If skip is set to 1, every second element is skipped.
// The first and the last item are always returned.
func Thinning[V any](items Iterable[V], n int) Iterable[V] {
	return func() Iterator[V] {
		return func(yield func(V) bool) bool {
			i := 0
			var skipped V
			if items()(func(v V) bool {
				if i == 0 {
					i = n
					return yield(v)
				} else {
					skipped = v
					i--
					return true
				}
			}) {
				if i < n {
					return yield(skipped)
				} else {
					return true
				}
			}
			return false
		}
	}
}

// Reduce reduces the items of the iterable to a single value by calling the reduce function.
func Reduce[V any](it Iterable[V], reduceFunc func(V, V) V) (V, bool) {
	var sum V
	isValue := false
	it()(func(v V) bool {
		if isValue {
			sum = reduceFunc(sum, v)
		} else {
			sum = v
			isValue = true
		}
		return true
	})
	return sum, isValue
}

// ReduceParallel usage makes sens only if reduce operation is costly.
// In cases like a+b, a*b or "if a>b then a else b" it makes no sense at all
// because the synchronization is more expensive than the operation itself.
// It should always be possible to do the heavy lifting in a map operation and
// make the reduce operation low cost.
func ReduceParallel[V any](it Iterable[V], reduceFuncFac func() func(V, V) V) (V, bool) {
	valChan, stop, wasPanic := ToChan(it())

	result := make(chan V)
	var wg sync.WaitGroup
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		reduceFunc := reduceFuncFac()
		go func() {
			defer func() {
				rec := recover()
				if rec != nil {
					wasPanic <- rec
				}
				wg.Done()
			}()
			var sum V
			isSum := false
			for v := range valChan {
				if isSum {
					sum = reduceFunc(sum, v)
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
					sum = reduceFunc(sum, r)
				} else {
					sum = r
					isSum = true
				}
			} else {
				return sum, isSum
			}
		case p := <-wasPanic:
			close(stop)
			panic(p)
		}
	}
}

// MapReduce combines a map and reduce step in one go.
// Avoids generating intermediate map results.
// Instead of map(n->n^2).reduce((a,b)->a+b) one
// can write  mapReduce(0, (s,n)->s+n^2)
// Useful if map and reduce are both low cost operations.
func MapReduce[S, V any](it Iterable[V], initial S, reduceFunc func(S, V) S) S {
	it()(func(v V) bool {
		initial = reduceFunc(initial, v)
		return true
	})
	return initial
}

// First returns the first item of the iterator
// The returned bool is false if there is no item because iterator is empty.
func First[V any](in Iterable[V]) (V, bool) {
	var first V
	isFirst := false
	in()(func(v V) bool {
		first = v
		isFirst = true
		return false
	})
	return first, isFirst
}

// Peek takes an iterator, gives a new iterator and the first item of the given iterator.
// The returned iterator still iterates all items including the first one.
// Expensive because all items have to go through a channel.
// Use only if the creation of the original iterator is even more expensive.
func Peek[V any](it Iterator[V]) (Iterator[V], V, bool) {
	c, stop, panicChan := ToChan[V](it)
	var first V
	var ok bool
	select {
	case first, ok = <-c:
	case p := <-panicChan:
		panic(p)
	}

	var iter Iterator[V]
	if ok {
		iter = func(yield func(V) bool) bool {
			if !yield(first) {
				return false
			}
			return FromChan[V](c, stop, panicChan)(yield)
		}
	} else {
		iter = Empty[V]()()
	}

	return iter, first, ok
}
