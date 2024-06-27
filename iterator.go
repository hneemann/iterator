package iterator

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var SBC = errors.New("stopped by consumer")

type Consumer[V any] func(V) error

type Iterator[V any] func(Consumer[V]) error

type Iterable[V, C any] func(c C) Iterator[V]

func Empty[V, C any]() Iterable[V, C] {
	return func(c C) Iterator[V] {
		return func(yield Consumer[V]) error {
			return nil
		}
	}
}

func Single[V, C any](v V) Iterable[V, C] {
	return func(c C) Iterator[V] {
		return func(yield Consumer[V]) error {
			return yield(v)
		}
	}
}

// Slice create an Iterable from a slice
func Slice[V, C any](items []V) Iterable[V, C] {
	return func(c C) Iterator[V] {
		return func(yield Consumer[V]) error {
			for _, i := range items {
				if e := yield(i); e != nil {
					return e
				}
			}
			return nil
		}
	}
}

// Append appends to Iterables
func Append[V, C any](iterables ...Iterable[V, C]) Iterable[V, C] {
	return func(c C) Iterator[V] {
		return func(yield Consumer[V]) error {
			for _, it := range iterables {
				if err := it(c)(yield); err != nil {
					return err
				}
			}
			return nil
		}
	}
}

func Generate[V, C any](n int, gen func(i int) (V, error)) Iterable[V, C] {
	return func(c C) Iterator[V] {
		return func(yield Consumer[V]) error {
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
}

func toError(rec any) error {
	if err, ok := rec.(error); ok {
		return err
	} else {
		return fmt.Errorf("%v", rec)
	}
}

type container[V any] struct {
	num int
	val V
	err error
}

// ToChan writes elements to a channel
func ToChan[V any](it Iterator[V]) (<-chan container[V], chan struct{}) {
	c := make(chan container[V])
	done := make(chan struct{})
	go func() {
		i := 0
		err := it(func(v V) error {
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

// ToSlice reads all items from the Iterator and stores them in a slice.
func ToSlice[V any](it Iterator[V]) ([]V, error) {
	var sl []V
	err := it(func(v V) error {
		sl = append(sl, v)
		return nil
	})
	return sl, err
}

// Equals checks if the two Iterators are equal.
func Equals[V any](i1, i2 Iterator[V], equals func(V, V) (bool, error)) (bool, error) {
	ch1, done1 := ToChan(i1)
	ch2, done2 := ToChan(i2)
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
func Map[I, O, C any](in Iterable[I, C], mapFunc func(int, I) (O, error)) Iterable[O, C] {
	return func(c C) Iterator[O] {
		return func(yield Consumer[O]) error {
			num := 0
			return in(c)(func(item I) error {
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
}

func splitWork[I, O any](jobs <-chan container[I], done <-chan struct{}, mapFuncFac func() func(n int, i I) (O, error)) <-chan container[O] {
	result := make(chan container[O])
	wg := sync.WaitGroup{}
	for n := 0; n < runtime.NumCPU(); n++ {
		wg.Add(1)
		mapFunc := mapFuncFac()
		go func() {
			defer wg.Done()
			for ci := range jobs {
				if ci.err != nil {
					select {
					case result <- container[O]{num: ci.num, err: ci.err}:
					case <-done:
					}
					return
				}
				o, err := mapFunc(ci.num, ci.val)
				select {
				case result <- container[O]{ci.num, o, err}:
				case <-done:
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

func collectResults[O any](result <-chan container[O], done chan<- struct{}, yield func(o O) error, num int) error {
	defer close(done)

	store := map[int]O{}

	sendAvail := func() error {
		for {
			if o, ok := store[num]; ok {
				if e := yield(o); e != nil {
					return e
				}
				delete(store, num)
				num++
			} else {
				return nil
			}
		}
	}

	for co := range result {
		if co.err != nil {
			return co.err
		}
		if co.num == num {
			if e := yield(co.val); e != nil {
				return e
			}
			num++
			if e := sendAvail(); e != nil {
				return e
			}
		} else {
			store[co.num] = co.val
		}
	}
	return sendAvail()
}

// MapParallel behaves the same as the Map Iterable.
// The mapping is distributed over all available cores.
func MapParallel[I, O, C any](in Iterable[I, C], mapFuncFac func() func(int, I) (O, error)) Iterable[O, C] {
	if runtime.NumCPU() == 1 {
		return Map(in, mapFuncFac())
	}
	return func(c C) Iterator[O] {
		iter := in(c)
		return func(yield Consumer[O]) error {
			jobs, done := ToChan(iter)
			result := splitWork(jobs, done, mapFuncFac)
			return collectResults(result, done, yield, 0)
		}
	}
}

// MapAuto behaves the same as the Map Iterable.
// It is measured how long the map function takes. If the map function requires so much
// computing time that it is worth distributing it over several cores, the map function
// is distributed over all available cores (reported by runtime.NumCPU()).
func MapAuto[I, O, C any](in Iterable[I, C], mapFuncFac func() func(int, I) (O, error)) Iterable[O, C] {
	return MapParallel(in, mapFuncFac)
}

const (
	itemProcessingTimeMicroSec = 200
	itemsToMeasure             = 11
)

// Filter filters the given Iterable by the given accept function
func Filter[V, C any](in Iterable[V, C], accept func(V) (bool, error)) Iterable[V, C] {
	return func(c C) Iterator[V] {
		return func(yield Consumer[V]) error {
			return in(c)(func(v V) error {
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
}

// FilterAuto behaves the same as the Filter Iterable.
// It is measured how long the accept function takes. If the accept function requires so much
// computing time that it is worth distributing it over several cores, the filtering
// is distributed over several cores.
func FilterAuto[V, C any](in Iterable[V, C], acceptFac func() func(V) (bool, error)) Iterable[V, C] {
	if runtime.NumCPU() == 1 {
		return Filter(in, acceptFac())
	}

	return func(c C) Iterator[V] {
		return func(yield Consumer[V]) error {
			return MapAuto(in, func() func(i int, val V) (filterContainer[V], error) {
				accept := acceptFac()
				return func(i int, val V) (filterContainer[V], error) {
					b, err := accept(val)
					if err != nil {
						return filterContainer[V]{}, err
					}
					return filterContainer[V]{val, b}, nil
				}
			})(c)(func(v filterContainer[V]) error {
				if v.accept {
					return yield(v.val)
				} else {
					return nil
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
func FilterParallel[V, C any](in Iterable[V, C], acceptFac func() func(V) (bool, error)) Iterable[V, C] {
	return func(c C) Iterator[V] {
		return func(yield Consumer[V]) error {
			return MapParallel[V, filterContainer[V]](in, func() func(i int, val V) (filterContainer[V], error) {
				accept := acceptFac()
				return func(i int, val V) (filterContainer[V], error) {
					b, err := accept(val)
					if err != nil {
						return filterContainer[V]{}, err
					}
					return filterContainer[V]{val, b}, nil
				}
			})(c)(func(v filterContainer[V]) error {
				if v.accept {
					return yield(v.val)
				} else {
					return nil
				}
			})
		}
	}
}

// Compact returns an iterable which contains no consecutive duplicates.
func Compact[V, M, C any](items Iterable[V, C], convert func(C, V) (M, error), equal func(C, M, M) (bool, error)) Iterable[V, C] {
	return func(c C) Iterator[V] {
		return func(yield Consumer[V]) error {
			isLast := false
			var last M
			return items(c)(func(v V) error {
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
}

// Group returns an iterable which contains iterables of equal values
func Group[V, C any](items Iterable[V, C], equal func(C, V, V) (bool, error)) Iterable[Iterable[V, C], C] {
	return func(c C) Iterator[Iterable[V, C]] {
		return func(yield Consumer[Iterable[V, C]]) error {
			var list []V
			err := items(c)(func(v V) error {
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
}

// Combine maps two consecutive elements to a new element.
// The generated iterable has one element less than the original iterable.
func Combine[I, O, C any](in Iterable[I, C], combine func(C, I, I) (O, error)) Iterable[O, C] {
	return func(c C) Iterator[O] {
		return func(yield Consumer[O]) error {
			isValue := false
			var last I
			return in(c)(func(i I) error {
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
}

// Combine3 maps three consecutive elements to a new element.
// The generated iterable has two elements less than the original iterable.
func Combine3[I, O, C any](in Iterable[I, C], combine func(C, I, I, I) (O, error)) Iterable[O, C] {
	return func(c C) Iterator[O] {
		return func(yield Consumer[O]) error {
			valuesPresent := 0
			var lastLast, last I
			return in(c)(func(i I) error {
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
}

// CombineN maps N consecutive elements to a new element.
// The generated iterable has (N-1) elements less than the original iterable.
func CombineN[I, O, C any](in Iterable[I, C], n int, combine func(C, int, []I) (O, error)) Iterable[O, C] {
	return func(c C) Iterator[O] {
		return func(yield Consumer[O]) error {
			valuesPresent := 0
			pos := 0
			vals := make([]I, n, n)
			return in(c)(func(i I) error {
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
}

// Merge is used to merge two iterables.
// The less function determines which element to take first
// Makes sens only if the provided iterables are ordered.
func Merge[V, C any](ai, bi Iterable[V, C], less func(C, V, V) (bool, error), cFac func() C) Iterable[V, C] {
	return func(c C) Iterator[V] {
		return func(yield Consumer[V]) error {
			aMain, aStop := ToChan(ai(cFac()))
			bMain, bStop := ToChan(bi(cFac()))
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
func MergeElements[A, B, C, Co any](ai Iterable[A, Co], bi Iterable[B, Co], combine func(Co, A, B) (C, error), cFac func() Co) Iterable[C, Co] {
	return func(c Co) Iterator[C] {
		return func(yield Consumer[C]) error {
			aMain, aStop := ToChan(ai(cFac()))
			bMain, bStop := ToChan(bi(cFac()))
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
}

// Cross is used to cross two iterables.
func Cross[A, B, C, Co any](a Iterable[A, Co], b Iterable[B, Co], cross func(Co, A, B) (C, error)) Iterable[C, Co] {
	return func(co Co) Iterator[C] {
		return func(yield Consumer[C]) error {
			return a(co)(func(a A) error {
				return b(co)(func(b B) error {
					c, err := cross(co, a, b)
					if err != nil {
						return err
					}
					return yield(c)
				})
			})
		}
	}
}

// IirMap maps a value, the last value and the last created value to a new element.
// Can be used to implement iir filters like a low-pass. The last item is provided
// to allow handling of non-equidistant values.
func IirMap[I, R, C any](items Iterable[I, C], initial func(c C, item I) (R, error), iir func(c C, item I, lastItem I, last R) (R, error)) Iterable[R, C] {
	return func(c C) Iterator[R] {
		return func(yield Consumer[R]) error {
			isLast := false
			var lastItem I
			var last R
			return items(c)(func(i I) error {
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
}

// FirstN returns the first n elements of an Iterable
func FirstN[V, C any](items Iterable[V, C], n int) Iterable[V, C] {
	return func(c C) Iterator[V] {
		return func(yield Consumer[V]) error {
			i := 0
			err := items(c)(func(v V) error {
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
}

// Skip skips the first elements.
// The number of elements to skip is given in skip.
func Skip[V, C any](items Iterable[V, C], n int) Iterable[V, C] {
	return func(c C) Iterator[V] {
		return func(yield Consumer[V]) error {
			i := 0
			return items(c)(func(v V) error {
				if i < n {
					i++
					return nil
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
func Thinning[V, C any](items Iterable[V, C], n int) Iterable[V, C] {
	return func(c C) Iterator[V] {
		return func(yield Consumer[V]) error {
			i := 0
			var skipped V
			err := items(c)(func(v V) error {
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
}

// Reduce reduces the items of the iterable to a single value by calling the reduce function.
func Reduce[V, C any](c C, it Iterable[V, C], reduceFunc func(C, V, V) (V, error)) (V, error) {
	var sum V
	isValue := false
	err := it(c)(func(v V) error {
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
func ReduceParallel[V, C any](it Iterable[V, C], reduceFuncFac func() func(V, V) (V, error), c C) (V, error) {
	valChan, done := ToChan(it(c))
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
func MapReduce[S, V, C any](c C, it Iterable[V, C], initial S, reduceFunc func(C, S, V) (S, error)) (S, error) {
	err := it(c)(func(v V) error {
		var err error
		initial, err = reduceFunc(c, initial, v)
		return err
	})
	return initial, err
}

// First returns the first item of the iterator
// The returned bool is false if there is no item because iterator is empty.
func First[V, C any](c C, in Iterable[V, C]) (V, bool) {
	var first V
	isFirst := false
	in(c)(func(v V) error {
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
	iter  Iterator[V]
	ended bool
}

func (ih *itHolder[V, C]) createIt() Iterable[V, C] {
	ih.iter = func(yield Consumer[V]) error {
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

	return func(c C) Iterator[V] {
		if ih.iter != nil {
			iter := ih.iter
			ih.iter = nil
			return iter
		}
		return func(yield Consumer[V]) error {
			return errors.New("copied iterator a can only be used once")
		}
	}
}

// CopyIterator copies the input iterable into num identical iterables.
// The returned iterables can be used in parallel to process the input in.
// The returned function needs to be called with the consumer context and
// returns an error if the producer returns an error.
func CopyIterator[V, C any](in Iterable[V, C], num int) ([]Iterable[V, C], func(C) error, func()) {
	done := make(chan struct{})

	var ihl []*itHolder[V, C]
	var il []Iterable[V, C]
	for i := 0; i < num; i++ {
		ih := &itHolder[V, C]{
			num:  i,
			c:    make(chan V),
			err:  make(chan error),
			done: done,
		}
		ihl = append(ihl, ih)
		il = append(il, ih.createIt())
	}

	prodDone := make(chan struct{})

	run := func(c C) error {
		defer close(done)

		running := num
		var mainErr error
		err := in(c)(func(v V) error {
			if mainErr != nil {
				return mainErr
			}
			for _, ih := range ihl {
				if !ih.ended {
					select {
					case ih.c <- v:
					case <-prodDone:
						ih.ended = true
						running--
						return errors.New("producer interrupted")
					case err := <-ih.err:
						ih.ended = true
						running--
						if err != SBC {
							mainErr = err
							return mainErr
						}
						if running == 0 {
							return SBC
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
		if err == SBC {
			return nil
		}
		return err
	}

	var isClosed atomic.Bool
	return il, run, func() {
		if !isClosed.CompareAndSwap(false, true) {
			close(prodDone)
		}
	}
}
