package iterator

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"runtime"
	"testing"
	"time"
)

func addOne(i int) int {
	return i + 1
}

func add(a, b int) int {
	return a + b
}

func square(i, v int) int {
	return v * v
}

func squareSlow(i, v int) int {
	time.Sleep(time.Millisecond * 10)
	return v * v
}

func isEven(i int) bool {
	return i&1 == 0
}

func equal[V comparable](a, b V) bool {
	return a == b
}

func check[V comparable](t *testing.T, it Iterable[V], items ...V) {
	for n := 0; n < 2; n++ {
		checkIterator[V](t, it(), items...)
	}
}

func checkIterator[V comparable](t *testing.T, it Iterator[V], items ...V) {
	assert.EqualValues(t, items, ToSlice(it))
}

func ints(n int) Iterable[int] {
	return Generate[int](n, func(i int) int {
		return i
	})
}

func TestIterables(t *testing.T) {
	type testCase[V any] struct {
		name string
		it   Iterable[V]
		want []V
	}
	var empty []int
	tests := []testCase[int]{
		{name: "empty", it: Empty[int](), want: empty},
		{name: "single", it: Single(2), want: []int{2}},
		{name: "slice", it: Slice([]int{1, 2, 3}), want: []int{1, 2, 3}},
		{name: "append", it: Append(Single(1), Single(2)), want: []int{1, 2}},
		{name: "appendSlice", it: Append(Slice([]int{1, 2}), Slice([]int{3, 4})), want: []int{1, 2, 3, 4}},
		{name: "generate", it: Generate(4, addOne), want: []int{1, 2, 3, 4}},
		{name: "map", it: Map(Slice([]int{1, 2, 3}), square), want: []int{1, 4, 9}},
		{name: "filter", it: Filter(Slice([]int{1, 2, 3, 4}), isEven), want: []int{2, 4}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got := ToSlice(test.it())
			assert.EqualValues(t, test.want, got)
			got = ToSlice(test.it())
			assert.EqualValues(t, test.want, got)
		})
	}
}

func TestEquals(t *testing.T) {
	type testCase[V any] struct {
		name  string
		it1   Iterable[V]
		it2   Iterable[V]
		equal bool
	}
	tests := []testCase[int]{
		{name: "empty", it1: Empty[int](), it2: Empty[int](), equal: true},
		{name: "slice", it1: Slice([]int{1, 2}), it2: Slice([]int{1, 2}), equal: true},
		{name: "slice", it1: Slice([]int{1, 2}), it2: Slice([]int{1, 3}), equal: false},
		{name: "slice", it1: Slice([]int{1, 2}), it2: Slice([]int{1}), equal: false},
		{name: "slice", it1: Slice([]int{1}), it2: Slice([]int{1, 2}), equal: false},
		{name: "append", it1: Append(Single(1), Single(2)), it2: Slice([]int{1, 2}), equal: true},
		{name: "generate", it1: ints(4), it2: Slice([]int{0, 1, 2, 3}), equal: true},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.equal, Equals(test.it1(), test.it2(), equal[int]))
		})
	}
}

func TestFirst(t *testing.T) {
	type testCase[V any] struct {
		name string
		it   Iterable[V]
		want V
		ok   bool
	}
	tests := []testCase[int]{
		{name: "empty", it: Empty[int](), want: 0, ok: false},
		{name: "single", it: Single(2), want: 2, ok: true},
		{name: "slice", it: Slice([]int{1, 2}), want: 1, ok: true},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got, ok := First(test.it)
			assert.EqualValues(t, test.want, got)
			assert.EqualValues(t, test.ok, ok)
		})
	}
}

func TestReduce(t *testing.T) {
	reduce, ok := Reduce(ints(11), add)
	assert.True(t, ok)
	assert.Equal(t, 55, reduce)
	reduce, ok = Reduce(Empty[int](), add)
	assert.False(t, ok)
	assert.Equal(t, 0, reduce)
}

func TestMapReduce(t *testing.T) {
	reduce := MapReduce[float64, int](ints(11), 0.0, func(s float64, i int) float64 {
		return s + float64(i)
	})
	assert.Equal(t, 55.0, reduce)
}

func checkBreak[V any](t *testing.T, in Iterator[V], n int) {
	i := 0
	in(func(v V) bool {
		i++
		if i < n {
			return true
		} else if i == n {
			return false
		}
		assert.Fail(t, "yield after stop")
		return false
	})
	assert.Equal(t, n, i, "not enough items")
}

func TestBreak(t *testing.T) {
	type testCase[V any] struct {
		name  string
		it    Iterator[V]
		count int
	}
	appended := Append(Slice[int]([]int{1, 2}), Slice[int]([]int{3, 4}))
	tests := []testCase[int]{
		{name: "slice", it: Slice[int]([]int{1, 2, 3})(), count: 2},
		{name: "append", it: appended(), count: 1},
		{name: "append", it: appended(), count: 2},
		{name: "append", it: appended(), count: 3},
		{name: "generate", it: Generate(10, addOne)(), count: 3},
		{name: "map", it: Map[int](ints(4), square)(), count: 2},
		{name: "parallelMap", it: ParallelMap[int](ints(40), squareSlow)(), count: 10},
		{name: "filter", it: Filter[int](ints(8), isEven)(), count: 2},
		{name: "chan", it: FromChan(ToChan[int](ints(10)())), count: 2},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			checkBreak(t, test.it, test.count)
		})
	}
}

func TestChannel(t *testing.T) {
	input := ints(10)
	res := ToSlice(FromChan(ToChan[int](input())))
	assert.EqualValues(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, res)
}

func TestPeek(t *testing.T) {
	it, i, ok := Peek(Generate(4, addOne)())
	assert.Equal(t, 1, i)
	assert.True(t, ok)
	assert.EqualValues(t, []int{1, 2, 3, 4}, ToSlice(it))

	it, i, ok = Peek(Empty[int]()())
	assert.Equal(t, 0, i)
	assert.False(t, ok)
	var empty []int
	assert.EqualValues(t, empty, ToSlice(it))
}

func TestIterableCombine(t *testing.T) {
	check(t, Combine(Slice([]int{1, 2, 3, 4}), add), 3, 5, 7)
	check(t, Combine(Slice([]int{1}), add))
	check(t, Combine(Slice([]int{}), add))
}

func TestIterableMerge(t *testing.T) {
	less := func(i1, i2 int) bool {
		return i1 < i2
	}
	check(t, Merge(Slice([]int{1, 3, 5}), Slice([]int{2, 4, 6}), less), 1, 2, 3, 4, 5, 6)
	check(t, Merge(Slice([]int{2, 4, 6}), Slice([]int{1, 3, 5}), less), 1, 2, 3, 4, 5, 6)
	check(t, Merge(Slice([]int{1, 2, 4}), Slice([]int{3, 5, 6}), less), 1, 2, 3, 4, 5, 6)
	check(t, Merge(Slice([]int{1, 2, 4, 6}), Slice([]int{3, 5}), less), 1, 2, 3, 4, 5, 6)
	check(t, Merge(Slice([]int{1, 3, 5}), Slice([]int{2, 4, 6, 7, 8}), less), 1, 2, 3, 4, 5, 6, 7, 8)
	check(t, Merge(Slice([]int{1, 3, 5, 7, 8}), Slice([]int{2, 4, 6}), less), 1, 2, 3, 4, 5, 6, 7, 8)
}

func TestIterableCross(t *testing.T) {
	cross := func(i1, i2 int) int {
		return i1 + i2
	}
	check(t, Cross(Slice([]int{10, 20, 30}), Slice([]int{1, 2, 3}), cross), 11, 12, 13, 21, 22, 23, 31, 32, 33)
	check(t, Cross(Slice([]int{1, 2, 3}), Slice([]int{10, 20, 30}), cross), 11, 21, 31, 12, 22, 32, 13, 23, 33)
	check(t, Cross(Slice([]int{1}), Slice([]int{10}), cross), 11)
}

func TestIirMap(t *testing.T) {
	all := Generate(10, func(n int) int { return n + 1 })
	ints := IirMap(all, func(item int) int {
		return item
	}, func(item int, lastItem int, last int) int {
		return item + last
	})
	expected := Generate(10, func(n int) int { return (n + 2) * (n + 1) / 2 })
	assert.True(t, Equals(ints(), expected(), equal[int]))
}

func TestIterableFirst(t *testing.T) {
	ints := Slice([]int{1, 2, 3, 4})
	check(t, FirstN(ints, 2), 1, 2)
	check(t, FirstN(ints, 6), 1, 2, 3, 4)
}

func TestIterableSkip(t *testing.T) {
	ints := Slice([]int{1, 2, 3, 4})
	check(t, Skip(ints, 2), 3, 4)
	check(t, Skip(ints, 6))
}

func TestIterableThinning(t *testing.T) {
	ints := Slice([]int{1, 2, 3, 4, 5, 6, 7})
	check(t, Thinning(ints, 1), 1, 3, 5, 7)
	check(t, Thinning(ints, 2), 1, 4, 7)
	check(t, Thinning(ints, 3), 1, 5, 7)
	check(t, Thinning(ints, 4), 1, 6, 7)
	check(t, Thinning(ints, 5), 1, 7)
	check(t, Thinning(ints, 10), 1, 7)
	ints = Slice([]int{1, 2, 3, 4, 5, 6})
	check(t, Thinning(ints, 1), 1, 3, 5, 6)
	check(t, Thinning(ints, 2), 1, 4, 6)
	check(t, Thinning(ints, 3), 1, 5, 6)
	check(t, Thinning(ints, 4), 1, 6)
	check(t, Thinning(ints, 10), 1, 6)
}

func TestParallelMap(t *testing.T) {
	src := Generate(20, func(n int) int { return n })
	ints := ParallelMap(src, func(i, v int) int { return v * 2 })
	expected := ToSlice(Generate(20, func(n int) int { return n * 2 })())
	assert.EqualValues(t, expected, ToSlice(ints()))
}

func TestParallelMapSlow(t *testing.T) {
	const count = 500
	const delay = time.Millisecond * 10
	all := Generate(count, func(n int) int { return n + 1 })
	ints := ParallelMap(all, func(n, v int) mapResult {
		time.Sleep(delay)
		return mapResult{res: v * 2, number: n}
	})
	expected := ToSlice(Generate(count, func(n int) mapResult {
		return mapResult{res: (n + 1) * 2, number: n}
	})())

	start := time.Now()
	assert.EqualValues(t, expected, ToSlice(ints()))

	measuredTime := time.Now().Sub(start)
	worstTime := time.Duration(count) * delay
	expectedTime := count * delay / time.Duration(runtime.NumCPU())

	fmt.Println("count:", count)
	fmt.Println("worst:", worstTime)
	fmt.Println("expected", expectedTime)
	fmt.Println("measured:", measuredTime)

	assert.True(t, measuredTime < (expectedTime+worstTime)/2, "to slow")
}

func TestParallelFilter(t *testing.T) {
	ints := Slice([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
	ints = ParallelFilter[int](ints, func(v int) bool {
		time.Sleep(time.Millisecond * 10)
		return v%2 == 0
	})

	check[int](t, ints, 2, 4, 6, 8, 10)
}

func TestParallelMapPanic1(t *testing.T) {
	src := Generate[int](20, func(n int) int { return n })
	ints := ParallelMap[int, int](src, func(i, v int) int {
		if v == 7 {
			panic("test")
		}
		return v * 2
	})

	var p any
	func() {
		defer func() {
			p = recover()
		}()
		ints()(func(i int) bool {
			return true
		})
	}()
	assert.Equal(t, "test", p)
}

func TestParallelMapPanic2(t *testing.T) {
	src := Generate[int](20, func(n int) int {
		if n == 7 {
			panic("test")
		}
		return n
	})
	ints := ParallelMap[int, int](src, func(i, v int) int { return v * 2 })

	var p any
	func() {
		defer func() {
			p = recover()
		}()
		ints()(func(i int) bool {
			return true
		})
	}()
	assert.Equal(t, "test", p)
}

type mapResult struct {
	res    int
	number int
}

func TestAutoMap(t *testing.T) {
	const count = itemsToMeasure * 50
	const delay = time.Millisecond * 10
	all := Generate[int](count, func(n int) int { return n + 1 })
	ints := AutoMap[int, mapResult](all, func(n, v int) mapResult {
		time.Sleep(delay)
		return mapResult{res: v * 2, number: n}
	})
	expected := ToSlice(Generate[mapResult](count, func(n int) mapResult {
		return mapResult{res: (n + 1) * 2, number: n}
	})())

	start := time.Now()
	slice := ToSlice(ints())
	measuredTime := time.Now().Sub(start)
	worstTime := time.Duration(count) * delay
	expectedTime := time.Duration(itemsToMeasure)*delay + (count-itemsToMeasure)*delay/time.Duration(runtime.NumCPU())
	assert.EqualValues(t, expected, slice)

	fmt.Println("count:", count)
	fmt.Println("infinite cores:", time.Duration(itemsToMeasure)*delay)
	fmt.Println("worst:", worstTime)
	fmt.Println("expected", expectedTime)
	fmt.Println("measured:", measuredTime)
	assert.True(t, measuredTime < (expectedTime+worstTime)/2, "to slow")

	ints = AutoMap[int, mapResult](all, func(n, v int) mapResult {
		return mapResult{res: v * 2, number: n}
	})
	assert.EqualValues(t, expected, ToSlice(ints()))
}

func TestAutoMapShort(t *testing.T) {
	count := itemsToMeasure
	all := Generate[int](count, func(n int) int { return n + 1 })
	ints := AutoMap[int, int](all, func(n, v int) int {
		return v * 2
	})
	expected := ToSlice(Generate[int](count, func(n int) int { return (n + 1) * 2 })())
	assert.EqualValues(t, expected, ToSlice(ints()))
	//assert.True(t, (it.(*iteratorAutoMap[int, int])).mode == mMeasure)
	assert.EqualValues(t, expected, ToSlice(ints()))
	//assert.True(t, (it.(*iteratorAutoMap[int, int])).mode == mMeasure)
}

func TestAutoFilter(t *testing.T) {
	const count = itemsToMeasure * 50
	const delay = time.Millisecond * 10
	all := Generate[int](count, func(n int) int { return n + 1 })
	ints := AutoFilter[int](all, func(v int) bool {
		time.Sleep(delay)
		return v%2 == 0
	})
	expected := ToSlice(Generate[int](count/2, func(n int) int { return (n + 1) * 2 })())

	start := time.Now()
	slice := ToSlice(ints())
	measuredTime := time.Now().Sub(start)
	worstTime := time.Duration(count) * delay
	expectedTime := time.Duration(itemsToMeasure)*delay + (count-itemsToMeasure)*delay/time.Duration(runtime.NumCPU())
	assert.EqualValues(t, expected, slice)

	fmt.Println("count:", count)
	fmt.Println("infinite cores:", time.Duration(itemsToMeasure)*delay)
	fmt.Println("worst:", worstTime)
	fmt.Println("expected", expectedTime)
	fmt.Println("measured:", measuredTime)

	assert.True(t, measuredTime < (expectedTime+worstTime)/2, "to slow")

	assert.EqualValues(t, expected, slice)
	//assert.True(t, (it.(*iteratorAutoFilter[int])).mode == mParallel)
	assert.EqualValues(t, expected, ToSlice(ints()))
	//assert.True(t, (it.(*iteratorAutoFilter[int])).mode == mParallel)
}

func TestAutoFilterSerial(t *testing.T) {
	count := itemsToMeasure * 2
	all := Generate(count, func(n int) int { return n + 1 })
	ints := AutoFilter(all, func(v int) bool {
		return v%2 == 0
	})
	expected := ToSlice(Generate(count/2, func(n int) int { return (n + 1) * 2 })())

	assert.EqualValues(t, expected, ToSlice(ints()))
	//assert.True(t, (it.(*iteratorAutoFilter[int])).mode == mSerial)
	assert.EqualValues(t, expected, ToSlice(ints()))
	//assert.True(t, (it.(*iteratorAutoFilter[int])).mode == mSerial)
}

func TestAutoFilterShort(t *testing.T) {
	count := itemsToMeasure
	all := Generate(count, func(n int) int { return n + 1 })
	ints := AutoFilter(all, func(v int) bool {
		time.Sleep(time.Microsecond * itemProcessingTimeMicroSec * 2)
		return v%2 == 0
	})
	expected := ToSlice(Generate(count/2, func(n int) int { return (n + 1) * 2 })())

	assert.EqualValues(t, expected, ToSlice(ints()))
	//assert.True(t, (it.(*iteratorAutoFilter[int])).mode == mMeasure)
	assert.EqualValues(t, expected, ToSlice(ints()))
	//assert.True(t, (it.(*iteratorAutoFilter[int])).mode == mMeasure)
}
