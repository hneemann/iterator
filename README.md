# iterator #

This project started out playing around with the Iterator [proposal](https://github.com/golang/go/issues/61405) filed by Russ Cox.

Not everything was plain sailing, but to me it feels quite nice.

Iteration itself uses old
```
iter(func(i I) bool {
  fmt.Println(i)
  return true
})
```
approach instead of proposed
```
for i := range iter {
    fmt.Println(i)
}
```
This project is more to explore how it feels to work with iterator functions.

In the end I had two problems: First, I had problems with errors generated in the yield method and second, it turns out that for my use case a context was needed for the iteration.

Both could be implemented with Russ Cox's original idea, but the well-known producer-consumer pattern seemed to make more sense to me. Here, not only the yield function is passed to the iterator, but also a context.