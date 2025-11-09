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