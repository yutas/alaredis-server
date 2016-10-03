package main

import (
	"sort"
)


type expireAtList []int64

func (l expireAtList) Len() int {
	return len(l)
}

func (l expireAtList) Less(i, j int) bool {
	return l[i] < l[j]
}

func (l expireAtList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

func main() {
	l := expireAtList{5,2,5,6,8,8,543}
	sort.Sort(l)
	print(l)
}