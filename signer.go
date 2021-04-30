package main

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

const TH = 6

var ExecutePipeline = func(jobs ...job) {
	wg := &sync.WaitGroup{}
	in := make(chan interface{})

	for _, jobTmp := range jobs {
		wg.Add(1)
		out := make(chan interface{})

		go func(in chan interface{}, out chan interface{}, wg *sync.WaitGroup, jobTmp job) {
			defer wg.Done()
			defer close(out)
			jobTmp(in, out)
		}(in,out, wg, jobTmp)
		in = out
	}

	wg.Wait()
}

var SingleHash = func(in chan interface{}, out chan interface{}) {
	mtx := &sync.Mutex{}
	wg := &sync.WaitGroup{}

	for i := range in {
		wg.Add(1)
		go SingleHashFun(i, out, wg, mtx)
	}

	wg.Wait()
}

func SingleHashFun(in interface{}, out chan interface{}, wg *sync.WaitGroup, mtx *sync.Mutex) {
	defer wg.Done()
	data := strconv.Itoa(in.(int))
	mtx.Lock()
	md5 := DataSignerMd5(data)
	mtx.Unlock()
	crc32 := make(chan string)

	go func(data string) {
		crc32 <- DataSignerCrc32(data)
	}(data)

	crc32DataMd5 := DataSignerCrc32(md5)
	crc32Data := <-crc32
	out <- crc32Data + "~" + crc32DataMd5
}


var MultiHash = func(in chan interface{}, out chan interface{}) {
	wg := &sync.WaitGroup{}

	for data := range in {
		wg.Add(1)
		go MultiHashFun(data, out, wg)
	}

	wg.Wait()
}


func MultiHashFun(in interface{}, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	wgTmp := &sync.WaitGroup{}
	result := make([]string, TH)
	strIn := in.(string)

	for i := 0; i < TH; i++ {
		wgTmp.Add(1)
		data := strconv.Itoa(i) + strIn

		go func(result []string, data string, i int, wg *sync.WaitGroup) {
			defer wg.Done()
			result[i] = DataSignerCrc32(data)
		}(result, data, i, wgTmp)
	}

	wgTmp.Wait()
	out <- strings.Join(result, "")
}

var CombineResults = func(in chan  interface{}, out chan interface{}) {
	var arr []string

	for data := range in {
		arr = append(arr, data.(string))
	}

	sort.Strings(arr)
	out <- strings.Join(arr, "_")

}
