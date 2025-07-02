package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const thRange = 6

var tokens = make(chan struct{}, 1)

func ExecutePipeline(jobs ...job) {
	in := make(chan any)
	wg := &sync.WaitGroup{}

	for _, j := range jobs {
		j := j
		out := make(chan any, 1)

		wg.Add(1)
		go func(in, out chan any, wg *sync.WaitGroup) {
			defer wg.Done()
			j(in, out)
			close(out)
		}(in, out, wg)

		in = out
	}

	wg.Wait()
}

func SingleHash(in, out chan any) {
	wg := &sync.WaitGroup{}

	for val := range in {
		data := fmt.Sprintf("%v", val)

		wg.Add(1)
		go func(data string) {
			defer wg.Done()

			crc32 := make(chan string)
			md5 := make(chan string)
			crc32second := make(chan string)

			go func() {
				crc32 <- DataSignerCrc32(data)
			}()

			go func() {
				md5 <- dataSignerMd5Safe(data)
			}()

			go func() {
				hash := DataSignerCrc32(<-md5)
				crc32second <- hash
			}()

			res := fmt.Sprintf("%s~%s", <-crc32, <-crc32second)

			out <- res
		}(data)
	}

	wg.Wait()
}

type hashResult struct {
	index int
	value string
}

func MultiHash(in, out chan any) {
	wg := &sync.WaitGroup{}

	for val := range in {
		data := val.(string)
		wg.Add(1)

		go func(data string) {
			defer wg.Done()

			results := make([]string, thRange)
			hashChan := make(chan hashResult, thRange)
			hashWG := &sync.WaitGroup{}
			for i := 0; i < thRange; i++ {
				hashWG.Add(1)

				go func(index int) {
					defer hashWG.Done()
					hash := DataSignerCrc32(strconv.Itoa(index) + data)
					hashChan <- hashResult{index, hash}
				}(i)
			}

			go func() {
				hashWG.Wait()
				close(hashChan)
			}()

			for result := range hashChan {
				results[result.index] = result.value
			}

			res := strings.Join(results, "")
			out <- res
		}(data)
	}

	wg.Wait()
}

func CombineResults(in, out chan any) {
	buf := make([]string, 0, 10)
	for val := range in {
		data := val.(string)
		buf = append(buf, data)
	}

	sort.Strings(buf)

	out <- strings.Join(buf, "_")
}

func dataSignerMd5Safe(data string) string {
	tokens <- struct{}{}
	d := DataSignerMd5(data)
	<-tokens

	return d
}
