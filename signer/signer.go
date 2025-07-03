package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

func ExecutePipeline(jobs ...job) {
	in := make(chan any)
	wg := &sync.WaitGroup{}

	for _, j := range jobs {
		j := j
		out := make(chan any)

		wg.Add(1)
		go func(in, out chan any) {
			defer wg.Done()
			j(in, out)
			close(out)
		}(in, out)

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

			crc32Ch := make(chan string, 1)
			md5Ch := make(chan string, 1)
			crc32md5Ch := make(chan string, 1)

			go func() {
				crc32Ch <- DataSignerCrc32(data)
			}()

			go func() {
				md5Ch <- dataSignerMd5Safe(data)
			}()

			go func() {
				hash := DataSignerCrc32(<-md5Ch)
				crc32md5Ch <- hash
			}()

			res := fmt.Sprintf("%s~%s", <-crc32Ch, <-crc32md5Ch)

			out <- res
		}(data)
	}

	wg.Wait()
}

type hashResult struct {
	index int
	value string
}

const thRange = 6

func MultiHash(in, out chan any) {
	wg := &sync.WaitGroup{}

	for val := range in {
		data := fmt.Sprintf("%v", val)

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
		data := fmt.Sprintf("%v", val)
		buf = append(buf, data)
	}

	sort.Strings(buf)

	out <- strings.Join(buf, "_")
}

var tokens = make(chan struct{}, 1)

func dataSignerMd5Safe(data string) string {
	tokens <- struct{}{}
	defer func() { <-tokens }()
	return DataSignerMd5(data)
}
