package main

import (
	"fmt"
	"hash/pkg/hashtable"
	"log"
	"runtime"
	"strconv"
	"time"
)

var (
	logger *log.Logger
	count = 1 * 1000  * 1000
)

type carrier struct {
	val int
}

func main() {

	PrintMemUsage()
	//m := make(map[string]carrier)

	//h, _ := hashtable.NewHwHash()
	h, _ := hashtable.NewSipHash()
	//h, _ := hashtable.NewT1Hash()
	//h, _ := hashtable.NewSpookyHash()
	m, _ := hashtable.NewHashMap(h)
	//m, _ := hashtable.NewAdvancedHashMap(uint64(count+500000), uint64(count*100), 0.8, 0.25)

	//m := make([]*hashtable.Bucket, 3000000)
	start := time.Now()
	PrintMemUsage()

	for i := 0; i < count; i++ {
		m.Add(strconv.Itoa(i), carrier {val: i})
		//m[strconv.Itoa(i)] = carrier {val: i}
	}

	duration := time.Since(start)
	//fmt.Printf("Length: %d, Load: %+d\n", m.Length, m.Load)
	//fmt.Printf("Length: %d, Load: %+d\n", len(m), cap(m))
	fmt.Printf("Time: %s\n\n", duration)
	time.Sleep(500 * time.Millisecond)
	PrintMemUsage()

	missCount := 0
	fmt.Println("Fetching...")
	start = time.Now()
	for i := 0; i < count; i++ {
		_, hit := m.Get(strconv.Itoa(i))
		//_, hit := m[strconv.Itoa(i)]

		if !hit {
			missCount++
		}
		//m[i] = i
	}
	duration = time.Since(start)
	fmt.Printf("Fetch time: %s\n\n", duration)
	fmt.Printf("Misses %d\n", missCount)
	PrintMemUsage()



	//tsc := gotsc.TSCOverhead()
	//fmt.Println("TSC Overhead:", tsc)



	//start := gotsc.BenchStart()
	/*
	for j := 0; j < 50; j++ {
		hash, err := hashtable.CreateHash("test")
		if err != nil {
			fmt.Println(err)
		}
	
	
		//Fold hash into 32 bits
	
		fmt.Printf("Hash64: %d, Hash64-folded: %d, Hash32: %d, position: %d\n", hash, h64, h32, hashtable.FastMod(uint32(h32), 20))
	}
	*/
	//end := gotsc.BenchEnd()
  	//avg := (end - start - tsc) / 20
	//fmt.Println("Cycles:", avg)

}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

