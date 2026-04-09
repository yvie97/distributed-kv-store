// Benchmark tool for DistKV — measures QPS and p99 read latency.
//
// Usage:
//
//	go run ./tests/benchmark -server=localhost:8080
//
// Start a local cluster first:
//
//	make dev-cluster
//
// Availability test (manual):
//
//	While the benchmark is running, kill one node to verify requests keep succeeding:
//	  pkill -f "distkv-server.*node2"
//	Requests should continue succeeding (quorum still reachable with 2 of 3 nodes).
//	Restart the node and confirm it rejoins:
//	  ./build/distkv-server --node-id=node2 --address=localhost:8081 --data-dir=data2 --seed-nodes=localhost:8080 &
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"distkv/proto"
)

const (
	numKeys       = 10_000
	valueSize     = 64
	benchDuration = 10 * time.Second
)

func main() {
	server := flag.String("server", "localhost:8080", "DistKV server address")
	flag.Parse()

	conn, err := grpc.Dial(*server, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to %s: %v\n", *server, err)
		os.Exit(1)
	}
	defer conn.Close()
	client := proto.NewDistKVClient(conn)

	// Pre-load keys
	fmt.Printf("Pre-loading %d keys into %s...\n", numKeys, *server)
	preload(client, numKeys)
	fmt.Println("Pre-load done.\n")

	// Phase 1: read-only, sweep concurrency
	fmt.Println("=== Phase 1: Read-only benchmark ===")
	fmt.Printf("%-12s %-12s %-12s\n", "Concurrency", "QPS", "p99 (ms)")
	readMaxQPS := 0.0
	for _, c := range []int{10, 25, 50, 100, 150, 200} {
		qps, p99 := runBenchmark(client, c, 1.0)
		marker := ""
		if p99 > 10 {
			marker = "  ← p99 > 10ms"
		} else {
			readMaxQPS = qps
		}
		fmt.Printf("%-12d %-12.0f %-12.2f%s\n", c, qps, p99, marker)
	}
	fmt.Printf("\nMax read QPS with p99 < 10ms: %.0f\n\n", readMaxQPS)

	// Phase 2: mixed read/write (50/50), sweep concurrency
	fmt.Println("=== Phase 2: Mixed read/write benchmark (50/50) ===")
	fmt.Printf("%-12s %-12s %-12s\n", "Concurrency", "QPS", "p99 (ms)")
	mixedMaxQPS := 0.0
	for _, c := range []int{10, 25, 50, 100, 150, 200} {
		qps, p99 := runBenchmark(client, c, 0.5)
		marker := ""
		if p99 > 10 {
			marker = "  ← p99 > 10ms"
		} else {
			mixedMaxQPS = qps
		}
		fmt.Printf("%-12d %-12.0f %-12.2f%s\n", c, qps, p99, marker)
	}
	fmt.Printf("\nMax mixed QPS with p99 < 10ms: %.0f\n", mixedMaxQPS)
}

// preload writes numKeys random key-value pairs into the cluster.
func preload(client proto.DistKVClient, n int) {
	value := make([]byte, valueSize)
	rand.Read(value)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("bench-key-%d", i)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _ = client.Put(ctx, &proto.PutRequest{Key: key, Value: value})
		cancel()
		if (i+1)%1000 == 0 {
			fmt.Printf("  %d / %d\n", i+1, n)
		}
	}
}

// runBenchmark runs a benchmark for benchDuration seconds with the given
// concurrency and read ratio, and returns (QPS, p99_ms).
func runBenchmark(client proto.DistKVClient, concurrency int, readRatio float64) (float64, float64) {
	var (
		mu        sync.Mutex
		latencies []time.Duration
		totalOps  int64
		stop      = make(chan struct{})
		wg        sync.WaitGroup
	)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("bench-key-%d", rand.Intn(numKeys))
				start := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if rand.Float64() < readRatio {
					_, _ = client.Get(ctx, &proto.GetRequest{Key: key})
				} else {
					value := make([]byte, valueSize)
					_, _ = client.Put(ctx, &proto.PutRequest{Key: key, Value: value})
				}
				cancel()
				lat := time.Since(start)
				atomic.AddInt64(&totalOps, 1)
				mu.Lock()
				latencies = append(latencies, lat)
				mu.Unlock()
			}
		}()
	}

	time.Sleep(benchDuration)
	close(stop)
	wg.Wait()

	ops := atomic.LoadInt64(&totalOps)
	qps := float64(ops) / benchDuration.Seconds()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p99idx := int(float64(len(latencies)) * 0.99)
	if p99idx >= len(latencies) {
		p99idx = len(latencies) - 1
	}
	p99ms := float64(latencies[p99idx]) / float64(time.Millisecond)

	return qps, p99ms
}
