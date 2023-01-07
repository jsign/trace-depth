package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"math/big"
	"net"
	"sort"
	"sync"

	"github.com/ethereum/go-ethereum/ethclient"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	showResults := flag.Bool("results", false, "Analyzes existing data and prints the result to stdout")
	flag.Parse()

	db := openDB()
	if *showResults {
		printStats(db)
		return
	}

	collectData(db)
}

func collectData(db *sql.DB) {
	udsPath := "/data/ethereum/geth.ipc"
	client, err := ethclient.Dial(udsPath)
	checkErr(err)
	defer client.Close()

	ctx := context.Background()
	lastBlock, err := client.BlockByNumber(ctx, nil)
	checkErr(err)

	endBlock := lastBlock.NumberU64()
	startBlock := endBlock - 25

	existingBlocks := map[uint64]struct{}{}
	res, err := db.Query("select distinct block_number from traces where block_number between ? and ?", startBlock, endBlock)
	checkErr(err)
	for res.Next() {
		checkErr(res.Err())
		var blockNum uint64
		checkErr(res.Scan(&blockNum))
		existingBlocks[blockNum] = struct{}{}
	}
	res.Close()

	for i := startBlock; i <= endBlock; i++ {
		if _, ok := existingBlocks[i]; ok {
			continue
		}

		var lock sync.Mutex
		newTraces := map[string]TraceOutput{}

		fmt.Printf("tracing block %d\n", i)
		block, err := client.BlockByNumber(ctx, big.NewInt(int64(i)))
		checkErr(err)
		// var wg sync.WaitGroup

		txns := block.Transactions()
		// wg.Add(len(txns))
		for _, tx := range txns {
			tx := tx
			// go func() {
			// defer wg.Done()
			trace, err := traceTxn(udsPath, tx.Hash().Hex(), i)
			checkErr(err)

			lock.Lock()
			newTraces[trace.TxnHash] = trace
			lock.Unlock()
			//}()
		}
		// wg.Wait()
		err = saveTraces(db, newTraces)
		checkErr(err)
	}
}

func openDB() *sql.DB {
	db, err := sql.Open("sqlite3", "traces.db")
	checkErr(err)

	_, err = db.Exec("create table if not exists traces (txn_hash TEXT, block_number INTEGER, output TEXT)")
	checkErr(err)

	return db
}

type VKTBranchAccess int

const (
	VKTAccessWriteFirstTime VKTBranchAccess = iota
	VKTAccessWriteFree
	VKTAccessWriteHot
	VKTAccessReadFirstTime
	VKTAccessReadFree
	VKTAccessReadHot
)

type Trace struct {
	Contract        string
	MPTDepth        int
	VKTBranchAccess VKTBranchAccess
}

type TraceOutput struct {
	TxnHash     string
	BlockNumber uint64

	Traces []Trace
}

func traceTxn(udsPath string, tx string, blockNumber uint64) (TraceOutput, error) {
	uds, err := net.Dial("unix", udsPath)
	checkErr(err)

	reqBody := fmt.Sprintf(
		`{"jsonrpc":"2.0","id": "1", "method": "debug_traceTransaction", "params": ["%s", {"tracer": "treeAccessLogger", "reexec": 1000000}]}`,
		tx)
	_, err = uds.Write([]byte(reqBody))
	if err != nil {
		return TraceOutput{}, fmt.Errorf("writing to uds: %s", err)
	}

	var traces struct {
		Error  interface{}
		Result []Trace
	}
	if err := json.NewDecoder(uds).Decode(&traces); err != nil {
		return TraceOutput{}, fmt.Errorf("decoding: %s", err)
	}
	if traces.Error != nil {
		return TraceOutput{}, fmt.Errorf("json rpc error: %s", err)
	}

	trace := TraceOutput{TxnHash: tx, BlockNumber: blockNumber, Traces: traces.Result}
	return trace, nil
}

func loadTraces(db *sql.DB) []TraceOutput {
	var ret []TraceOutput

	rows, err := db.Query("select * from traces")
	checkErr(err)
	defer rows.Close()

	for rows.Next() {
		checkErr(rows.Err())

		var txnHash string
		var blockNumber uint64
		var traceOutput []byte
		err = rows.Scan(&txnHash, &blockNumber, &traceOutput)
		checkErr(err)

		var traces []Trace
		err = json.Unmarshal(traceOutput, &traces)
		checkErr(err)

		ret = append(ret, TraceOutput{
			TxnHash:     txnHash,
			BlockNumber: blockNumber,
			Traces:      traces,
		})
	}
	checkErr(rows.Err())

	return ret
}

func saveTraces(db *sql.DB, traces map[string]TraceOutput) error {
	txn, err := db.Begin()
	if err != nil {
		return fmt.Errorf("opening db txn: %s", err)
	}
	for _, trace := range traces {
		traceJSON, err := json.Marshal(trace.Traces)
		if err != nil {
			return fmt.Errorf("marshaling trace: %s", err)
		}
		if _, err := db.Exec(
			"insert into traces values (?, ?, ?)",
			trace.TxnHash,
			trace.BlockNumber,
			traceJSON,
		); err != nil {
			return fmt.Errorf("inserting into traces table: %s", err)
		}
	}
	if err := txn.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %s", err)
	}

	return nil
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func printStats(db *sql.DB) {
	txnTraces := loadTraces(db)

	firstBlock, lastBlock := uint64(2<<32), uint64(0)

	type ContractAccessStat struct {
		address     string
		depthSum    uint64
		accessCount uint64

		VKTAccessWriteFirstTimeCount uint64
		VKTAccessWriteFreeCount      uint64
		VKTAccessWriteHotCount       uint64

		VKTAccessReadFirstTimeCount uint64
		VKTAccessReadFreeCount      uint64
		VKTAccessReadHotCount       uint64
	}
	contractAccessCount := map[string]ContractAccessStat{}
	for _, txnTrace := range txnTraces {
		for _, trace := range txnTrace.Traces {
			contractStats := contractAccessCount[trace.Contract]
			contractStats.address = trace.Contract
			contractStats.depthSum += uint64(trace.MPTDepth)
			contractStats.accessCount++

			switch trace.VKTBranchAccess {
			case VKTAccessWriteFirstTime:
				contractStats.VKTAccessWriteFirstTimeCount++
			case VKTAccessWriteFree:
				contractStats.VKTAccessWriteFreeCount++
			case VKTAccessWriteHot:
				contractStats.VKTAccessWriteHotCount++
			case VKTAccessReadFirstTime:
				contractStats.VKTAccessReadFirstTimeCount++
			case VKTAccessReadFree:
				contractStats.VKTAccessReadFreeCount++
			case VKTAccessReadHot:
				contractStats.VKTAccessReadHotCount++
			default:
				panic("unknown vkt branch access")
			}
			contractAccessCount[trace.Contract] = contractStats
		}
		if txnTrace.BlockNumber > lastBlock {
			lastBlock = txnTrace.BlockNumber
		}
		if txnTrace.BlockNumber < firstBlock {
			firstBlock = txnTrace.BlockNumber
		}
	}

	contractAccessCountSorted := make([]ContractAccessStat, 0, len(contractAccessCount))
	for _, stat := range contractAccessCount {
		contractAccessCountSorted = append(contractAccessCountSorted, stat)
	}
	sort.Slice(contractAccessCountSorted, func(i, j int) bool {
		return contractAccessCountSorted[i].accessCount > contractAccessCountSorted[j].accessCount
	})

	fmt.Println("-- Summary --")
	fmt.Printf("Block range: [%d, %d] (%d total blocks)\n", firstBlock, lastBlock, lastBlock-firstBlock+1)
	fmt.Printf("Total txns: %d\n\n", len(txnTraces))

	var totalDepthSum uint64
	var totalAccesses uint64
	for _, contractStats := range contractAccessCount {
		totalDepthSum += contractStats.depthSum
		totalAccesses += contractStats.accessCount
	}
	fmt.Printf("Total read/write accesses: %d\n", totalAccesses)
	fmt.Printf("MPT average depth is: %.02f\n\n", float64(totalDepthSum)/float64(totalAccesses))

	fmt.Printf("Top 20 contracts with the most accesses:\n")
	for i := 0; i < 20; i++ {
		fmt.Printf("\tContract %s\n", contractAccessCountSorted[i].address)
		fmt.Printf("\t\tRead/Write slot accesses: %d (%.02f%%)\n", contractAccessCountSorted[i].accessCount, float64(contractAccessCountSorted[i].accessCount)/float64(totalAccesses)*100)
		fmt.Printf("\t\tMPT Avg. read/write access depth: %.02f\n", float64(contractAccessCountSorted[i].depthSum)/float64(contractAccessCountSorted[i].accessCount))

		totalReadAccess := float64(contractAccessCountSorted[i].VKTAccessReadFreeCount +
			contractAccessCountSorted[i].VKTAccessReadHotCount +
			contractAccessCountSorted[i].VKTAccessReadFirstTimeCount)
		fmt.Printf("\t\tVKT Reads branch accesses: %0.02f%% free, %0.02f%% hot, %0.02f%% cold\n",
			float64(contractAccessCountSorted[i].VKTAccessReadFreeCount)/totalReadAccess*100,
			float64(contractAccessCountSorted[i].VKTAccessReadHotCount)/totalReadAccess*100,
			float64(contractAccessCountSorted[i].VKTAccessReadFirstTimeCount)/totalReadAccess*100,
		)

		totalWriteAccess := float64(contractAccessCountSorted[i].VKTAccessWriteFreeCount +
			contractAccessCountSorted[i].VKTAccessWriteHotCount +
			contractAccessCountSorted[i].VKTAccessWriteFirstTimeCount)
		fmt.Printf("\t\tVKT Writes branch accesses: %0.02f%% free, %0.02f%% hot, %0.02f%% cold\n",
			float64(contractAccessCountSorted[i].VKTAccessWriteFreeCount)/totalWriteAccess*100,
			float64(contractAccessCountSorted[i].VKTAccessWriteHotCount)/totalWriteAccess*100,
			float64(contractAccessCountSorted[i].VKTAccessWriteFirstTimeCount)/totalWriteAccess*100,
		)
	}
}
