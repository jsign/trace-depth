package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"sort"
	"sync"

	"github.com/ethereum/go-ethereum/ethclient"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	ctx := context.Background()
	udsPath := "/data/ethereum/geth.ipc"
	client, err := ethclient.Dial(udsPath)
	checkErr(err)
	defer client.Close()

	db := openDB()
	existingTraces := loadTraces(db)
	sort.Slice(existingTraces, func(i, j int) bool { return existingTraces[i].BlockNumber < existingTraces[j].BlockNumber })

	lastBlock, err := client.BlockByNumber(ctx, nil)
	checkErr(err)

	endBlock := lastBlock.NumberU64()
	// startBlock := uint64(16_100_000)
	startBlock := endBlock - 10

	for i := startBlock; i <= endBlock; i++ {
		idx := sort.Search(len(existingTraces), func(j int) bool { return existingTraces[j].BlockNumber >= i })
		if idx != len(existingTraces) {
			continue
		}

		var lock sync.Mutex
		newTraces := map[string]TraceOutput{}

		fmt.Printf("tracing block %d\n", i)
		block, err := client.BlockByNumber(ctx, big.NewInt(int64(i)))
		checkErr(err)
		var wg sync.WaitGroup

		txns := block.Transactions()
		wg.Add(len(txns))
		for _, tx := range txns {
			tx := tx
			go func() {
				defer wg.Done()
				trace, err := traceTxn(udsPath, tx.Hash().Hex(), i)
				checkErr(err)

				lock.Lock()
				newTraces[trace.TxnHash] = trace
				lock.Unlock()
			}()
		}
		wg.Wait()
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

type Trace struct {
	Contract    string
	MPTDepth    int
	Opcode      string
	StorageSlot string
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
		`{"jsonrpc":"2.0","id": "1", "method": "debug_traceTransaction", "params": ["%s", {"tracer": "treeAccessLogger"}]}`,
		tx)
	_, err = uds.Write([]byte(reqBody))
	if err != nil {
		return TraceOutput{}, fmt.Errorf("writing to uds: %s", err)
	}

	var traces struct {
		Result []Trace
	}
	if err := json.NewDecoder(uds).Decode(&traces); err != nil {
		return TraceOutput{}, fmt.Errorf("decoding: %s", err)
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
