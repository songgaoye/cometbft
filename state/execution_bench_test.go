package state_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cometbft/cometbft/internal/test"
	"github.com/cometbft/cometbft/types"
)

func BenchmarkApplyBlockBlockEvents(b *testing.B) {
	testCases := []struct {
		txs   int
		delay time.Duration
	}{
		{txs: 0},
		{txs: 100},
		{txs: 1000},
		{txs: 10, delay: 100 * time.Microsecond},
		{txs: 100, delay: 100 * time.Microsecond},
	}

	for _, tc := range testCases {
		name := fmt.Sprintf("txs=%d/delay=%s", tc.txs, tc.delay)
		b.Run(name, func(b *testing.B) {
			benchmarkApplyBlockBlockEvents(b, tc.txs, tc.delay)
		})
	}
}

// BenchmarkApplyBlockRealEventBus measures the existing EventBus buffer using
// an unbuffered consumer, as used by the indexer. Run with
// a fixed iteration count so slow-consumer time does not make calibration
// excessively long, for example: -benchtime=20x. Each iteration drains the
// consumer outside the timer, so the benchmark measures ApplyBlock's
// critical path without treating an ever-growing event backlog as a speedup.
func BenchmarkApplyBlockRealEventBus(b *testing.B) {
	testCases := []struct {
		txs      int
		delay    time.Duration
		capacity int
	}{
		{txs: 100, delay: 100 * time.Microsecond, capacity: 0},
		{txs: 100, delay: 100 * time.Microsecond, capacity: 128},
		// These capacities can hold every event emitted for one block. They
		// isolate fireEvents processing cost from EventBus queue backpressure.
		{txs: 100, delay: 100 * time.Microsecond, capacity: 1024},
		{txs: 1000, capacity: 0},
		{txs: 1000, capacity: 128},
		{txs: 1000, capacity: 2048},
	}

	for _, tc := range testCases {
		name := fmt.Sprintf("txs=%d/delay=%s/capacity=%d", tc.txs, tc.delay, tc.capacity)
		b.Run(name, func(b *testing.B) {
			benchmarkApplyBlockRealEventBus(b, tc.txs, tc.delay, tc.capacity)
		})
	}
}

func benchmarkApplyBlockRealEventBus(b *testing.B, txs int, delay time.Duration, capacity int) {
	state, stateDB, privVals := makeState(1, 1)
	blockExec := newCachedBlockExec(b, stateDB)
	events := newBenchmarkEventBus(b, capacity, delay)
	b.Cleanup(events.Close)
	blockExec.SetEventBus(events.bus)

	proposerAddr := state.NextValidators.Validators[0].Address
	lastCommit := new(types.Commit)
	b.ReportAllocs()
	b.ReportMetric(float64(txs+3), "events/block")
	b.ReportMetric(float64(capacity), "eventbus-capacity")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		height := state.LastBlockHeight + 1
		block, err := state.MakeBlock(height, test.MakeNTxs(height, int64(txs)), lastCommit, nil, proposerAddr)
		require.NoError(b, err)
		bps, err := block.MakePartSet(testPartSize)
		require.NoError(b, err)
		blockID := types.BlockID{Hash: block.Hash(), PartSetHeader: bps.Header()}
		events.ExpectBlock(txs)

		b.StartTimer()
		state, err = blockExec.ApplyBlock(state, blockID, block)
		b.StopTimer()
		require.NoError(b, err)

		events.Wait()

		extendedCommit, _, err := makeValidCommit(height, blockID, state.Validators, privVals)
		require.NoError(b, err)
		lastCommit = extendedCommit.ToCommit()
	}
}

type benchmarkEventBus struct {
	bus *types.EventBus

	delay     time.Duration
	pending   sync.WaitGroup
	consumers sync.WaitGroup
}

func newBenchmarkEventBus(b *testing.B, capacity int, delay time.Duration) *benchmarkEventBus {
	b.Helper()
	events := &benchmarkEventBus{
		bus:   types.NewEventBusWithBufferCapacity(capacity),
		delay: delay,
	}
	require.NoError(b, events.bus.Start())

	blockSub, err := events.bus.SubscribeUnbuffered(
		context.Background(), "benchmark-indexer", types.EventQueryNewBlockEvents)
	require.NoError(b, err)
	txSub, err := events.bus.SubscribeUnbuffered(
		context.Background(), "benchmark-indexer", types.EventQueryTx)
	require.NoError(b, err)

	events.consume(blockSub)
	events.consume(txSub)
	return events
}

func (e *benchmarkEventBus) consume(sub types.Subscription) {
	e.consumers.Add(1)
	go func() {
		defer e.consumers.Done()
		for {
			select {
			case <-sub.Canceled():
				return
			case <-sub.Out():
				if e.delay > 0 {
					time.Sleep(e.delay)
				}
				e.pending.Done()
			}
		}
	}()
}

func (e *benchmarkEventBus) ExpectBlock(txs int) {
	e.pending.Add(txs + 1) // NewBlockEvents plus one event for each transaction.
}

func (e *benchmarkEventBus) Wait() {
	e.pending.Wait()
}

func (e *benchmarkEventBus) Close() {
	e.pending.Wait()
	_ = e.bus.Stop()
	e.consumers.Wait()
}

func benchmarkApplyBlockBlockEvents(b *testing.B, txs int, delay time.Duration) {
	state, stateDB, privVals := makeState(1, 1)
	blockExec := newCachedBlockExec(b, stateDB)
	blockExec.SetEventBus(delayedBlockEventPublisher{delay: delay})

	proposerAddr := state.NextValidators.Validators[0].Address
	lastCommit := new(types.Commit)
	b.ReportAllocs()
	b.ReportMetric(float64(txs+3), "events/block")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		height := state.LastBlockHeight + 1
		block, err := state.MakeBlock(height, test.MakeNTxs(height, int64(txs)), lastCommit, nil, proposerAddr)
		require.NoError(b, err)
		bps, err := block.MakePartSet(testPartSize)
		require.NoError(b, err)
		blockID := types.BlockID{Hash: block.Hash(), PartSetHeader: bps.Header()}

		b.StartTimer()
		state, err = blockExec.ApplyBlock(state, blockID, block)
		b.StopTimer()
		require.NoError(b, err)

		extendedCommit, _, err := makeValidCommit(height, blockID, state.Validators, privVals)
		require.NoError(b, err)
		lastCommit = extendedCommit.ToCommit()
	}
}

type delayedBlockEventPublisher struct {
	types.NopEventBus

	delay time.Duration
}

func (p delayedBlockEventPublisher) publish() error {
	if p.delay > 0 {
		time.Sleep(p.delay)
	}
	return nil
}

func (p delayedBlockEventPublisher) PublishEventNewBlock(types.EventDataNewBlock) error {
	return p.publish()
}

func (p delayedBlockEventPublisher) PublishEventNewBlockHeader(types.EventDataNewBlockHeader) error {
	return p.publish()
}

func (p delayedBlockEventPublisher) PublishEventNewBlockEvents(types.EventDataNewBlockEvents) error {
	return p.publish()
}

func (p delayedBlockEventPublisher) PublishEventNewEvidence(types.EventDataNewEvidence) error {
	return p.publish()
}

func (p delayedBlockEventPublisher) PublishEventTx(types.EventDataTx) error {
	return p.publish()
}
