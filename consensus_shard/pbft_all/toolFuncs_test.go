package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/params"
	"testing"
)

func TestCreateTxReq(t *testing.T) {
	// Create test node
	node := &PbftConsensusNode{
		CurChain: &chain.BlockChain{
			Txpool: core.NewTxPool(),
		},
	}

	// Test cases
	tests := []struct {
		name     string
		shardID  int
		wantFrom string
		wantTo   string
	}{
		{
			name:     "Create transaction request for shard 0",
			shardID:  0,
			wantFrom: params.SenderAddr[0],
			wantTo:   params.SenderAddr[3],
		},
		{
			name:     "Create transaction request for shard 1",
			shardID:  1,
			wantFrom: params.SenderAddr[1],
			wantTo:   params.SenderAddr[3],
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear pool before test
			node.CurChain.Txpool = core.NewTxPool()

			// Create tx request
			node.CreateTxReq(tt.shardID)

			// Get transaction from pool
			txs := node.CurChain.Txpool.PackTxs(2000)
			if len(txs) != 1 {
				t.Errorf("Expected 1 transaction in pool, got %d", len(txs))
			}

			tx := txs[0]
			if !tx.Is_Txreq {
				t.Error("Transaction should be marked as TxReq")
			}

			if tx.Sender != tt.wantFrom {
				t.Errorf("Expected From address %s, got %s", tt.wantFrom, tx.Sender)
			}

			if tx.Recipient != tt.wantTo {
				t.Errorf("Expected To address %s, got %s", tt.wantTo, tx.Recipient)
			}

			if tx.Value != params.Init_Balance {
				t.Errorf("Expected Value %d, got %d", params.Init_Balance, tx.Value)
			}
		})
	}
}
