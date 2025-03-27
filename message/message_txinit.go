package message

import (
	"blockEmulator/chain"
	"blockEmulator/core"
)

type Txinit_Relay struct {
	Txs           []*core.TxinitTransaction
	TxProofs      []chain.TxInitProofResult
	SenderShardID uint64
	SenderSeq     uint64
}
