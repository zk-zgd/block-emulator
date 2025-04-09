package dataSupport

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"
	"sync"
	"time"
)

// 创建替身账户的共识阶段
type Data_supportTransientAccount struct {
	ModifiedMap             []map[string]uint64                   // record the modified map from the decider(s)
	AccountTransferRound    uint64                                // denote how many times accountTransfer do
	ReceivedNewAccountState map[string]*core.AccountState         // the new accountState From other Shards
	ReceivedNewTx           map[uint64][]*core.Transaction        // new transactions from other shards' pool
	AccountStateTx          map[uint64]*message.AccountStateAndTx // the map of accountState and transactions, pool
	TransientOn             bool                                  // judge nextEpoch is partition or not

	TransientReady map[uint64]bool // judge whether all shards has done all txs
	T_ReadyLock    sync.Mutex      // lock for ready

	ReadySeq     map[uint64]uint64 // 分片准备好后的序列号
	ReadySeqLock sync.Mutex        // lock for seqMap

	CollectOver bool       // judge whether all txs is collected or not
	CollectLock sync.Mutex // lock for collect
	TempTime    time.Time

	TdmMu sync.Mutex // receiveNewTXlock
}

// 初始化
func NewTransientSupport() *Data_supportTransientAccount {
	return &Data_supportTransientAccount{
		ModifiedMap:             make([]map[string]uint64, 0),
		AccountTransferRound:    0,
		ReceivedNewAccountState: make(map[string]*core.AccountState),
		ReceivedNewTx:           make(map[uint64][]*core.Transaction, params.ShardNum),
		AccountStateTx:          make(map[uint64]*message.AccountStateAndTx),
		TransientOn:             false,
		TransientReady:          make(map[uint64]bool),
		CollectOver:             false,
		ReadySeq:                make(map[uint64]uint64),
	}
}
