package message

import (
	"blockEmulator/core"
)

// 交易重定向消息的消息体
type TxRedirect struct {
	Isnil            bool        // 交易池是否是空的
	ShardID          uint64      // 当前消息属于哪个分片
	Temppool         core.TxPool // 临时交易池
	ReceiveTxShardID uint64      // 交易接收分片ID
}

// 中继交易的重定向，需要重新分配
type TxRedirectRelay struct {
	Isnil                 bool
	ShardID               uint64
	TempRelayPool         core.TxPool
	ReceiveTxRelayShardID uint64
}

type TxRedirectMsg struct {
	// noting 没有任何东西在这里，就是一个通知消息体
	Txs2DestinationShard []*core.Transaction
}
