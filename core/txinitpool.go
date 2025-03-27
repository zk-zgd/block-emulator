package core

import (
	"fmt"
	"sync"
	"time"
	"unsafe"
)

// TxInitPool 用于存储和管理TxInit交易
type TxInitPool struct {
	TxInitQueue []*TxinitTransaction            // TxInit交易队列
	RelayPool   map[uint64][]*TxinitTransaction // 分片间转发池
	lock        sync.Mutex
}

// NewTxInitPool 创建新的TxInit交易池
func NewTxInitPool() *TxInitPool {
	fmt.Print("\nCreating new TxInitPool\n\n")
	return &TxInitPool{
		TxInitQueue: make([]*TxinitTransaction, 0),
		RelayPool:   make(map[uint64][]*TxinitTransaction),
	}
}

// AddTxInit2Pool 添加单个TxInit交易到池中
func (pool *TxInitPool) AddTxInit2Pool(tx *TxinitTransaction) {
	pool.lock.Lock()
	defer pool.lock.Unlock()
	if tx.Time.IsZero() {
		tx.Time = time.Now()
	}
	pool.TxInitQueue = append(pool.TxInitQueue, tx)
}

// AddTxInits2Pool 批量添加TxInit交易到池中
func (pool *TxInitPool) AddTxInits2Pool(txs []*TxinitTransaction) {
	pool.lock.Lock()
	defer pool.lock.Unlock()
	for _, tx := range txs {
		if tx.Time.IsZero() {
			tx.Time = time.Now()
		}
		pool.TxInitQueue = append(pool.TxInitQueue, tx)
	}
}

// PackTxInits 打包指定数量的TxInit交易
func (pool *TxInitPool) PackTxInits(max_txs uint64) []*TxinitTransaction {
	pool.lock.Lock()
	defer pool.lock.Unlock()
	txNum := max_txs
	if uint64(len(pool.TxInitQueue)) < txNum {
		txNum = uint64(len(pool.TxInitQueue))
	}
	txs_Packed := pool.TxInitQueue[:txNum]
	pool.TxInitQueue = pool.TxInitQueue[txNum:]
	return txs_Packed
}

// PackTxInitsWithBytes 根据字节大小打包TxInit交易
func (pool *TxInitPool) PackTxInitsWithBytes(max_bytes int) []*TxinitTransaction {
	pool.lock.Lock()
	defer pool.lock.Unlock()

	txNum := len(pool.TxInitQueue)
	currentSize := 0
	for tx_idx, tx := range pool.TxInitQueue {
		currentSize += int(unsafe.Sizeof(*tx))
		if currentSize > max_bytes {
			txNum = tx_idx
			break
		}
	}

	txs_Packed := pool.TxInitQueue[:txNum]
	pool.TxInitQueue = pool.TxInitQueue[txNum:]
	return txs_Packed
}

// AddRelayTxInit 添加TxInit交易到转发池
func (pool *TxInitPool) AddRelayTxInit(tx *TxinitTransaction, shardID uint64) {
	pool.lock.Lock()
	defer pool.lock.Unlock()
	if pool.RelayPool == nil {
		pool.RelayPool = make(map[uint64][]*TxinitTransaction)
	}
	_, ok := pool.RelayPool[shardID]
	if !ok {
		pool.RelayPool[shardID] = make([]*TxinitTransaction, 0)
	}
	pool.RelayPool[shardID] = append(pool.RelayPool[shardID], tx)
}

// GetTxInitQueueLen 获取交易队列长度
func (pool *TxInitPool) GetTxInitQueueLen() int {
	pool.lock.Lock()
	defer pool.lock.Unlock()
	return len(pool.TxInitQueue)
}

// ClearRelayPool 清空转发池
func (pool *TxInitPool) ClearRelayPool() {
	pool.lock.Lock()
	defer pool.lock.Unlock()
	pool.RelayPool = nil
}

// GetLocked 获取锁
func (pool *TxInitPool) GetLocked() {
	pool.lock.Lock()
}

// GetUnlocked 释放锁
func (pool *TxInitPool) GetUnlocked() {
	pool.lock.Unlock()
}
