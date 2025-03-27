package core

import (
	"blockEmulator/params"
	"blockEmulator/utils"
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"log"
	"time"
)

// 此处存放txinit交易的定义

// 该条为TXinit的接收特殊地址
const TXINIT_ADDR = "12345678987654321"

type TxinitTransaction struct {
	// 发送账户地址
	Sender utils.Address

	// Recipient utils.Address
	Recipient utils.Address
	// Nonce     uint64
	Signature []byte // not implemented now.
	// Value     *big.Int
	TxHash []byte

	// TimeStamp the tx proposed.
	Time time.Time

	// 该条交易是Txinit交易
	Is_TxInit bool

	// 过渡账户地址
	TransientAccountAddr utils.Address

	// 该条交易包含原账户的ip地址，此条用于修改该账户的目标分片,只用于istxinit=true的情况下
	DestinationshardID uint64
}

func NewTxinitTransaction(sender utils.Address, transientAccountAddr utils.Address, destShardID uint64, time time.Time) *TxinitTransaction {
	tx := &TxinitTransaction{
		Sender:               sender,
		TransientAccountAddr: transientAccountAddr,
		Time:                 time,
		Is_TxInit:            true,
		DestinationshardID:   destShardID,
		Recipient:            params.TxinitSpecialReceiver,
	}
	tx.Signature = []byte("")
	hash := sha256.Sum256(tx.Encode())
	tx.TxHash = hash[:]

	return tx
}

// ...existing code...

// PrintTxInit 打印TxInit交易的详细信息
func (tx *TxinitTransaction) PrintTxInit() string {
	vals := []interface{}{
		tx.Sender,
		tx.TransientAccountAddr,
		tx.DestinationshardID,
		string(tx.TxHash),
		tx.Time,
	}
	return fmt.Sprintf("TxInit{发送方: %v, 过渡账户: %v, 目标分片: %v, 交易哈希: %v, 时间: %v}\n",
		vals...)
}

// Encode 将TxInit交易编码为字节数组用于存储
func (tx *TxinitTransaction) Encode() []byte {
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	err := enc.Encode(tx)
	if err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}

// DecodeTxInit 从字节数组解码TxInit交易
func DecodeTxInit(to_decode []byte) *TxinitTransaction {
	var tx TxinitTransaction
	decoder := gob.NewDecoder(bytes.NewReader(to_decode))
	err := decoder.Decode(&tx)
	if err != nil {
		log.Panic(err)
	}
	return &tx
}
