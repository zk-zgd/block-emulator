// Definition of block

package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	"github.com/bits-and-blooms/bitset"
)

// The definition of blockheader
type BlockHeader struct {
	ParentBlockHash []byte
	StateRoot       []byte
	TxRoot          []byte
	TxinitRoot      []byte
	Bloom           bitset.BitSet
	Number          uint64
	Time            time.Time
	Miner           int32
}

// Encode blockHeader for storing further
func (bh *BlockHeader) Encode() []byte {
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	err := enc.Encode(bh)
	if err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}

// Decode blockHeader
func DecodeBH(b []byte) *BlockHeader {
	var blockHeader BlockHeader

	decoder := gob.NewDecoder(bytes.NewReader(b))
	err := decoder.Decode(&blockHeader)
	if err != nil {
		log.Panic(err)
	}

	return &blockHeader
}

// Hash the blockHeader
func (bh *BlockHeader) Hash() []byte {
	hash := sha256.Sum256(bh.Encode())
	return hash[:]
}

func (bh *BlockHeader) PrintBlockHeader() string {
	vals := []interface{}{
		hex.EncodeToString(bh.ParentBlockHash),
		hex.EncodeToString(bh.StateRoot),
		hex.EncodeToString(bh.TxRoot),
		hex.EncodeToString(bh.TxinitRoot),
		bh.Number,
		bh.Time,
	}
	res := fmt.Sprintf("%v\n", vals)
	return res
}

// The definition of block
type Block struct {
	Header     *BlockHeader
	Body       []*Transaction
	TxinitBody []*TxinitTransaction
	IsAdded    bool
	Hash       []byte
}

func NewBlock(bh *BlockHeader, bb []*Transaction, binit []*TxinitTransaction) *Block {

	return &Block{Header: bh, Body: bb, TxinitBody: binit, IsAdded: false}
}

func (b *Block) PrintBlock() string {
	vals := []interface{}{
		b.Header.Number,
		b.Hash,
	}
	res := fmt.Sprintf("%v\n", vals)
	fmt.Println(res)
	return res
}

// Encode Block for storing
func (b *Block) Encode() []byte {
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	err := enc.Encode(b)
	if err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}

// Decode Block
func DecodeB(b []byte) *Block {
	var block Block

	decoder := gob.NewDecoder(bytes.NewReader(b))
	err := decoder.Decode(&block)
	if err != nil {
		log.Panic(err)
	}

	return &block
}
