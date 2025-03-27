package chain

import (
	"blockEmulator/core"
	"blockEmulator/utils"
	"bytes"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/trie"
)

type TxProofResult struct {
	Found       bool
	BlockHash   []byte
	TxHash      []byte
	TxRoot      []byte
	BlockHeight uint64
	KeyList     [][]byte
	ValueList   [][]byte
	Error       string
}

type TxInitProofResult struct {
	Is_Txinit   bool
	Found       bool
	BlockHash   []byte
	TxHash      []byte
	TxRoot      []byte
	TxinitRoot  []byte
	BlockHeight uint64
	KeyList     [][]byte
	ValueList   [][]byte
	Error       string
}

// Generate proof for the tx which hash is txHash.
// Find all blocks in this chain.
func (bc *BlockChain) TxProofGenerate(txHash []byte) TxProofResult {
	nowblockHash := bc.CurrentBlock.Hash
	nowheight := bc.CurrentBlock.Header.Number

	for ; nowheight > 0; nowheight-- {
		// get a block from db
		block, err1 := bc.Storage.GetBlock(nowblockHash)
		if err1 != nil {
			return TxProofResult{
				Found:  false,
				TxHash: txHash,
				Error:  err1.Error(),
			}
		}
		if ret := TxProofGenerateOnTheBlock(txHash, block); ret.Found {
			return ret
		}

		// go into next block
		nowblockHash = block.Header.ParentBlockHash
	}

	return TxProofResult{
		Found:  false,
		TxHash: txHash,
		Error:  errors.New("cannot find this tx").Error(),
	}
}

// Make Tx proof on a certain block.
func TxProofGenerateOnTheBlock(txHash []byte, block *core.Block) TxProofResult {
	// If no value in bloom filter, then the tx must not be in this block
	bitMapIdxofTx := utils.ModBytes(txHash, 2048)
	if !block.Header.Bloom.Test(bitMapIdxofTx) {
		return TxProofResult{
			Found:  false,
			TxHash: txHash,
			Error:  errors.New("cannot find this tx").Error(),
		}
	}

	// now try to find whether this tx is in this block
	// check the correctness of this tx Trie
	txExist := false
	triedb := trie.NewDatabase(rawdb.NewMemoryDatabase())
	transactionTree := trie.NewEmpty(triedb)
	for _, tx := range block.Body {
		if bytes.Equal(txHash, tx.TxHash) {
			txExist = true
		}
		transactionTree.Update(tx.TxHash, []byte{0})
	}
	if !txExist {
		return TxProofResult{
			Found:  false,
			TxHash: txHash,
			Error:  fmt.Errorf("this tx cannot be found in this block (height=%d)", block.Header.Number).Error(),
		}
	}
	if !bytes.Equal(transactionTree.Hash().Bytes(), block.Header.TxRoot) {
		return TxProofResult{
			Found:  false,
			TxHash: txHash,
			Error:  fmt.Errorf("tx root mismatch in height %d", block.Header.Number).Error(),
		}
	}

	// generate proof
	keylist, valuelist := make([][]byte, 0), make([][]byte, 0)
	proof := rawdb.NewMemoryDatabase()
	if err := transactionTree.Prove(txHash, 0, proof); err == nil {
		it := proof.NewIterator(nil, nil)
		for it.Next() {
			keylist = append(keylist, it.Key())
			valuelist = append(valuelist, it.Value())
		}
		return TxProofResult{
			Found:       true,
			BlockHash:   block.Hash,
			TxHash:      txHash,
			TxRoot:      block.Header.TxRoot,
			BlockHeight: block.Header.Number,
			KeyList:     keylist,
			ValueList:   valuelist,
		}
	}
	return TxProofResult{
		Found:  false,
		TxHash: txHash,
		Error:  errors.New("cannot find this tx").Error(),
	}
}

func TxProofBatchGenerateOnBlock(txHashes [][]byte, block *core.Block) []TxProofResult {
	txProofs := make([]TxProofResult, len(txHashes))
	// check the tx trie first.
	// check the correctness of this tx Trie
	triedb := trie.NewDatabase(rawdb.NewMemoryDatabase())
	transactionTree := trie.NewEmpty(triedb)
	for _, tx := range block.Body {
		transactionTree.Update(tx.TxHash, []byte{0})
	}
	if !bytes.Equal(transactionTree.Hash().Bytes(), block.Header.TxRoot) {
		for i := 0; i < len(txHashes); i++ {
			txProofs[i] = TxProofResult{
				Found:  false,
				TxHash: txHashes[i],
				Error:  fmt.Errorf("tx root mismatch in height %d", block.Header.Number).Error(),
			}
		}
		return txProofs
	}

	for idx, txHash := range txHashes {
		bitMapIdxofTx := utils.ModBytes(txHash, 2048)
		if !block.Header.Bloom.Test(bitMapIdxofTx) {
			txProofs[idx] = TxProofResult{
				Found:  false,
				TxHash: txHash,
				Error:  errors.New("cannot find this tx").Error(),
			}
			continue
		}
		// generate proof
		keylist, valuelist := make([][]byte, 0), make([][]byte, 0)
		proof := rawdb.NewMemoryDatabase()
		if err := transactionTree.Prove(txHash, 0, proof); err == nil {
			it := proof.NewIterator(nil, nil)
			for it.Next() {
				keylist = append(keylist, it.Key())
				valuelist = append(valuelist, it.Value())
			}
			txProofs[idx] = TxProofResult{
				Found:       true,
				BlockHash:   block.Hash,
				TxHash:      txHash,
				TxRoot:      block.Header.TxRoot,
				BlockHeight: block.Header.Number,
				KeyList:     keylist,
				ValueList:   valuelist,
			}
		} else {
			txProofs[idx] = TxProofResult{
				Found:  false,
				TxHash: txHash,
				Error:  errors.New("cannot find this tx").Error(),
			}
		}
	}
	return txProofs
}

func TxProofVerify(txHash []byte, proof *TxProofResult) (bool, error) {
	if !proof.Found {
		return false, errors.New("the result shows not found")
	}

	// check the proof
	recoveredProof := rawdb.NewMemoryDatabase()
	listLen := len(proof.KeyList)
	for i := 0; i < listLen; i++ {
		recoveredProof.Put(proof.KeyList[i], proof.ValueList[i])
	}
	if ret, err := trie.VerifyProof(common.BytesToHash(proof.TxRoot), []byte(proof.TxHash), recoveredProof); ret == nil || err != nil {
		return false, errors.New("wrong proof")
	}

	return true, nil
}

func TxInitProofBatchGenerateOnBlock(txHashes [][]byte, block *core.Block) []TxInitProofResult {
	// ...existing code...

	// 打印详细的调试信息
	fmt.Printf("区块高度: %d\n", block.Header.Number)
	fmt.Printf("区块中 TxInit 交易数量: %d\n", len(block.TxinitBody))
	fmt.Printf("区块头中的 TxInit 根: %x\n", block.Header.TxinitRoot)

	// 构建 TxInit 树
	triedb := trie.NewDatabase(rawdb.NewMemoryDatabase())
	txInitTree := trie.NewEmpty(triedb)
	for i, txinit := range block.TxinitBody {
		// encodedTx := txinit.Encode()
		txInitTree.Update(txinit.TxHash, []byte{0})
		fmt.Printf("TxInit[%d] Hash: %x\n", i, txinit.TxHash)
	}

	computedRoot := txInitTree.Hash().Bytes()
	fmt.Printf("计算得到的 TxInit 根: %x\n", computedRoot)

	if !bytes.Equal(computedRoot, block.Header.TxinitRoot) {
		fmt.Printf("根不匹配!\n预期: %x\n实际: %x\n", block.Header.TxinitRoot, computedRoot)
	}

	// 初始化结果数组
	txInitProofs := make([]TxInitProofResult, len(txHashes))

	// 首先构建完整的TxInit树并验证树根
	// triedb := trie.NewDatabase(rawdb.NewMemoryDatabase())
	// txInitTree := trie.NewEmpty(triedb)
	/*for _, txinit := range block.TxinitBody {
		txInitTree.Update(txinit.TxHash, txinit.Encode())
	}*/
	fmt.Print("当前处理生成交易证明时的交易根是:?")
	fmt.Println(txInitTree.Hash().Bytes())
	fmt.Print("\n\n\n")
	// 验证TxInit树根是否匹配
	if !bytes.Equal(txInitTree.Hash().Bytes(), block.Header.TxinitRoot) {
		for i := 0; i < len(txHashes); i++ {
			txInitProofs[i] = TxInitProofResult{
				Found:  false,
				TxHash: txHashes[i],
				Error:  fmt.Errorf("txinit root mismatch in height %d", block.Header.Number).Error(),
			}
		}
		fmt.Print("\n\n\n整块生成验证 错误是什么？\n")
		fmt.Println(txInitProofs)
		fmt.Print("\n\n\n\n")
		return txInitProofs
	}

	// 为每个txHash生成证明
	for idx, txHash := range txHashes {
		// 检查布隆过滤器
		/*
			bitMapIdxofTx := utils.ModBytes(txHash, 2048)
			if !block.Header.Bloom.Test(bitMapIdxofTx) {
				txInitProofs[idx] = TxInitProofResult{
					Found:  false,
					TxHash: txHash,
					Error:  errors.New("cannot find this txinit").Error(),
				}
				continue
			}*/

		// 生成证明
		keylist, valuelist := make([][]byte, 0), make([][]byte, 0)
		proof := rawdb.NewMemoryDatabase()
		if err := txInitTree.Prove(txHash, 0, proof); err == nil {
			it := proof.NewIterator(nil, nil)
			for it.Next() {
				keylist = append(keylist, it.Key())
				valuelist = append(valuelist, it.Value())
			}
			txInitProofs[idx] = TxInitProofResult{
				Is_Txinit:   true,
				Found:       true,
				BlockHash:   block.Hash,
				TxHash:      txHash,
				TxRoot:      block.Header.TxRoot,
				TxinitRoot:  block.Header.TxinitRoot,
				BlockHeight: block.Header.Number,
				KeyList:     keylist,
				ValueList:   valuelist,
			}
		} else {
			txInitProofs[idx] = TxInitProofResult{
				Found:  false,
				TxHash: txHash,
				Error:  errors.New("cannot generate proof for this txinit").Error(),
			}
		}
	}
	return txInitProofs
}

// TxInitProofGenerateOnTheBlock 在指定区块上为TxInit交易生成Merkle证明
func TxInitProofGenerateOnTheBlock(txHash []byte, block *core.Block) TxInitProofResult {
	// 检查TxInit交易是否存在并构建Merkle树
	txInitExist := false
	triedb := trie.NewDatabase(rawdb.NewMemoryDatabase())
	txInitTree := trie.NewEmpty(triedb)

	// 遍历TxInit交易列表
	for _, txinit := range block.TxinitBody {
		if bytes.Equal(txHash, txinit.TxHash) {
			txInitExist = true
		}
		txInitTree.Update(txinit.TxHash, []byte{0})
	}

	if !txInitExist {
		return TxInitProofResult{
			Found:  false,
			TxHash: txHash,
			Error:  fmt.Errorf("this txinit cannot be found in block (height=%d)", block.Header.Number).Error(),
		}
	}

	// 验证TxInit树根
	txInitRoot := txInitTree.Hash().Bytes()
	if !bytes.Equal(txInitRoot, block.Header.TxinitRoot) {
		return TxInitProofResult{
			Found:  false,
			TxHash: txHash,
			Error:  fmt.Errorf("txinit root mismatch in height %d", block.Header.Number).Error(),
		}
	}

	// 生成Merkle证明
	keylist, valuelist := make([][]byte, 0), make([][]byte, 0)
	proof := rawdb.NewMemoryDatabase()
	if err := txInitTree.Prove(txHash, 0, proof); err == nil {
		it := proof.NewIterator(nil, nil)
		for it.Next() {
			keylist = append(keylist, it.Key())
			valuelist = append(valuelist, it.Value())
		}
		return TxInitProofResult{
			Is_Txinit:   true,
			Found:       true,
			BlockHash:   block.Hash,
			TxHash:      txHash,
			TxRoot:      block.Header.TxRoot,
			TxinitRoot:  block.Header.TxinitRoot,
			BlockHeight: block.Header.Number,
			KeyList:     keylist,
			ValueList:   valuelist,
		}
	}

	return TxInitProofResult{
		Found:  false,
		TxHash: txHash,
		Error:  errors.New("cannot generate proof for this txinit").Error(),
	}
}

// TxInitProofVerify 验证TxInit的Merkle证明
func TxInitProofVerify(txHash []byte, proof *TxInitProofResult) (bool, error) {
	if !proof.Found {
		return false, errors.New("the result shows not found")
	}

	if !proof.Is_Txinit {
		return false, errors.New("this is not a txinit proof")
	}

	recoveredProof := rawdb.NewMemoryDatabase()
	listLen := len(proof.KeyList)
	for i := 0; i < listLen; i++ {
		recoveredProof.Put(proof.KeyList[i], proof.ValueList[i])
	}

	if ret, err := trie.VerifyProof(common.BytesToHash(proof.TxinitRoot), []byte(proof.TxHash), recoveredProof); ret == nil || err != nil {

		fmt.Print("\n\n\n\n\nnow 错误是什么？")
		fmt.Println(err)
		fmt.Print("\n\n\n\n")

		return false, err
		//	return false, errors.New("wrong proof")
	}

	return true, nil
}
