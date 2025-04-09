// Here the blockchain structrue is defined
// each node in this system will maintain a blockchain object.

package chain

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"blockEmulator/storage"
	"blockEmulator/utils"
	"errors"
	"fmt"
	"log"
	"math/big"
	"sync"
	"time"

	"github.com/bits-and-blooms/bitset"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
)

type BlockChain struct {
	db           ethdb.Database      // the leveldb database to store in the disk, for status trie
	triedb       *trie.Database      // the trie database which helps to store the status trie
	ChainConfig  *params.ChainConfig // the chain configuration, which can help to identify the chain
	CurrentBlock *core.Block         // the top block in this blockchain
	Storage      *storage.Storage    // Storage is the bolt-db to store the blocks
	Txpool       *core.TxPool        // the transaction pool
	Txinitpool   *core.TxInitPool    // txinit交易池
	PartitionMap map[string]uint64   // the partition map which is defined by some algorithm can help account parition
	DirtyMap     map[string]uint64   // 存放修改的账户地址部分
	dmlock       sync.RWMutex
	pmlock       sync.RWMutex

	
}

// Get the transaction root, this root can be used to check the transactions
func GetTxTreeRoot(txs []*core.Transaction) []byte {
	// use a memory trie database to do this, instead of disk database
	triedb := trie.NewDatabase(rawdb.NewMemoryDatabase())
	transactionTree := trie.NewEmpty(triedb)
	for _, tx := range txs {
		transactionTree.Update(tx.TxHash, []byte{0})
	}
	return transactionTree.Hash().Bytes()
}

// Get the transaction root, 这个交易根是txinit的交易根
func GetTxinitTreeRoot(txinits []*core.TxinitTransaction) []byte {
	triedb := trie.NewDatabase(rawdb.NewMemoryDatabase())
	transactionTree := trie.NewEmpty(triedb)
	for _, tx := range txinits {
		transactionTree.Update(tx.TxHash, []byte{0})
	}
	return transactionTree.Hash().Bytes()
}

// Get bloom filter
func GetBloomFilter(txs []*core.Transaction) *bitset.BitSet {
	bs := bitset.New(2048)
	for _, tx := range txs {
		bs.Set(utils.ModBytes(tx.TxHash, 2048))
	}
	return bs
}

// Write Partition Map
func (bc *BlockChain) Update_PartitionMap(key string, val uint64) {
	bc.pmlock.Lock()
	defer bc.pmlock.Unlock()
	bc.PartitionMap[key] = val
}

// Get parition (if not exist, return default)
func (bc *BlockChain) Get_PartitionMap(key string) uint64 {
	bc.pmlock.RLock()
	defer bc.pmlock.RUnlock()
	if _, ok := bc.PartitionMap[key]; !ok {
		return uint64(utils.Addr2Shard(key))
	}
	return bc.PartitionMap[key]
}

// Send a transaction to the pool (need to decide which pool should be sended)
func (bc *BlockChain) SendTx2Pool(txs []*core.Transaction) {
	bc.Txpool.AddTxs2Pool(txs)
}

// handle transactions and modify the status trie
func (bc *BlockChain) GetUpdateStatusTrie(txs []*core.Transaction, txinits []*core.TxinitTransaction) common.Hash {
	fmt.Printf("The len of txs is %d\n", len(txs))
	// the empty block (length of txs is 0) condition
	if len(txs) == 0 {
		return common.BytesToHash(bc.CurrentBlock.Header.StateRoot)
	}
	// build trie from the triedb (in disk)
	st, err := trie.New(trie.TrieID(common.BytesToHash(bc.CurrentBlock.Header.StateRoot)), bc.triedb)
	if err != nil {
		log.Panic(err)
	}
	cnt := 0
	// handle transactions, the signature check is ignored here
	for i, tx := range txs {
		// fmt.Printf("tx %d: %s, %s\n", i, tx.Sender, tx.Recipient)
		// senderIn := false
		if !tx.Relayed && (bc.Get_PartitionMap(tx.Sender) == bc.ChainConfig.ShardID || tx.HasBroker) {
			// senderIn = true
			// fmt.Printf("the sender %s is in this shard %d, \n", tx.Sender, bc.ChainConfig.ShardID)
			// modify local accountstate
			s_state_enc, _ := st.Get([]byte(tx.Sender))
			var s_state *core.AccountState
			if s_state_enc == nil {
				// fmt.Println("missing account SENDER, now adding account")
				ib := new(big.Int)
				ib.Add(ib, params.Init_Balance)
				s_state = &core.AccountState{
					Nonce:   uint64(i),
					Balance: ib,
				}
			} else {
				s_state = core.DecodeAS(s_state_enc)
			}
			s_balance := s_state.Balance
			if s_balance.Cmp(tx.Value) == -1 {
				fmt.Printf("the balance is less than the transfer amount\n")
				continue
			}
			s_state.Deduct(tx.Value)
			st.Update([]byte(tx.Sender), s_state.Encode())
			cnt++
		}
		// recipientIn := false
		if bc.Get_PartitionMap(tx.Recipient) == bc.ChainConfig.ShardID || tx.HasBroker {
			// fmt.Printf("the recipient %s is in this shard %d, \n", tx.Recipient, bc.ChainConfig.ShardID)
			// recipientIn = true
			// modify local state
			r_state_enc, _ := st.Get([]byte(tx.Recipient))
			var r_state *core.AccountState
			if r_state_enc == nil {
				// fmt.Println("missing account RECIPIENT, now adding account")
				ib := new(big.Int)
				ib.Add(ib, params.Init_Balance)
				r_state = &core.AccountState{
					Nonce:   uint64(i),
					Balance: ib,
				}
			} else {
				r_state = core.DecodeAS(r_state_enc)
			}
			r_state.Deposit(tx.Value)
			st.Update([]byte(tx.Recipient), r_state.Encode())
			cnt++
		}
	}

	for i, txinit := range txinits {

		if bc.ChainConfig.ShardID == txinit.DestinationshardID {
			fmt.Print("这里是进行txinit在目的分片上链的地方，注意哈希值改变:\n\n\n")
			r_state_enc, _ := st.Get([]byte(txinit.Sender))
			var r_state *core.AccountState
			if r_state_enc == nil {
				r_state = &core.AccountState{
					Nonce:   uint64(i),
					Balance: big.NewInt(0),
				}
			} else {
				r_state = core.DecodeAS(r_state_enc)
				fmt.Print("本mudi分片已经处理了txinit交易了，不需要再处理\n\n\n")
			}
			st.Update([]byte(txinit.Sender), r_state.Encode())
		} else {
			fmt.Print("这里是其他收到交易广播分片进行txinit上链的地方，注意哈希值改变:\n\n\n")
			r_state_enc, _ := st.Get([]byte(params.TxinitSpecialReceiver))
			var r_state *core.AccountState
			if r_state_enc == nil {
				r_state = &core.AccountState{
					Nonce:   uint64(i),
					Balance: big.NewInt(0),
				}
			} else {
				r_state = core.DecodeAS(r_state_enc)
				fmt.Print("本其他分片已经处理了txinit交易了，不需要再处理\n\n\n")
			}
			st.Update([]byte(params.TxinitSpecialReceiver), r_state.Encode())
		}

	}

	if cnt == 0 && len(txinits) != 0 {
		rt, ns := st.Commit(false)
		// if `ns` is nil, the `err = bc.triedb.Update(trie.NewWithNodeSet(ns))` will report an error.
		if ns != nil {
			err = bc.triedb.Update(trie.NewWithNodeSet(ns))
			if err != nil {
				log.Panic()
			}
			err = bc.triedb.Commit(rt, false)
			if err != nil {
				log.Panic(err)
			}
		}
		fmt.Println("modified account number is ", cnt)
		return rt
	}
	// commit the memory trie to the database in the disk
	if cnt == 0 {
		return common.BytesToHash(bc.CurrentBlock.Header.StateRoot)
	}

	rt, ns := st.Commit(false)
	// if `ns` is nil, the `err = bc.triedb.Update(trie.NewWithNodeSet(ns))` will report an error.
	if ns != nil {
		err = bc.triedb.Update(trie.NewWithNodeSet(ns))
		if err != nil {
			log.Panic()
		}
		err = bc.triedb.Commit(rt, false)
		if err != nil {
			log.Panic(err)
		}
	}
	fmt.Println("modified account number is ", cnt)
	return rt
}

// generate (mine) a block, this function return a block
func (bc *BlockChain) GenerateBlock(miner int32) *core.Block {
	var txs []*core.Transaction
	var txinits []*core.TxinitTransaction
	// pack the transactions from the txpool
	if params.UseBlocksizeInBytes == 1 {
		txs = bc.Txpool.PackTxsWithBytes(params.BlocksizeInBytes)
	} else {
		txs = bc.Txpool.PackTxs(bc.ChainConfig.BlockSize)
		txinits = bc.Txinitpool.PackTxInits(bc.ChainConfig.BlockSize)
	}

	// 更新本分片的账户路由
	/*
		for _, txx := range txs {
			// fmt.Print("这里是更新本分片map中没有的账户的地方.\n")
			if bc.Get_PartitionMap(txx.Sender) == uint64(utils.Addr2Shard(txx.Sender)) {
				bc.Update_PartitionMap(txx.Sender, uint64(utils.Addr2Shard(txx.Sender)))
			}
			if bc.Get_PartitionMap(txx.Recipient) == uint64(utils.Addr2Shard(txx.Recipient)) {
				bc.Update_PartitionMap(txx.Recipient, uint64(utils.Addr2Shard(txx.Recipient)))
			}
		}
	*/

	if bc.ChainConfig.ShardID == 0 {
		for _, tx := range txs {
			if tx.Sender == "32be343b94f860124dc4fee278fdcbd38c102d88" {
				params.AddrNum++
			}
		}
		fmt.Printf("当前账户32be343b94f860124dc4fee278fdcbd38c102d88的交易数量是：%d\n\n\n", params.AddrNum)
	}
	if bc.ChainConfig.ShardID == 1 {
		for _, tx := range txs {
			if tx.Sender == "32be343b94f860124dc4fee278fdcbd38c102d88" {
				params.Addrnum1++
			}
		}
		fmt.Printf("当前账户32be343b94f860124dc4fee278fdcbd38c102d88的交易数量是：%d\n\n\n", params.Addrnum1)
	}

	bh := &core.BlockHeader{
		ParentBlockHash: bc.CurrentBlock.Hash,
		Number:          bc.CurrentBlock.Header.Number + 1,
		Time:            time.Now(),
	}
	// handle transactions to build root
	rt := bc.GetUpdateStatusTrie(txs, txinits)

	bh.StateRoot = rt.Bytes()
	bh.TxRoot = GetTxTreeRoot(txs)
	bh.TxinitRoot = GetTxinitTreeRoot(txinits)

	fmt.Print("新的块生成时txinit交易的根是：")
	fmt.Println(bh.TxinitRoot)
	params.Changetxinitroot = bh.TxinitRoot
	fmt.Print("\n\n\n")

	bh.Bloom = *GetBloomFilter(txs)
	bh.Miner = miner

	b := core.NewBlock(bh, txs, txinits)
	/*
		// 验证交易根是否一致
		if !bytes.Equal(b.Header.TxRoot, GetTxTreeRoot(txs)) {
			log.Printf("gene交易根不匹配: 期望 %x, 实际 %x", b.Header.TxRoot, GetTxTreeRoot(txs))
		}

		// 验证 TxInit 根是否一致
		if !bytes.Equal(b.Header.TxinitRoot, GetTxinitTreeRoot(txinits)) {
			log.Printf("geneTxInit根不匹配: 期望 %x, 实际 %x", b.Header.TxinitRoot, GetTxinitTreeRoot(txinits))
		}
	*/
	b.Hash = b.Header.Hash()
	return b
}

// new a genisis block, this func will be invoked only once for a blockchain object
func (bc *BlockChain) NewGenisisBlock() *core.Block {
	body := make([]*core.Transaction, 0)
	bodyinit := make([]*core.TxinitTransaction, 0)
	bh := &core.BlockHeader{
		Number: 0,
	}
	// build a new trie database by db
	triedb := trie.NewDatabaseWithConfig(bc.db, &trie.Config{
		Cache:     0,
		Preimages: true,
	})
	bc.triedb = triedb
	statusTrie := trie.NewEmpty(triedb)
	bh.StateRoot = statusTrie.Hash().Bytes()
	bh.TxRoot = GetTxTreeRoot(body)
	bh.TxinitRoot = GetTxinitTreeRoot(bodyinit)
	bh.Bloom = *GetBloomFilter(body)
	b := core.NewBlock(bh, body, bodyinit)
	b.Hash = b.Header.Hash()
	return b
}

// add the genisis block in a blockchain
func (bc *BlockChain) AddGenisisBlock(gb *core.Block) {
	bc.Storage.AddBlock(gb)
	newestHash, err := bc.Storage.GetNewestBlockHash()
	if err != nil {
		log.Panic()
	}
	curb, err := bc.Storage.GetBlock(newestHash)
	if err != nil {
		log.Panic()
	}
	bc.CurrentBlock = curb
}

// add a block
func (bc *BlockChain) AddBlock(b *core.Block) {
	if b.Header.Number != bc.CurrentBlock.Header.Number+1 {
		fmt.Println("the block height is not correct")
		return
	}
	/*
		if !bytes.Equal(b.Header.ParentBlockHash, bc.CurrentBlock.Hash) {

			fmt.Print("没出错，这里修改一下块哈希就行\n")
			fmt.Print("原来的新块父哈希是：")
			fmt.Println(b.Header.ParentBlockHash)
			fmt.Print("现在的CurrentBlock哈希是: ")
			fmt.Println(bc.CurrentBlock.Hash)
			// bc.CurrentBlock.IsAdded = false
			b.Header.ParentBlockHash = bc.CurrentBlock.Hash
			b.Header.StateRoot = bc.GetUpdateStatusTrie(b.Body, b.TxinitBody).Bytes()
			b.Header.TxRoot = GetTxTreeRoot(b.Body)
			b.Hash = b.Header.Hash()
			fmt.Print("修改后的块哈希是: ")
			fmt.Println(b.Header.ParentBlockHash)
			fmt.Print("\n\n\n")
			_, err := trie.New(trie.TrieID(common.BytesToHash(b.Header.StateRoot)), bc.triedb)
			if err != nil {
				rt := bc.GetUpdateStatusTrie(b.Body, b.TxinitBody)
				fmt.Println(bc.CurrentBlock.Header.Number+1, "the root = ", rt.Bytes())
			}
			bc.CurrentBlock = b
			bc.Storage.AddBlock(b)
			return

			fmt.Println("err parent block hash")
			return
		}
	*/

	// if the treeRoot is existed in the node, the transactions is no need to be handled again
	_, err := trie.New(trie.TrieID(common.BytesToHash(b.Header.StateRoot)), bc.triedb)
	if err != nil {
		rt := bc.GetUpdateStatusTrie(b.Body, b.TxinitBody)
		fmt.Println(bc.CurrentBlock.Header.Number+1, "the root = ", rt.Bytes())
	}
	bc.CurrentBlock = b
	// 此处修改Partitionmap
	/*
		for _, txinit := range b.TxinitBody {
			if utils.Addr2Shard(txinit.Sender) != int(bc.ChainConfig.ShardID) {
				fmt.Print("\n\n\n当前迁移账户的分片号是: ")
				fmt.Println(utils.Addr2Shard(txinit.Sender))
				fmt.Printf("这里修改每个分片的迁移账户路由1，当前分片为%d\n\n\n", bc.ChainConfig.ShardID)
				bc.Update_PartitionMap(txinit.TransientAccountAddr, txinit.DestinationshardID)
				bc.DirtyMap[txinit.TransientAccountAddr] = txinit.DestinationshardID
			} else {
				fmt.Printf("这里修改每个分片的迁移账户路由2，当前分片为%d\n\n\n", bc.ChainConfig.ShardID)
				bc.DirtyMap[txinit.TransientAccountAddr] = txinit.DestinationshardID
			}
		}
	*/
	bc.Storage.AddBlock(b)
	/*
		if bc.ChainConfig.ShardID == 3 {
			block, err := bc.Storage.GetBlock(b.Header.ParentBlockHash)
			log.Println(block.Header.Number)
			log.Print(err)
		}
	*/
}

// new a blockchain.
// the ChainConfig is pre-defined to identify the blockchain; the db is the status trie database in disk
func NewBlockChain(cc *params.ChainConfig, db ethdb.Database) (*BlockChain, error) {
	fmt.Println("Generating a new blockchain", db)
	chainDBfp := params.DatabaseWrite_path + fmt.Sprintf("chainDB/S%d_N%d", cc.ShardID, cc.NodeID)
	bc := &BlockChain{
		db:           db,
		ChainConfig:  cc,
		Txpool:       core.NewTxPool(),
		Txinitpool:   core.NewTxInitPool(),
		Storage:      storage.NewStorage(chainDBfp, cc),
		PartitionMap: make(map[string]uint64),
		DirtyMap:     make(map[string]uint64),
	}
	curHash, err := bc.Storage.GetNewestBlockHash()
	if err != nil {
		fmt.Println("There is no existed blockchain in the database. ")
		// if the Storage bolt database cannot find the newest blockhash,
		// it means the blockchain should be built in height = 0
		if err.Error() == "cannot find the newest block hash" {
			genisisBlock := bc.NewGenisisBlock()
			bc.AddGenisisBlock(genisisBlock)
			fmt.Println("New genisis block")
			return bc, nil
		}
		log.Panic()
	}

	// there is a blockchain in the storage
	fmt.Println("Existing blockchain found")
	curb, err := bc.Storage.GetBlock(curHash)
	if err != nil {
		log.Panic()
	}

	bc.CurrentBlock = curb
	triedb := trie.NewDatabaseWithConfig(db, &trie.Config{
		Cache:     0,
		Preimages: true,
	})
	bc.triedb = triedb
	// check the existence of the trie database
	_, err = trie.New(trie.TrieID(common.BytesToHash(curb.Header.StateRoot)), triedb)
	if err != nil {
		log.Panic()
	}
	fmt.Println("The status trie can be built")
	fmt.Println("Generated a new blockchain successfully")
	return bc, nil
}

// check a block is valid or not in this blockchain config
func (bc *BlockChain) IsValidBlock(b *core.Block) error {
	/*
		if bc.CurrentBlock.IsAdded {
			fmt.Print("现在块的哈希已经改过了，需要重新修改当前块的父哈希\n\n")
			if string(GetTxTreeRoot(b.Body)) != string(b.Header.TxRoot) {
				fmt.Println("the transaction root is wrong")
				return errors.New("the transaction root is wrong")
			}
			// bc.CurrentBlock.IsAdded = false
			bc.CurrentBlock.Hash = bc.CurrentBlock.Header.Hash()
			b.Header.ParentBlockHash = bc.CurrentBlock.Hash
			return errors.New("修改一下当前块的哈希")
		}
	*/
	return nil

	// 此部分往下在单个节点启动时可以使用，如果有多个节点将会因为不同步的问题导致错误
	if string(b.Header.ParentBlockHash) != string(bc.CurrentBlock.Hash) {
		/*
			if bc.CurrentBlock.IsAdded {
				fmt.Print("这里修改新块头的父哈希\n")
				b.Header.ParentBlockHash = bc.CurrentBlock.Hash
				b.Hash = b.Header.Hash()
				fmt.Print("修改好了喵~\n\n")
				return nil
			}
		*/
		fmt.Print("headparenthash\n")
		fmt.Println(b.Header.ParentBlockHash)
		fmt.Print("currentBlockHash\n")
		fmt.Println(bc.CurrentBlock.Hash)
		fmt.Println("the parentblock hash is not equal to the current block hash")
		return errors.New("the parentblock hash is not equal to the current block hash")
	} else if string(GetTxTreeRoot(b.Body)) != string(b.Header.TxRoot) || string(GetTxinitTreeRoot(b.TxinitBody)) != string(b.Header.TxinitRoot) {
		fmt.Println("the transaction root is wrong")
		return errors.New("the transaction root is wrong")
	}
	return nil
}

/* func (bc *BlockChain) AddTransientAccount(ac string, as core.AccountState, miner int32) {
fmt.Printf("The len of accounts is %d, now adding the accounts\n", len(ac))
bh := &core.BlockHeader{
	ParentBlockHash: bc.CurrentBlock.Header.ParentBlockHash,
	Number:          bc.CurrentBlock.Header.Number,
	Time:            bc.CurrentBlock.Header.Time,
	Bloom:           bc.CurrentBlock.Header.Bloom,
	StateRoot:       bc.CurrentBlock.Header.StateRoot,
	TxRoot:          bc.CurrentBlock.Header.TxRoot,
}

// handle transactions to build root
rt := bc.CurrentBlock.Header.StateRoot
if len(ac) != 0 {
	st, err := trie.New(trie.TrieID(common.BytesToHash(bc.CurrentBlock.Header.StateRoot)), bc.triedb)
	if err != nil {
		log.Panic(err)
	}
	if bc.Get_PartitionMap(ac) == bc.ChainConfig.ShardID {
		new_state := &core.AccountState{
			Balance: params.Init_Balance,
			Nonce:   as.Nonce,
		}
		st.Update([]byte(ac), new_state.Encode())
	}
	/*
		for i, addr := range ac {
			if bc.Get_PartitionMap(addr) == bc.ChainConfig.ShardID {
				new_state := &core.AccountState{
					Balance: as[i].Balance,
					Nonce:   as[i].Nonce,
				}
				st.Update([]byte(addr), new_state.Encode())
			}
		}
*/
/*
		rrt, ns := st.Commit(false)

		// if `ns` is nil, the `err = bc.triedb.Update(trie.NewWithNodeSet(ns))` will report an error.
		if ns != nil {
			err = bc.triedb.Update(trie.NewWithNodeSet(ns))
			if err != nil {
				log.Panic(err)
			}
			err = bc.triedb.Commit(rrt, false)
			if err != nil {
				log.Panic(err)
			}
			rt = rrt.Bytes()
		}
	}
	bh.StateRoot = rt
	bh.Miner = 0

	bc.CurrentBlock.Header = bh
	bc.CurrentBlock.IsAdded = true
	bc.CurrentBlock.Hash = bc.CurrentBlock.Header.Hash()

	fmt.Println(bc.CurrentBlock.Header.ParentBlockHash)
	fmt.Println(bc.CurrentBlock.Hash)

	bc.Storage.UpdateNewestBlock(bc.CurrentBlock.Hash)
}
*/

// 仅修改状态树
/* func (bc *BlockChain) AddTransientAccount(ac string, as core.AccountState, miner int32) {
	fmt.Printf("Adding transient account: %s\n", ac)

	// 直接使用现有状态树进行更新
	if len(ac) != 0 {
		st, err := trie.New(trie.TrieID(common.BytesToHash(bc.CurrentBlock.Header.StateRoot)), bc.triedb)
		if err != nil {
			log.Panic(err)
		}

		// 检查账户是否属于当前分片
		if bc.Get_PartitionMap(ac) == bc.ChainConfig.ShardID {
			new_state := &core.AccountState{
				Balance: params.Init_Balance,
				Nonce:   as.Nonce,
			}
			st.Update([]byte(ac), new_state.Encode())

			// 提交状态树更改
			rrt, ns := st.Commit(false)
			if ns != nil {
				err = bc.triedb.Update(trie.NewWithNodeSet(ns))
				if err != nil {
					log.Panic(err)
				}
				err = bc.triedb.Commit(rrt, false)
				if err != nil {
					log.Panic(err)
				}

				// 只更新当前块的状态根
				bc.CurrentBlock.Header.StateRoot = rrt.Bytes()
			}
		}
	}
} */

// 修改当前块的哈希
func (bc *BlockChain) AddTransientAccount(ac string, as core.AccountState, miner int32) {
	fmt.Printf("Adding transient account: %s\n", ac)

	// 直接使用现有状态树进行更新
	if len(ac) != 0 {
		st, err := trie.New(trie.TrieID(common.BytesToHash(bc.CurrentBlock.Header.StateRoot)), bc.triedb)
		if err != nil {
			log.Panic(err)
		}

		// 检查账户是否属于当前分片
		if bc.Get_PartitionMap(ac) == bc.ChainConfig.ShardID {
			new_state := &core.AccountState{
				Balance: as.Balance,
				Nonce:   as.Nonce,
			}
			st.Update([]byte(ac), new_state.Encode())

			// 提交状态树更改
			rrt, ns := st.Commit(false)
			if ns != nil {
				err = bc.triedb.Update(trie.NewWithNodeSet(ns))
				if err != nil {
					log.Panic(err)
				}
				err = bc.triedb.Commit(rrt, false)
				if err != nil {
					log.Panic(err)
				}

				// 更新当前块的状态根
				bc.CurrentBlock.Header.StateRoot = rrt.Bytes()
				// 标记当前块已被修改
				// bc.CurrentBlock.IsAdded = true
				// 重新计算当前块的哈希
				// bc.CurrentBlock.Hash = bc.CurrentBlock.Header.Hash()
				// newb.Header.ParentBlockHash = bc.CurrentBlock.Hash
			}
		}
	}
}

// add accounts
func (bc *BlockChain) AddAccounts(ac []string, as []*core.AccountState, miner int32) {
	fmt.Printf("The len of accounts is %d, now adding the accounts\n", len(ac))

	bh := &core.BlockHeader{
		ParentBlockHash: bc.CurrentBlock.Hash,
		Number:          bc.CurrentBlock.Header.Number + 1,
		Time:            time.Time{},
	}
	// handle transactions to build root
	rt := bc.CurrentBlock.Header.StateRoot
	if len(ac) != 0 {
		st, err := trie.New(trie.TrieID(common.BytesToHash(bc.CurrentBlock.Header.StateRoot)), bc.triedb)
		if err != nil {
			log.Panic(err)
		}
		for i, addr := range ac {
			if bc.Get_PartitionMap(addr) == bc.ChainConfig.ShardID {
				new_state := &core.AccountState{
					Balance: as[i].Balance,
					Nonce:   as[i].Nonce,
				}
				st.Update([]byte(addr), new_state.Encode())
			}
		}

		rrt, ns := st.Commit(false)

		// if `ns` is nil, the `err = bc.triedb.Update(trie.NewWithNodeSet(ns))` will report an error.
		if ns != nil {
			err = bc.triedb.Update(trie.NewWithNodeSet(ns))
			if err != nil {
				log.Panic(err)
			}
			err = bc.triedb.Commit(rrt, false)
			if err != nil {
				log.Panic(err)
			}
			rt = rrt.Bytes()
		}
	}

	emptyTxs := make([]*core.Transaction, 0)
	emptyTxinits := make([]*core.TxinitTransaction, 0)
	bh.StateRoot = rt
	bh.TxRoot = GetTxTreeRoot(emptyTxs)
	bh.TxinitRoot = GetTxinitTreeRoot(emptyTxinits)
	bh.Bloom = *GetBloomFilter(emptyTxs)
	bh.Miner = 0
	b := core.NewBlock(bh, emptyTxs, emptyTxinits)
	b.Hash = b.Header.Hash()
	fmt.Println(b.Header.ParentBlockHash)
	fmt.Println(bc.CurrentBlock.Hash)

	bc.CurrentBlock = b
	bc.Storage.AddBlock(b)
}

// fetch accounts
func (bc *BlockChain) FetchAccounts(addrs []string) []*core.AccountState {
	res := make([]*core.AccountState, 0)
	st, err := trie.New(trie.TrieID(common.BytesToHash(bc.CurrentBlock.Header.StateRoot)), bc.triedb)
	if err != nil {
		log.Panic(err)
	}
	for _, addr := range addrs {
		asenc, _ := st.Get([]byte(addr))
		var state_a *core.AccountState
		if asenc == nil {
			ib := new(big.Int)
			ib.Add(ib, params.Init_Balance)
			state_a = &core.AccountState{
				Nonce:   uint64(0),
				Balance: ib,
			}
			fmt.Print("还没注入好呢宝贝\n\n\n\n")
		} else {
			state_a = core.DecodeAS(asenc)
		}
		res = append(res, state_a)
	}
	return res
}

// close a blockChain, close the database inferfaces
func (bc *BlockChain) CloseBlockChain() {
	bc.Storage.DataBase.Close()
	bc.triedb.CommitPreimages()
	bc.db.Close()
}

// print the details of a blockchain
func (bc *BlockChain) PrintBlockChain() string {
	vals := []interface{}{
		bc.CurrentBlock.Header.Number,
		bc.CurrentBlock.Hash,
		bc.CurrentBlock.Header.StateRoot,
		bc.CurrentBlock.Header.Time,
		bc.triedb,
		// len(bc.Txpool.RelayPool[1]),
	}
	res := fmt.Sprintf("%v\n", vals)
	fmt.Println(res)
	return res
}

// 存放当前修改过的账户地址分布
func (bc *BlockChain) AddDirtyMap(key string, val uint64) {
	bc.dmlock.Lock()
	defer bc.dmlock.Unlock()
	bc.DirtyMap[key] = val
}
