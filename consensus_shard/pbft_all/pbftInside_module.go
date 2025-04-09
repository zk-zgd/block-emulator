// addtional module for new consensus
package pbft_all

import (
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"
)

// simple implementation of pbftHandleModule interface ...
// only for block request and use transaction relay
type RawRelayPbftExtraHandleMod struct {
	tdm      *dataSupport.Data_supportTransientAccount
	pbftNode *PbftConsensusNode
	// pointer to pbft data
}

// propose request with different types
func (rphm *RawRelayPbftExtraHandleMod) HandleinPropose() (bool, *message.Request) {
	//rphm.pbftNode.pl.Plog.Print("卡在这里了1\n\n\n\n")
	//rphm.pbftNode.notifyPoolLock.Lock()
	//rphm.pbftNode.pl.Plog.Print("卡在这里了2\n\n\n\n")
	/*
		if len(rphm.pbftNode.notifyPool) != 0 {
			rphm.tdm.TransientOn = true
			for _, notify := range rphm.pbftNode.notifyPool {
				// 处理通知
				rphm.pbftNode.CurChain.Update_PartitionMap(notify.TransientTxAddr, rphm.pbftNode.ShardID)
				// rphm.pbftNode.CurChain.AddTransientAccount(notify.TransientTxAddr, notify.MigAccount_State, 0)
				// 此处创建txinit交易
				rphm.pbftNode.New_TxInit(notify.TransientTxAddr)
				rphm.pbftNode.pl.Plog.Print("创建成功了xd，现在检查一下状态\n\n")
				rphm.pbftNode.pl.Plog.Print(rphm.pbftNode.CurChain.FetchAccounts([]string{notify.TransientTxAddr})[0])
				rphm.pbftNode.pl.Plog.Print("状态检查结束\n\n")
			}

		}*/
	/*
		if rphm.tdm.TransientOn {
			rphm.sendTransientaccountcreateready()
			for !rphm.getTransientAccountCreateReady() {
				time.Sleep(time.Second)
			}
			// 这里不直接发送交易，等到最后再发送
			// propose a partition
			for !rphm.getTransientAccountCreateCollectOver() {
				time.Sleep(time.Second)
			}

			return rphm.proposeTransientAccountCreate()
		}
	*/
	//rphm.pbftNode.notifyPoolLock.Unlock()
	//rphm.pbftNode.pl.Plog.Print("卡在这里了3\n\n\n\n")

	//rphm.pbftNode.pl.Plog.Print("现在是提案阶段，生成新块之前检查notifypool\n\n")
	// check notify pool
	/*
		rphm.pbftNode.notifyPoolLock.Lock() // 通知池的锁
		if len(rphm.pbftNode.notifyPool) != 0 {
			// 修改currentblock的哈希
			for _, notify := range rphm.pbftNode.notifyPool {
				// 处理通知

				rphm.pbftNode.CurChain.Update_PartitionMap(notify.TransientTxAddr, rphm.pbftNode.ShardID)
				rphm.pbftNode.CurChain.AddTransientAccount(notify.TransientTxAddr, notify.MigAccount_State, 0)
				// 此处创建txinit交易
				rphm.pbftNode.New_TxInit(notify.TransientTxAddr)
				rphm.pbftNode.pl.Plog.Print("创建成功了xd，现在检查一下状态\n\n")
				rphm.pbftNode.pl.Plog.Print(rphm.pbftNode.CurChain.FetchAccounts([]string{notify.TransientTxAddr})[0])
				rphm.pbftNode.pl.Plog.Print("状态检查结束\n\n")
			}
		}
		rphm.pbftNode.notifyPool = make([]message.TxinitCreate, 0)
		rphm.pbftNode.notifyPoolLock.Unlock() // 通知池的锁

		// 检查一下当前块的哈希有没有变
		if !bytes.Equal(rphm.pbftNode.CurChain.CurrentBlock.Hash, rphm.pbftNode.CurChain.CurrentBlock.Header.Hash()) {
			rphm.pbftNode.pl.Plog.Print("出错啦，当前块的哈希变了！修改一下哈希\n\n")
			rphm.pbftNode.CurChain.CurrentBlock.Hash = rphm.pbftNode.CurChain.CurrentBlock.Header.Hash()
			rphm.pbftNode.pl.Plog.Print("修改好了，输出一下哈希：\n")
			rphm.pbftNode.pl.Plog.Println(rphm.pbftNode.CurChain.CurrentBlock.Hash)
		}
		rphm.pbftNode.CurChain.Storage.UpdateNewestBlock(rphm.pbftNode.CurChain.CurrentBlock.Hash)
	*/
	// new blocks
	if len(rphm.pbftNode.notifyPool) != 0 {
		rphm.pbftNode.pl.Plog.Print("当前notify监听池中有通知, 这里将进行添加账户操作！\n\n")
		// rphm.tdm.TransientOn = true
	}
	if rphm.tdm.TransientOn {
		rphm.sendTransientaccountcreateready()
		for !rphm.getTransientAccountCreateReady() {
			time.Sleep(time.Second)
		}
		// 这里不直接发送交易，等到最后再发送
		// 收集交易
		rphm.getTxsRelated()
		// propose a partition
		for !rphm.getTransientAccountCreateCollectOver() {
			time.Sleep(time.Second)
		}

		return rphm.proposeTransientAccountCreate()
	}
	/*



	 */
	block := rphm.pbftNode.CurChain.GenerateBlock(int32(rphm.pbftNode.NodeID))
	// 检查一下当前块里有无txinit交易
	if len(block.TxinitBody) != 0 {
		rphm.pbftNode.pl.Plog.Print("当前块里有txinit交易，输出一下txinit交易\n\n")
		for _, txinit := range block.TxinitBody {
			rphm.pbftNode.pl.Plog.Println(txinit)
			rphm.pbftNode.pl.Plog.Print("\n\n")
		}
	}
	// 添加 TxInit 交易时重新计算根
	/*
		if len(block.TxinitBody) > 0 {
			txinitRoot := chain.GetTxinitTreeRoot(block.TxinitBody)
			fmt.Print("生成新块时的 TxInit 根:\n")
			fmt.Println(txinitRoot)
			block.Header.TxinitRoot = txinitRoot
			block.Hash = block.Header.Hash()
		}
	*/
	// rphm.pbftNode.newblockTemp = block
	rphm.pbftNode.pl.Plog.Printf("S%dN%d : 新块已经生成啦，副本也保存啦, 块高是 = %d\n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID, block.Header.Number)
	/*
		rphm.pbftNode.pl.Plog.Printf("当前块高currentblockheight是 ：%d\n", rphm.pbftNode.CurChain.CurrentBlock.Header.Number)
		rphm.pbftNode.pl.Plog.Printf("当前新块高度newblockheight是 ：%d\n\n\n", block.Header.Number)
	*/
	r := &message.Request{
		RequestType: message.BlockRequest,
		ReqTime:     time.Now(),
	}
	r.Msg.Content = block.Encode()

	// 检查交易根
	if !bytes.Equal(params.Changetxinitroot, block.Header.TxinitRoot) {
		rphm.pbftNode.pl.Plog.Print("提交结束阶段交易txinit根就错了\n\n\n\n")
	}
	return true, r
}

// the DIY operation in preprepare
func (rphm *RawRelayPbftExtraHandleMod) HandleinPrePrepare(ppmsg *message.PrePrepare) bool {

	// rphm.pbftNode.notifyPoolLock.Lock()

	// rphm.pbftNode.notifyPool = make([]message.TxinitCreate, 0)
	// rphm.pbftNode.notifyPoolLock.Unlock()
	/*
		b := core.DecodeB(ppmsg.RequestMsg.Msg.Content)
		rphm.pbftNode.pl.Plog.Print("输出一下当前新块的哈希:\n")
		rphm.pbftNode.pl.Plog.Println(b.Header.ParentBlockHash)
		rphm.pbftNode.pl.Plog.Print("输出一下父哈希的哈希值:\n")
		rphm.pbftNode.pl.Plog.Println(rphm.pbftNode.CurChain.CurrentBlock.Hash)
		rphm.pbftNode.pl.Plog.Print("\n\n\n\n")
	*/
	/*
		if rphm.pbftNode.CurChain.IsValidBlock(core.DecodeB(ppmsg.RequestMsg.Msg.Content)) == errors.New("修改一下当前块的哈希") {
			b := core.DecodeB(ppmsg.RequestMsg.Msg.Content)
			rphm.pbftNode.pl.Plog.Println(b.Header.ParentBlockHash)
			rphm.pbftNode.pl.Plog.Println(rphm.pbftNode.CurChain.CurrentBlock.Hash)
			rphm.pbftNode.pl.Plog.Printf("S%dN%d : the pre-prepare message is correct, putting it into the RequestPool. \n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID)
			rphm.pbftNode.requestPool[string(ppmsg.Digest)] = ppmsg.RequestMsg
			return true
		}
	*/

	isTransientAccountCreateReq := ppmsg.RequestMsg.RequestType == message.TransientAccountCreateReq

	if isTransientAccountCreateReq {
		rphm.pbftNode.pl.Plog.Printf("S%dN%d : a TransientAccountCreate block\n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID)
		rphm.pbftNode.pl.Plog.Printf("S%dN%d : the pre-prepare message is correct, putting it into the RequestPool. \n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID)
		rphm.pbftNode.requestPool[string(ppmsg.Digest)] = ppmsg.RequestMsg
		return true
	}
	/*
		rphm.pbftNode.pl.Plog.Print("现在是预准备阶段，预准备之前检查notifypool\n\n")
		if len(rphm.pbftNode.notifyPool) != 0 && rphm.pbftNode.NodeID == 0 {
			for _, notify := range rphm.pbftNode.notifyPool {
				// 处理通知
				rphm.pbftNode.CurChain.Update_PartitionMap(notify.TransientTxAddr, rphm.pbftNode.ShardID)
				rphm.pbftNode.CurChain.AddTransientAccount(notify.TransientTxAddr, notify.MigAccount_State, 0)
				// 此处创建txinit交易
				rphm.pbftNode.New_TxInit(notify.TransientTxAddr)
				rphm.pbftNode.pl.Plog.Print("创建成功了xd，现在检查一下状态\n\n")
				rphm.pbftNode.pl.Plog.Print(rphm.pbftNode.CurChain.FetchAccounts([]string{notify.TransientTxAddr})[0])
				rphm.pbftNode.pl.Plog.Print("状态检查结束\n\n")
			}
		}
		if !bytes.Equal(rphm.pbftNode.CurChain.CurrentBlock.Hash, rphm.pbftNode.CurChain.CurrentBlock.Header.Hash()) {
			rphm.pbftNode.CurChain.CurrentBlock.Hash = rphm.pbftNode.CurChain.CurrentBlock.Header.Hash()
		}
		rphm.pbftNode.newblockTemp.Header.ParentBlockHash = rphm.pbftNode.CurChain.CurrentBlock.Hash
		rphm.pbftNode.newblockTemp.Hash = rphm.pbftNode.newblockTemp.Header.Hash()


		if !bytes.Equal(params.Changetxinitroot, b.Header.TxinitRoot) {
			rphm.pbftNode.pl.Plog.Print("提交结束阶段交易txinit根就错了\n\n\n\n")
		}
	*/
	// 现在验证临时块ovo-chx
	b := core.DecodeB(ppmsg.RequestMsg.Msg.Content)
	if rphm.pbftNode.CurChain.IsValidBlock(b) != nil {
		rphm.pbftNode.pl.Plog.Printf("S%dN%d : not a valid block\n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID)
		return false
	}
	rphm.pbftNode.pl.Plog.Printf("S%dN%d : the pre-prepare message is correct, putting it into the RequestPool. \n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID)
	rphm.pbftNode.requestPool[string(ppmsg.Digest)] = ppmsg.RequestMsg
	// merge to be a prepare message
	/*
		if !bytes.Equal(params.Changetxinitroot, rphm.pbftNode.newblockTemp.Header.TxinitRoot) {
			rphm.pbftNode.pl.Plog.Print("预准备阶段交易txinit根就错了\n\n\n\n")
		}*/
	return true
}

// the operation in prepare, and in pbft + tx relaying, this function does not need to do any.
func (rphm *RawRelayPbftExtraHandleMod) HandleinPrepare(pmsg *message.Prepare) bool {

	if rphm.tdm.TransientOn {
		fmt.Println("No operations are performed in Extra handle mod")
		return true
	}
	/*
		// rphm.pbftNode.notifyPoolLock.Lock()
		rphm.pbftNode.pl.Plog.Printf("现在是准备阶段，要进行prepare操作，首先进行notifypool检查啦！\n\n")
		if len(rphm.pbftNode.notifyPool) != 0 && rphm.pbftNode.NodeID == 0 {
			for _, notify := range rphm.pbftNode.notifyPool {
				// 处理通知
				rphm.pbftNode.CurChain.Update_PartitionMap(notify.TransientTxAddr, rphm.pbftNode.ShardID)
				rphm.pbftNode.CurChain.AddTransientAccount(notify.TransientTxAddr, notify.MigAccount_State, 0)
				// 此处创建txinit交易
				rphm.pbftNode.New_TxInit(notify.TransientTxAddr)
				rphm.pbftNode.pl.Plog.Print("创建成功了xd，现在检查一下状态\n\n")
				rphm.pbftNode.pl.Plog.Print(rphm.pbftNode.CurChain.FetchAccounts([]string{notify.TransientTxAddr})[0])
				rphm.pbftNode.pl.Plog.Print("状态检查结束\n\n")
			}
		}

		if !bytes.Equal(rphm.pbftNode.CurChain.CurrentBlock.Hash, rphm.pbftNode.CurChain.CurrentBlock.Header.Hash()) {
			rphm.pbftNode.CurChain.CurrentBlock.Hash = rphm.pbftNode.CurChain.CurrentBlock.Header.Hash()
		}
		rphm.pbftNode.newblockTemp.Header.ParentBlockHash = rphm.pbftNode.CurChain.CurrentBlock.Hash
		rphm.pbftNode.newblockTemp.Hash = rphm.pbftNode.newblockTemp.Header.Hash()
		rphm.pbftNode.newblockTemp.Header.ParentBlockHash = rphm.pbftNode.CurChain.CurrentBlock.Hash

		rphm.pbftNode.notifyPool = make([]message.TxinitCreate, 0)
		// rphm.pbftNode.notifyPoolLock.Unlock()
	*/
	fmt.Println("No operations are performed in Extra handle mod")
	/*
		if !bytes.Equal(params.Changetxinitroot, rphm.pbftNode.newblockTemp.Header.TxinitRoot) {
			rphm.pbftNode.pl.Plog.Print("准备阶段交易txinit根就错了\n\n\n\n")
		}*/
	return true
}

// the operation in commit.
func (rphm *RawRelayPbftExtraHandleMod) HandleinCommit(cmsg *message.Commit) bool {

	r := rphm.pbftNode.requestPool[string(cmsg.Digest)]
	if r.RequestType == message.TransientAccountCreateReq {
		// if a partition Requst ...
		atm := message.DecodeAccountTransferMsg(r.Msg.Content)

		rphm.TransientAccountCreate_do(atm)
		return true
	}
	// requestType ...
	block := core.DecodeB(r.Msg.Content)
	// 此处检查通知池，每个阶段都要检查一次
	/*
		if !bytes.Equal(params.Changetxinitroot, block.Header.TxinitRoot) {
			rphm.pbftNode.pl.Plog.Print("提交结束阶段交易txinit根就错了\n\n\n\n")
		}
	*/
	/*
		rphm.pbftNode.notifyPoolLock.Lock()
		rphm.pbftNode.pl.Plog.Print("现在是commit阶段，提交之前检查notifypool\n\n")
		if len(rphm.pbftNode.notifyPool) != 0 && rphm.pbftNode.NodeID == 0 {
			for _, notify := range rphm.pbftNode.notifyPool {
				// 处理通知
				rphm.pbftNode.CurChain.Update_PartitionMap(notify.TransientTxAddr, rphm.pbftNode.ShardID)
				rphm.pbftNode.CurChain.AddTransientAccount(notify.TransientTxAddr, notify.MigAccount_State, 0)
				// 此处创建txinit交易
				rphm.pbftNode.New_TxInit(notify.TransientTxAddr)
				rphm.pbftNode.pl.Plog.Print("创建成功了xd，现在检查一下状态\n\n")
				rphm.pbftNode.pl.Plog.Print(rphm.pbftNode.CurChain.FetchAccounts([]string{notify.TransientTxAddr})[0])
				rphm.pbftNode.pl.Plog.Print("状态检查结束\n\n")
			}
		}
		if !bytes.Equal(rphm.pbftNode.CurChain.CurrentBlock.Hash, rphm.pbftNode.CurChain.CurrentBlock.Header.Hash()) {
			rphm.pbftNode.CurChain.CurrentBlock.Hash = rphm.pbftNode.CurChain.CurrentBlock.Header.Hash()
		}
		rphm.pbftNode.newblockTemp.Header.ParentBlockHash = rphm.pbftNode.CurChain.CurrentBlock.Hash
		rphm.pbftNode.newblockTemp.Hash = rphm.pbftNode.newblockTemp.Header.Hash()
		rphm.pbftNode.newblockTemp.Header.ParentBlockHash = rphm.pbftNode.CurChain.CurrentBlock.Hash

		rphm.pbftNode.notifyPool = make([]message.TxinitCreate, 0)
		rphm.pbftNode.notifyPoolLock.Unlock()
	*/
	/*
		if !bytes.Equal(block.Header.ParentBlockHash, rphm.pbftNode.CurChain.CurrentBlock.Hash) {
			if rphm.pbftNode.CurChain.CurrentBlock.IsAdded {
				block.Header.ParentBlockHash = rphm.pbftNode.CurChain.CurrentBlock.Hash
				block.Hash = block.Header.Hash()
				// rphm.pbftNode.CurChain.CurrentBlock.IsAdded = false
				rphm.pbftNode.CurChain.CurrentBlock.Hash = rphm.pbftNode.CurChain.CurrentBlock.Header.Hash()
				rphm.pbftNode.pl.Plog.Print("修改好了，输出一下哈希：\n")
				rphm.pbftNode.pl.Plog.Println(block.Header.ParentBlockHash)
				rphm.pbftNode.pl.Plog.Println(rphm.pbftNode.CurChain.CurrentBlock.Hash)

			} else {
				rphm.pbftNode.pl.Plog.Print("出错了咩，当前块头和父块哈希不一样咩~\n\n")
			}
		}
	*/
	// 验证一下当前新块的哈希
	/*
		if !bytes.Equal(block.Header.ParentBlockHash, rphm.pbftNode.CurChain.CurrentBlock.Hash) {
			rphm.pbftNode.pl.Plog.Print("出错啦，当前块的哈希变了！修改一下哈希\n\n")
			block.Header.ParentBlockHash = rphm.pbftNode.CurChain.CurrentBlock.Hash
			block.Hash = block.Header.Hash()
			rphm.pbftNode.pl.Plog.Print("修改好了，输出一下哈希：\n")
			rphm.pbftNode.pl.Plog.Println(block.Header.ParentBlockHash)
			rphm.pbftNode.pl.Plog.Println(rphm.pbftNode.CurChain.CurrentBlock.Hash)
		}
	*/
	/*
		if !bytes.Equal(block.Hash, rphm.pbftNode.newblockTemp.Hash) {
			rphm.pbftNode.pl.Plog.Print("出错啦，当前块的哈希变了！修改一下哈希\n\n")
			block = rphm.pbftNode.newblockTemp
			rphm.pbftNode.pl.Plog.Print("修改好了，输出一下哈希：\n")
			rphm.pbftNode.pl.Plog.Println(block.Hash)
		}*/

	// 如果有多个节点，此处要进行通知
	/************************************************************************************/
	// TODO-chx: 通知其他节点

	// check the block
	// rphm.pbftNode.newblockTemp.Header.ParentBlockHash = rphm.pbftNode.CurChain.CurrentBlock.Hash

	rphm.pbftNode.pl.Plog.Printf("S%dN%d : adding the block %d...now height = %d \n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID, block.Header.Number, rphm.pbftNode.CurChain.CurrentBlock.Header.Number)
	rphm.pbftNode.CurChain.AddBlock(block)
	rphm.pbftNode.pl.Plog.Printf("S%dN%d : added the block %d... \n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID, block.Header.Number)
	rphm.pbftNode.CurChain.PrintBlockChain()

	// now try to relay txs to other shards (for main nodes)
	if rphm.pbftNode.NodeID == uint64(rphm.pbftNode.view.Load()) {
		rphm.pbftNode.pl.Plog.Printf("S%dN%d : main node is trying to send relay txs at height = %d \n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID, block.Header.Number)
		// generate relay pool and collect txs excuted
		rphm.pbftNode.CurChain.Txpool.RelayPool = make(map[uint64][]*core.Transaction)
		interShardTxs := make([]*core.Transaction, 0)
		relay1Txs := make([]*core.Transaction, 0)
		relay2Txs := make([]*core.Transaction, 0)
		txinits1 := make([]*core.TxinitTransaction, 0)
		txinits2 := make([]*core.TxinitTransaction, 0)
		for _, tx := range block.Body {
			ssid := rphm.pbftNode.CurChain.Get_PartitionMap(tx.Sender)
			rsid := rphm.pbftNode.CurChain.Get_PartitionMap(tx.Recipient)
			if tx.Is_Txreq {
				rphm.pbftNode.pl.Plog.Printf("本分片节点收到了迁移计划请求,我是分片%d，节点%d\n\n\n\n\n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID)
				rsid = uint64(params.ShardNum) - 1 // 3 is the supervisor shard
			}

			// if tx.Is_Txreq && rphm.pbftNode.ShardID == uint64(params.ShardNum)-1 {
			// rphm.pbftNode.Informallshardcreatetransientaccount()
			// }

			if !tx.Relayed && ssid != rphm.pbftNode.ShardID && tx.Is_Txreq {
				log.Print("\n\ntx.relayed： ")
				log.Println(tx.Relayed)
				log.Print("\n\nssid: ")
				log.Println(ssid)
				log.Print("\n\nshardid: ")
				log.Println(rphm.pbftNode.ShardID)
				log.Panic("incorrect tx1 tx.relayed + shardid")
			}

			if !tx.Relayed && ssid != rphm.pbftNode.ShardID {
				tmsg := new(message.TxRedirectMsg)
				tmsg.Txs2DestinationShard = make([]*core.Transaction, 0)
				tmsg.Txs2DestinationShard = append(tmsg.Txs2DestinationShard, tx)
				tbyte, err1 := json.Marshal(tmsg)
				if err1 != nil {
					log.Panic(err1)
				}
				mmsg := message.MergeMessage(message.CTxRedirectout, tbyte)
				networks.TcpDial(mmsg, rphm.pbftNode.ip_nodeTable[ssid][0])
				// log.panic()
			}

			if tx.Relayed && rsid != rphm.pbftNode.ShardID {
				// log.panic()
				tmsg := new(message.TxRedirectMsg)
				tmsg.Txs2DestinationShard = make([]*core.Transaction, 0)
				tmsg.Txs2DestinationShard = append(tmsg.Txs2DestinationShard, tx)
				tbyte, err1 := json.Marshal(tmsg)
				if err1 != nil {
					log.Panic(err1)
				}
				mmsg := message.MergeMessage(message.CTxRedirectout, tbyte)
				networks.TcpDial(mmsg, rphm.pbftNode.ip_nodeTable[rsid][0])
			}
			if rsid != rphm.pbftNode.ShardID {
				relay1Txs = append(relay1Txs, tx)
				tx.Relayed = true
				rphm.pbftNode.CurChain.Txpool.AddRelayTx(tx, rsid)
			} else {
				if tx.Relayed {
					relay2Txs = append(relay2Txs, tx)
				} else {
					interShardTxs = append(interShardTxs, tx)
				}
			}
		}

		// send relay txs
		if params.RelayWithMerkleProof == 1 {
			rphm.pbftNode.RelayWithProofSend(block)
		} else {
			rphm.pbftNode.RelayMsgSend()
		}
		rphm.pbftNode.pl.Plog.Print("开始处理txinit交易啦~，当前已经运行过addblock啦，现在是中继relaytxinit\n\n\n")
		for _, txinit := range block.TxinitBody {
			if txinit.DestinationshardID == rphm.pbftNode.ShardID {
				rphm.pbftNode.pl.Plog.Printf("现在是提交阶段，进行txinit的处理，加入txinitrelay交易池中\n\n\n")
				// 当前迁移目标分片等于当前分片ID 并且当前节点是0
				txinits1 = append(txinits1, txinit)
				for i := 0; i < params.ShardNum; i++ {
					if txinit.DestinationshardID != uint64(i) {
						txinits2 = append(txinits2, txinit)
						rphm.pbftNode.CurChain.Txinitpool.AddRelayTxInit(txinit, uint64(i))
					} else {
						continue
					}
				}

			} else {
				// rphm.pbftNode.CurChain.Update_PartitionMap(txinit.TransientAccountAddr, txinit.DestinationshardID)
			}

		}
		// 添加txinitrelay处理啦-chx
		rphm.pbftNode.TxinitRelayMsgSend(block)

		// send txs excuted in this block to the listener
		// add more message to measure more metrics
		bim := message.BlockInfoMsg{
			BlockBodyLength: len(block.Body),
			InnerShardTxs:   interShardTxs,
			Epoch:           0,

			Relay1Txs: relay1Txs,
			Relay2Txs: relay2Txs,

			SenderShardID: rphm.pbftNode.ShardID,
			ProposeTime:   r.ReqTime,
			CommitTime:    time.Now(),
			// 读取数据库所用的两个数据
			ConsensusRound: params.ConsensusRoundForEvrShard[rphm.pbftNode.ShardID],
			NowShardID:     rphm.pbftNode.ShardID,
		}
		bByte, err := json.Marshal(bim)
		if err != nil {
			log.Panic()
		}
		msg_send := message.MergeMessage(message.CBlockInfo, bByte)
		go networks.TcpDial(msg_send, rphm.pbftNode.ip_nodeTable[params.SupervisorShard][0])
		rphm.pbftNode.pl.Plog.Printf("S%dN%d : sended excuted txs\n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID)
		rphm.pbftNode.CurChain.Txpool.GetLocked()
		metricName := []string{
			"Block Height",
			"EpochID of this block",
			"TxPool Size",
			"# of all Txs in this block",
			"# of Relay1 Txs in this block",
			"# of Relay2 Txs in this block",
			"TimeStamp - Propose (unixMill)",
			"TimeStamp - Commit (unixMill)",

			"SUM of confirm latency (ms, All Txs)",
			"SUM of confirm latency (ms, Relay1 Txs) (Duration: Relay1 proposed -> Relay1 Commit)",
			"SUM of confirm latency (ms, Relay2 Txs) (Duration: Relay1 proposed -> Relay2 Commit)",
		}
		metricVal := []string{
			strconv.Itoa(int(block.Header.Number)),
			strconv.Itoa(bim.Epoch),
			strconv.Itoa(len(rphm.pbftNode.CurChain.Txpool.TxQueue)),
			strconv.Itoa(len(block.Body)),
			strconv.Itoa(len(relay1Txs)),
			strconv.Itoa(len(relay2Txs)),
			strconv.FormatInt(bim.ProposeTime.UnixMilli(), 10),
			strconv.FormatInt(bim.CommitTime.UnixMilli(), 10),

			strconv.FormatInt(computeTCL(block.Body, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(relay1Txs, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(relay2Txs, bim.CommitTime), 10),
		}
		rphm.pbftNode.writeCSVline(metricName, metricVal)
		rphm.pbftNode.CurChain.Txpool.GetUnlocked()
	}

	// 将当前分片的块哈希发送给supervisor节点，注明分片号-chx
	// 每10个共识轮发送一次
	/* 	if params.ConsensusRoundForEvrShard[rphm.pbftNode.ShardID] > 0 && (params.ConsensusRoundForEvrShard[rphm.pbftNode.ShardID])%10 == 0 {
	   		msg2supervisor := message.Msg2Supervisor{
	   			ShardID: rphm.pbftNode.ShardID,
	   			NodeID:  rphm.pbftNode.NodeID,
	   		}

	   		nowhash := block.Header.ParentBlockHash

	   		var err error
	   		for i := 0; i < 10; i++ {
	   			block, err = rphm.pbftNode.CurChain.Storage.GetBlock(nowhash)
	   			msg2supervisor.Block = append(msg2supervisor.Block, block)

	   			nowhash = block.Header.ParentBlockHash
	   			rphm.pbftNode.pl.Plog.Print("当前块的哈希：\n\n\n\n")
	   			rphm.pbftNode.pl.Plog.Println(nowhash)
	   			rphm.pbftNode.pl.Plog.Print("当前块高是: \n\n\n")
	   			rphm.pbftNode.pl.Plog.Println(block.Header.Number)
	   		}
	   		bytes, err := json.Marshal(msg2supervisor)
	   		if err != nil {
	   			log.Panic()
	   		}
	   		waitsend := message.MergeMessage(message.CBlockHash2Supervisor, bytes)
	   		go networks.TcpDial(waitsend, params.SupervisorAddr)
	   	}
	*/
	return true
}

func (rphm *RawRelayPbftExtraHandleMod) HandleReqestforOldSeq(*message.RequestOldMessage) bool {
	fmt.Println("No operations are performed in Extra handle mod")
	return true
}

// the operation for sequential requests
func (rphm *RawRelayPbftExtraHandleMod) HandleforSequentialRequest(som *message.SendOldMessage) bool {
	if int(som.SeqEndHeight-som.SeqStartHeight+1) != len(som.OldRequest) {
		rphm.pbftNode.pl.Plog.Printf("S%dN%d : the SendOldMessage message is not enough\n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID)
	} else { // add the block into the node pbft blockchain
		for height := som.SeqStartHeight; height <= som.SeqEndHeight; height++ {
			r := som.OldRequest[height-som.SeqStartHeight]
			if r.RequestType == message.BlockRequest {
				b := core.DecodeB(r.Msg.Content)
				rphm.pbftNode.CurChain.AddBlock(b)
			}
		}
		rphm.pbftNode.sequenceID = som.SeqEndHeight + 1
		rphm.pbftNode.CurChain.PrintBlockChain()
	}
	return true
}
