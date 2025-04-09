package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"encoding/json"
	"log"
	"time"
)

func (rphm *RawRelayPbftExtraHandleMod) sendTransientaccountcreateready() {
	rphm.tdm.T_ReadyLock.Lock()
	rphm.tdm.TransientReady[rphm.pbftNode.ShardID] = true
	rphm.tdm.T_ReadyLock.Unlock()

	tr := message.TransientAccountCreateReady{
		FromShard: rphm.pbftNode.ShardID,
		NowSeqID:  rphm.pbftNode.sequenceID,
	}
	tByte, err := json.Marshal(tr)
	if err != nil {
		log.Panic()
	}
	send_msg := message.MergeMessage(message.CTransientAccountCreateReady, tByte)
	for sid := 0; sid < int(rphm.pbftNode.pbftChainConfig.ShardNums); sid++ {
		if sid != int(tr.FromShard) {
			go networks.TcpDial(send_msg, rphm.pbftNode.ip_nodeTable[uint64(sid)][0])
		}
	}
	rphm.pbftNode.pl.Plog.Print("Ready for create transientaccount\n")
}

func (rphm *RawRelayPbftExtraHandleMod) getTransientAccountCreateReady() bool {
	rphm.tdm.T_ReadyLock.Lock()
	defer rphm.tdm.T_ReadyLock.Unlock()
	rphm.pbftNode.seqMapLock.Lock()
	defer rphm.pbftNode.seqMapLock.Unlock()
	rphm.tdm.ReadySeqLock.Lock()
	defer rphm.tdm.ReadySeqLock.Unlock()

	flag := true
	for sid, val := range rphm.pbftNode.seqIDMap {
		if rval, ok := rphm.tdm.ReadySeq[sid]; !ok || (rval-1 != val) {
			flag = false
		}
	}
	return len(rphm.tdm.TransientReady) == int(rphm.pbftNode.pbftChainConfig.ShardNums) && flag
}

// fetch collect infos
func (rphm *RawRelayPbftExtraHandleMod) getTransientAccountCreateCollectOver() bool {
	rphm.tdm.CollectLock.Lock()
	defer rphm.tdm.CollectLock.Unlock()
	return rphm.tdm.CollectOver
}

// 提案阶段
func (rphm *RawRelayPbftExtraHandleMod) proposeTransientAccountCreate() (bool, *message.Request) {
	rphm.pbftNode.pl.Plog.Printf("S%dN%d : begin transient account create proposing\n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID)
	// 暂且不处理重新定向交易
	// 这里进行notify中的处理
	// rphm.pbftNode.notifyPoolLock.Lock()
	// 存放交易地址
	if len(rphm.pbftNode.notifyPool) != 0 {
		for _, notify := range rphm.pbftNode.notifyPool {
			rphm.tdm.ReceivedNewAccountState[notify.TransientTxAddr] = &notify.MigAccount_State
			rphm.tdm.TempTime = notify.Nowtime
		}

	}
	rphm.pbftNode.pl.Plog.Printf("需要创建替身账户的notify池子长度是: %d\n\n", len(rphm.tdm.ReceivedNewAccountState))
	// rphm.pbftNode.notifyPoolLock.Unlock()
	rphm.pbftNode.notifyPool = make([]message.TxinitCreate, 0)
	atmaddr := make([]string, 0)
	atmAs := make([]*core.AccountState, 0)
	for key, val := range rphm.tdm.ReceivedNewAccountState {
		atmaddr = append(atmaddr, key)
		atmAs = append(atmAs, val)
	}
	atm := message.AccountTransferMsg{
		// ModifiedMap:  rphm.tdm.ModifiedMap[rphm.tdm.AccountTransferRound],
		Addrs:        atmaddr,
		AccountState: atmAs,
		//ATid:         uint64(len(rphm.tdm.ModifiedMap)),
	}
	atmbyte := atm.Encode()
	r := &message.Request{
		RequestType: message.TransientAccountCreateReq,
		Msg: message.RawMessage{
			Content: atmbyte,
		},
		ReqTime: time.Now(),
	}
	return true, r
}

// all nodes in a shard will do accout Transfer, to sync the state trie
func (rphm *RawRelayPbftExtraHandleMod) TransientAccountCreate_do(atm *message.AccountTransferMsg) {
	// change the partition Map
	cnt := 0
	for key, _ := range rphm.tdm.ReceivedNewAccountState { //
		cnt++
		//暂时先不修改
		rphm.pbftNode.CurChain.Update_PartitionMap(key, rphm.pbftNode.ShardID)
		// print(key)
	}
	rphm.pbftNode.pl.Plog.Printf("%d key-vals are updated\n", cnt)
	// add the account into the state trie
	rphm.pbftNode.pl.Plog.Printf("%d addrs to add\n", len(atm.Addrs))
	rphm.pbftNode.pl.Plog.Printf("%d accountstates to add\n", len(atm.AccountState))
	rphm.pbftNode.CurChain.AddAccounts(atm.Addrs, atm.AccountState, rphm.pbftNode.view.Load())

	rphm.tdm.AccountTransferRound = atm.ATid
	rphm.tdm.AccountStateTx = make(map[uint64]*message.AccountStateAndTx)
	rphm.tdm.ReceivedNewAccountState = make(map[string]*core.AccountState)
	// rphm.tdm.ReceivedNewTx = make([]*core.Transaction, 0)
	rphm.tdm.TransientOn = false

	rphm.tdm.CollectLock.Lock()
	rphm.tdm.CollectOver = false
	rphm.tdm.CollectLock.Unlock()

	rphm.tdm.T_ReadyLock.Lock()
	rphm.tdm.TransientReady = make(map[uint64]bool)
	rphm.tdm.T_ReadyLock.Unlock()

	for _, addr := range atm.Addrs {
		rphm.pbftNode.pl.Plog.Print("添加init交易完成\n")
		rphm.pbftNode.New_TxInit(addr)
		// rphm.pbftNode.CurChain.Update_PartitionMap(addr, rphm.pbftNode.ShardID)
	}
	rphm.pbftNode.CurChain.PrintBlockChain()
}

// send the transactions and the accountState to other leaders
func (rphm *RawRelayPbftExtraHandleMod) getTxsRelated() {
	rphm.pbftNode.CurChain.Txpool.GetLocked()
	// generate accout transfer and txs message

	rphm.pbftNode.pl.Plog.Printf("S%dN%d: start lock transaction!", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID)
	rphm.pbftNode.pl.Plog.Printf("before deleting, The size of tx pool is:%d ", len(rphm.pbftNode.CurChain.Txpool.TxQueue))

	for addr, _ := range rphm.pbftNode.CurChain.DirtyMap {
		for _, tx := range rphm.pbftNode.CurChain.Txpool.TxQueue {
			// 非中继交易且发送者是待迁移账户
			if !tx.Relayed && tx.Sender == addr {
				//rphm.pbftNode.pl.Plog.Println(tx)

				rphm.tdm.ReceivedNewTx[rphm.pbftNode.CurChain.DirtyMap[addr]] = append(rphm.tdm.ReceivedNewTx[rphm.pbftNode.CurChain.DirtyMap[addr]], tx)
				rphm.pbftNode.CurChain.Txpool.TxQueue = DeleteElementsInList(rphm.pbftNode.CurChain.Txpool.TxQueue, []*core.Transaction{tx})
			}
			// 是个中继交易并且接收者是待迁移账户
			if tx.Relayed && tx.Recipient == addr {
				//rphm.pbftNode.pl.Plog.Println(tx)
				rphm.tdm.ReceivedNewTx[rphm.pbftNode.CurChain.DirtyMap[addr]] = append(rphm.tdm.ReceivedNewTx[rphm.pbftNode.CurChain.DirtyMap[addr]], tx)
				rphm.pbftNode.CurChain.Txpool.TxQueue = DeleteElementsInList(rphm.pbftNode.CurChain.Txpool.TxQueue, []*core.Transaction{tx})
			}
		}
	}

	rphm.pbftNode.pl.Plog.Println("after deleting, The size of tx pool is: ", len(rphm.pbftNode.CurChain.Txpool.TxQueue))
	rphm.tdm.CollectOver = true
	rphm.pbftNode.CurChain.Txpool.GetUnlocked()
}
