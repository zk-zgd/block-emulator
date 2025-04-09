package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"bytes"
	"encoding/json"
	"log"
	"time"
)

// This module used in the blockChain using transaction relaying mechanism.
// "Raw" means that the pbft only make block consensus.
type RawRelayOutsideModule struct {
	tdm      *dataSupport.Data_supportTransientAccount
	pbftNode *PbftConsensusNode
}

// msgType canbe defined in message
func (rrom *RawRelayOutsideModule) HandleMessageOutsidePBFT(msgType message.MessageType, content []byte) bool {
	switch msgType {
	case message.CRelay:
		rrom.handleRelay(content)
	case message.CRelayWithProof:
		rrom.handleRelayWithProof(content)
	case message.CInject:
		rrom.handleInjectTx(content)
	case message.CTxinitRelyay:
		rrom.handleRelayTxInit(content)
	case message.CTxRedirect:
		go rrom.handleTxRedirectout(content)
	// 在propose中发出的通知消息，用于同步
	case message.CTransientAccountCreateReady:
		rrom.handleTransientAccountCreateReady(content)

	case message.CReady2AddAccount:
		rrom.handleReady2AddAccount(content)
	/*	case message.CDataSet2Mshard:
		go rrom.handleDataset2Mshard(content)
	*/
	default:
	}
	return true
}

// receive relay transaction, which is for cross shard txs
func (rrom *RawRelayOutsideModule) handleRelay(content []byte) {
	relay := new(message.Relay)
	err := json.Unmarshal(content, relay)
	if err != nil {
		log.Panic(err)
	}
	for _, tx := range relay.Txs {
		if tx.Is_Txreq {
			rrom.pbftNode.pl.Plog.Printf("now shard %d has received a Txreq\n\n\n\n", rrom.pbftNode.ShardID)
			/*
				if len(rrom.pbftNode.CurChain.FetchAccounts([]string{"32be343b94f860124dc4fee278fdcbd38c102d88"})) == 0 {
					rrom.pbftNode.pl.Plog.Print("交易还没注入好，你别急\n\n\n")
				}
				acstate := rrom.pbftNode.CurChain.FetchAccounts([]string{"32be343b94f860124dc4fee278fdcbd38c102d88"})[0]
			*/
			// 此处处理迁移计划下发

			//rrom.pbftNode.pl.Plog.Printf("分片%d收到Txreq请求，开始下发迁移计划\n\n", rrom.pbftNode.ShardID)
			//migplan1 := &message.MigPlan{
			//ReceiverShardID: 3,
			//AccountAddr:     "4bb96091ee9d802ed039c4d1a5f6216f90f81b01",
			// 暂时的账户状态
			/*
				TransientState: core.AccountState{
					Nonce:       acstate.Nonce,
					Balance:     big.NewInt(0),
					AcAddress:   "32be343b94f860124dc4fee278fdcbd38c102d88",
					StorageRoot: acstate.StorageRoot,
					CodeHash:    acstate.CodeHash,
				},
			*/
			//}

			//migplan2 := &message.MigPlan{
			//ReceiverShardID: 3,
			//	AccountAddr:     "91337a300e0361bddb2e377dd4e88ccb7796663d",
			//}
			/*
				migplan3 := &message.MigPlan{
					ReceiverShardID: 3,
					AccountAddr:     "cd88e0e0c455345833ce31c5452c1c37f4b4c438",
				}*/
			rrom.pbftNode.pl.Plog.Printf("迁移计划等待supervisor节点分发哦，现在准备发送给原分片喵~\n\n\n")
			// rrom.pbftNode.pl.Plog.Print(acstate)
			// rrom.pbftNode.pl.Plog.Print("\n\n\n")
			//planout := message.PlanOut{
			//Plans: []*message.MigPlan{migplan1, migplan2},
			//}

			//planmsg := message.PlanOutMsg{
			//	PlanOuts: planout,
			//}

			//rByte, err := json.Marshal(planmsg)
			//if err != nil {
			//	log.Panic()
			//}
			//msg_send := message.MergeMessage(message.CTxPlanOut, rByte)
			//rrom.pbftNode.pl.Plog.Printf("分片%d收到Txreq请求，已经下发plan消息\n\n\n\n", rrom.pbftNode.ShardID)
			//go networks.TcpDial(msg_send, rrom.pbftNode.ip_nodeTable[uint64(utils.Addr2Shard(tx.Sender))][0])
		}
	}

	// 给所有分片发送准备进行过渡账户添加的消息

	/*
		tempcmp := 0 // 临时计算器
		for _, val := range relay.Txs {
			if val.Is_Txreq {
				tempcmp++
			}
		}
		if tempcmp != 0 {
			for i := 0; i < params.ShardNum-1; i++ {
				readytocreateaccount := new(message.Ready2AddAccount)
				rbyte, err := json.Marshal(readytocreateaccount)
				if err != nil {
					log.Panic(err)
				}
				rmsg := message.MergeMessage(message.CReady2AddAccount, rbyte)
				go networks.TcpDial(rmsg, rrom.pbftNode.ip_nodeTable[uint64(i)][0])
			}
			rrom.tdm.TransientOn = true
		}
	*/
	rrom.pbftNode.pl.Plog.Printf("S%dN%d : has received relay txs from shard %d, the senderSeq is %d\n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID, relay.SenderShardID, relay.SenderSeq)
	rrom.pbftNode.CurChain.Txpool.AddTxs2Pool(relay.Txs)
	rrom.pbftNode.seqMapLock.Lock()
	rrom.pbftNode.seqIDMap[relay.SenderShardID] = relay.SenderSeq
	rrom.pbftNode.seqMapLock.Unlock()
	rrom.pbftNode.pl.Plog.Printf("S%dN%d : has handled relay txs msg\n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID)
}

func (rrom *RawRelayOutsideModule) handleRelayWithProof(content []byte) {
	rwp := new(message.RelayWithProof)
	err := json.Unmarshal(content, rwp)
	if err != nil {
		log.Panic(err)
	}
	rrom.pbftNode.pl.Plog.Printf("S%dN%d : has received relay txs & proofs from shard %d, the senderSeq is %d\n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID, rwp.SenderShardID, rwp.SenderSeq)
	// validate the proofs of txs
	isAllCorrect := true
	for i, tx := range rwp.Txs {
		if ok, err := chain.TxProofVerify(tx.TxHash, &rwp.TxProofs[i]); !ok {
			rrom.pbftNode.pl.Plog.Print("当前的错误是：")
			rrom.pbftNode.pl.Plog.Println(err)
			rrom.pbftNode.pl.Plog.Print("\n\n\n")
			isAllCorrect = false
			break
		}
	}
	if isAllCorrect {
		rrom.pbftNode.pl.Plog.Println("All proofs are passed.")
		rrom.pbftNode.CurChain.Txpool.AddTxs2Pool(rwp.Txs)
	} else {
		rrom.pbftNode.pl.Plog.Println("Err: wrong proof!")
	}

	rrom.pbftNode.seqMapLock.Lock()
	rrom.pbftNode.seqIDMap[rwp.SenderShardID] = rwp.SenderSeq
	rrom.pbftNode.seqMapLock.Unlock()
	rrom.pbftNode.pl.Plog.Printf("S%dN%d : has handled relay txs msg\n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID)
}

// 此处进行relaytxinit消息的处理哦，新增merkle证明捏
func (rrom *RawRelayOutsideModule) handleRelayTxInit(content []byte) {
	rwp := new(message.Txinit_Relay)
	err := json.Unmarshal(content, rwp)
	if err != nil {
		log.Panic(err)
	}

	rrom.pbftNode.pl.Plog.Printf("S%dN%d : 我收到txinit的中继交易啦，来自 shard %d, the senderSeq is %d\n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID, rwp.SenderShardID, rwp.SenderSeq)
	// validate the proofs of txs
	isAllCorrect := true
	for i, tx := range rwp.Txs {
		if ok, errors := chain.TxInitProofVerify(tx.TxHash, &rwp.TxProofs[i]); !ok {
			rrom.pbftNode.pl.Plog.Print("当前的错误是：")
			rrom.pbftNode.pl.Plog.Println(errors)
			rrom.pbftNode.pl.Plog.Print("\n\n\n")
			isAllCorrect = false
			break
		}
		// 此处修改
		rrom.pbftNode.CurChain.Update_PartitionMap(tx.TransientAccountAddr, tx.DestinationshardID)
	}
	if isAllCorrect {
		rrom.pbftNode.pl.Plog.Println("All txinit交易的证明proofs are passed.")
		rrom.pbftNode.CurChain.Txinitpool.AddTxInits2Pool(rwp.Txs)
	} else {
		rrom.pbftNode.pl.Plog.Println("Err: txinit的证明wrong proof!")
	}

	rrom.pbftNode.seqMapLock.Lock()
	rrom.pbftNode.seqIDMap[rwp.SenderShardID] = rwp.SenderSeq
	rrom.pbftNode.seqMapLock.Unlock()
	rrom.pbftNode.pl.Plog.Printf("S%dN%d : 已经处理txinit结束啦宝宝 msg\n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID)
	// 此处修改账户位置，同时查找本分片是否有相关的交易
	//rrom.pbftNode.CurChain.Txpool.GetLocked()
	for _, tx := range rrom.pbftNode.CurChain.Txpool.TxQueue {
		for _, txmsg := range rwp.Txs {
			if !tx.Relayed && txmsg.TransientAccountAddr == tx.Sender {
				rrom.tdm.TdmMu.Lock()
				rrom.tdm.ReceivedNewTx[txmsg.DestinationshardID] = append(rrom.tdm.ReceivedNewTx[txmsg.DestinationshardID], tx)
				rrom.tdm.TdmMu.Unlock()
				rrom.pbftNode.CurChain.Txpool.GetLocked()
				rrom.pbftNode.CurChain.Txpool.TxQueue = DeleteElementsInList(rrom.pbftNode.CurChain.Txpool.TxQueue, []*core.Transaction{tx})
				rrom.pbftNode.CurChain.Txpool.GetUnlocked()
			}
			if tx.Relayed && txmsg.TransientAccountAddr == tx.Recipient {
				rrom.tdm.TdmMu.Lock()
				rrom.tdm.ReceivedNewTx[txmsg.DestinationshardID] = append(rrom.tdm.ReceivedNewTx[txmsg.DestinationshardID], tx)
				rrom.tdm.TdmMu.Unlock()
				rrom.pbftNode.CurChain.Txpool.GetLocked()
				rrom.pbftNode.CurChain.Txpool.TxQueue = DeleteElementsInList(rrom.pbftNode.CurChain.Txpool.TxQueue, []*core.Transaction{tx})
				rrom.pbftNode.CurChain.Txpool.GetUnlocked()
			}
			// rrom.pbftNode.CurChain.Update_PartitionMap(txmsg.TransientAccountAddr, txmsg.DestinationshardID)
		}

	}

	// 此处发送交易给目标分片让他接收
	newmessage := new(message.TxRedirectMsg)
	/* newmessage.Txs2DestinationShard =
	newmessage.Txs2DestinationShard = rrom.tdm.ReceivedNewTx */
	for i, temppool := range rrom.tdm.ReceivedNewTx {
		if len(temppool) == 0 {
			continue
		}
		if len(temppool) != 0 {
			newmessage.Txs2DestinationShard = temppool
			rbyte, err := json.Marshal(newmessage)
			if err != nil {
				log.Panic(err)
			}
			rmsg := message.MergeMessage(message.CTxRedirect, rbyte)
			networks.TcpDial(rmsg, rrom.pbftNode.ip_nodeTable[i][0])
			rrom.pbftNode.pl.Plog.Printf("S%dN%d : 交易重定向消息已经发送给目标分片啦\n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID)
		}
	}
	rrom.tdm.ReceivedNewTx = make(map[uint64][]*core.Transaction, 5)
}

func (rrom *RawRelayOutsideModule) handleInjectTx(content []byte) {
	it := new(message.InjectTxs)
	err := json.Unmarshal(content, it)
	if err != nil {
		log.Panic(err)
	}
	rrom.pbftNode.CurChain.Txpool.AddTxs2Pool(it.Txs)
	// rrom.pbftNode.pl.Plog.Printf("现在分片ID是%d\n\n\n\n", rrom.pbftNode.ShardID)
	rrom.pbftNode.pl.Plog.Printf("S%dN%d : has handled injected txs msg, txs: %d \n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID, len(it.Txs))
}

// wait for other shards' last rounds are over
func (rrom *RawRelayOutsideModule) handleTransientAccountCreateReady(content []byte) {
	tr := new(message.TransientAccountCreateReady)
	err := json.Unmarshal(content, tr)
	if err != nil {
		log.Panic()
	}
	rrom.tdm.T_ReadyLock.Lock()
	rrom.tdm.TransientReady[tr.FromShard] = true
	rrom.tdm.T_ReadyLock.Unlock()

	rrom.pbftNode.seqMapLock.Lock()
	rrom.tdm.ReadySeq[tr.FromShard] = tr.NowSeqID
	rrom.pbftNode.seqMapLock.Unlock()

	rrom.pbftNode.pl.Plog.Printf("transient create ready message from shard %d, seqid is %d\n", tr.FromShard, tr.NowSeqID)
	/*
		rrom.tdm.TransientOn = true
	*/
	rrom.tdm.CollectOver = true
}

func (rrom *RawRelayOutsideModule) handleTxRedirectout(content []byte) {
	it := new(message.TxRedirectMsg)
	err := json.Unmarshal(content, it)
	if err != nil {
		log.Panic(err)
	}

	if len(it.Txs2DestinationShard) == 0 {
		rrom.pbftNode.pl.Plog.Printf("当前分片%d,节点%d,收到的相关迁移账户的交易列表是空的，不做任何处理\n\n\n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID)
	} else {
		rrom.pbftNode.pl.Plog.Print("现在开始处理收到的相关迁移账户交易\n\n\n")
		rrom.pbftNode.pl.Plog.Printf("现在共有%d个交易再交易池里面\n\n\n\n", len(it.Txs2DestinationShard))
		for _, tx := range it.Txs2DestinationShard {
			tx1 := false
			for _, tx2 := range rrom.pbftNode.CurChain.Txpool.TxQueue {
				if bytes.Equal(tx.TxHash, tx2.TxHash) {
					rrom.pbftNode.pl.Plog.Print("交易已经存在了，不需要再进行添加了\n\n")
					tx1 = true
					break
				}
			}
			if tx1 {
				continue
			}
			rrom.pbftNode.CurChain.Txpool.AddTxs2Pool_Head([]*core.Transaction{tx})
		}

	}

	rrom.pbftNode.pl.Plog.Print("\n\n当前锁定时间是: ")
	duration := time.Now().Sub(rrom.tdm.TempTime)
	rrom.pbftNode.pl.Plog.Println(duration.Seconds())

}

// 处理数据集
/*
func (rrom *RawRelayOutsideModule) handleDataset2Mshard(content []byte) {

	// 解码消息
	dataset := new(message.Dataset2mshard)
	err := json.Unmarshal(content, dataset)
	if err != nil {
		log.Panic(err)
	}

	rrom.pbftNode.pl.Plog.Printf("S%dN%d : 收到来自分片 %d 节点 %d 的数据集\n",
		rrom.pbftNode.ShardID, rrom.pbftNode.NodeID,
		dataset.ShardID, dataset.NodeID)

	// 创建存储目录
	dir := params.MainShardDataRootDir
	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		log.Fatalf("无法创建目录: %v", err)
	}

	// 构建 CSV 文件路径
	fileName := fmt.Sprintf("%s/shard_%d_node_%d_dataset.csv",
		dir, dataset.ShardID, dataset.NodeID)

	// 检查文件是否存在
	var file *os.File
	// var err error
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		// 文件不存在，创建新文件
		file, err = os.Create(fileName)
		if err != nil {
			log.Fatalf("无法创建CSV文件: %v", err)
		}
		// 写入表头
		writer := csv.NewWriter(file)
		header := []string{
			"BlockNumber",
			"BlockHash",
			"ParentHash",
			"StateRoot",
			"TxHash",
			"Sender",
			"Recipient",
			"Value",
			"Timestamp",
		}
		if err := writer.Write(header); err != nil {
			log.Fatalf("无法写入CSV表头: %v", err)
		}
		writer.Flush()
	} else {
		// 文件存在，以追加模式打开
		file, err = os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("无法打开CSV文件: %v", err)
		}
	}
	defer file.Close()

	// 创建 CSV 写入器
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 将区块按照区块号从大到小排序
	blocks := make([]*core.Block, 0)
	for _, block := range dataset.Blocks {
		if block != nil {
			blocks = append(blocks, block)
		}
	}
	// 冒泡排序，按区块号从大到小排序
	for i := 0; i < len(blocks)-1; i++ {
		for j := 0; j < len(blocks)-1-i; j++ {
			if blocks[j].Header.Number < blocks[j+1].Header.Number {
				blocks[j], blocks[j+1] = blocks[j+1], blocks[j]
			}
		}
	}

	// 遍历排序后的区块数据并写入CSV
	for _, block := range blocks {
		// 遍历区块中的交易
		for _, tx := range block.Body {
			record := []string{
				strconv.FormatUint(block.Header.Number, 10),     // 区块号
				fmt.Sprintf("%x", block.Hash),                   // 区块哈希
				fmt.Sprintf("%x", block.Header.ParentBlockHash), // 父区块哈希
				fmt.Sprintf("%x", block.Header.StateRoot),       // 状态根
				fmt.Sprintf("%x", tx.TxHash),                    // 交易哈希
				tx.Sender,                                       // 发送者
				tx.Recipient,                                    // 接收者
				tx.Value.String(),                               // 交易金额
				tx.Time.Format("2006-01-02 15:04:05"),           // 时间戳
			}
			if err := writer.Write(record); err != nil {
				log.Fatalf("无法写入交易记录到CSV: %v", err)
			}
		}
	}

	rrom.pbftNode.pl.Plog.Printf("分片 %d 节点 %d 的数据集已成功追加到 %s 文件中！\n",
		dataset.ShardID, dataset.NodeID, fileName)

	// 进行额外的数据分析（可选）
	// analysisResult := analyzeDataset(dataset.Blocks)
	// rrom.pbftNode.pl.Plog.Printf("数据分析结果：\n%s", analysisResult)

}
*/

func (rrom *RawRelayOutsideModule) handleReady2AddAccount(content []byte) {
	ready2addaccount := new(message.Ready2AddAccount)
	err := json.Unmarshal(content, ready2addaccount)
	if err != nil {
		log.Panic(err)
	}

	rrom.tdm.TransientOn = true
}
