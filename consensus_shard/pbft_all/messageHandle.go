package pbft_all

import (
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/shard"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// this func is only invoked by main node
func (p *PbftConsensusNode) Propose() {
	// wait other nodes to start TCPlistening, sleep 5 sec.
	time.Sleep(5 * time.Second)

	nextRoundBeginSignal := make(chan bool)

	go func() {
		// go into the next round
		for {
			time.Sleep(time.Duration(int64(p.pbftChainConfig.BlockInterval)) * time.Millisecond)
			// send a signal to another GO-Routine. It will block until a GO-Routine try to fetch data from this channel.
			for p.pbftStage.Load() != 1 {
				time.Sleep(time.Millisecond * 100)
			}
			nextRoundBeginSignal <- true
		}
	}()
	// 这个函数仅仅适合m分片进行数据的读取，每隔10个共识轮读取一次
	/*
		go func() {
			//
			for {
				time.Sleep(time.Second)
				// 主分片轮询检查当前共识轮的大小, 当为10 的时候进行数据的读取
				if p.ShardID == uint64(params.ShardNum-1) && params.ConsensusRoundForEvrShard[params.ShardNum-1] > 0 && params.ConsensusRoundForEvrShard[params.ShardNum-1]%5 == 0 {
					// 遍历所有分片
					for shardID := 0; shardID < params.ShardNum-1; shardID++ {
						p.pl.Plog.Print("现在开始读取分片0的区块交易\n\n\n\n")
						go exportShardBlocks(shardID, 0)
						p.pl.Plog.Print("读取结束！！！！！！\n\n\n\n")
					}
					fmt.Println("所有分片的区块数据已成功导出到 CSV 文件中！")
				}
			}

		}()*/

	go func() {
		// check whether to view change
		for {
			time.Sleep(time.Second)
			if time.Now().UnixMilli()-p.lastCommitTime.Load() > int64(params.PbftViewChangeTimeOut) {
				p.lastCommitTime.Store(time.Now().UnixMilli())
				go p.viewChangePropose()
			}
		}
	}()

	/*
		go func() {
			for {
				time.Sleep(time.Second)
				// 发送消息
				if p.ShardID != uint64(params.ShardNum)-1 && params.ConsensusRoundForEvrShard[p.ShardID] > 0 && params.ConsensusRoundForEvrShard[p.ShardID]%5 == 0 {
					if len(p.notifyPool) != 0 {
						time.Sleep(time.Millisecond * 10)
						continue
					}
					go p.SendMessage2MShard(p.ShardID, 0)
				}
			}
		}()
	*/

	for {
		select {
		case <-nextRoundBeginSignal:
			go func() {
				// if this node is not leader, do not propose.
				if uint64(p.view.Load()) != p.NodeID {
					return
				}

				// 在此处进行改变交易路由
				if p.ShardID == uint64(1) && !params.Cishu && params.ConsensusRoundForEvrShard[uint64(1)] >= 10 {
					p.pl.Plog.Printf("现在分片ID是%d\n\n\n\n", p.ShardID)
					p.pl.Plog.Printf("现在我要开始添加Txreq请求了！\n\n\n\n")
					p.CreateTxReq(int(p.ShardID))
					params.Cishu = true
				}
				p.sequenceLock.Lock()
				p.pl.Plog.Printf("S%dN%d get sequenceLock locked, now trying to propose...\n", p.ShardID, p.NodeID)
				// propose
				// implement interface to generate propose
				_, r := p.ihm.HandleinPropose()

				digest := getDigest(r)
				p.requestPool[string(digest)] = r
				p.pl.Plog.Printf("S%dN%d put the request into the pool ...\n", p.ShardID, p.NodeID)

				ppmsg := message.PrePrepare{
					RequestMsg: r,
					Digest:     digest,
					SeqID:      p.sequenceID,
				}
				p.height2Digest[p.sequenceID] = string(digest)
				// marshal and broadcast
				ppbyte, err := json.Marshal(ppmsg)
				if err != nil {
					log.Panic()
				}
				msg_send := message.MergeMessage(message.CPrePrepare, ppbyte)
				networks.Broadcast(p.RunningNode.IPaddr, p.getNeighborNodes(), msg_send)
				networks.TcpDial(msg_send, p.RunningNode.IPaddr)
				p.pbftStage.Store(2)
			}()

		case <-p.pStop:
			p.pl.Plog.Printf("S%dN%d get stopSignal in Propose Routine, now stop...\n", p.ShardID, p.NodeID)
			return
		}
	}
}

// Handle pre-prepare messages here.
// If you want to do more operations in the pre-prepare stage, you can implement the interface "ExtraOpInConsensus",
// and call the function: **ExtraOpInConsensus.HandleinPrePrepare**
func (p *PbftConsensusNode) handlePrePrepare(content []byte) {
	p.RunningNode.PrintNode()
	fmt.Println("received the PrePrepare ...")
	// decode the message
	ppmsg := new(message.PrePrepare)
	err := json.Unmarshal(content, ppmsg)
	if err != nil {
		log.Panic(err)
	}

	curView := p.view.Load()
	p.pbftLock.Lock()
	defer p.pbftLock.Unlock()
	for p.pbftStage.Load() < 1 && ppmsg.SeqID >= p.sequenceID && p.view.Load() == curView {
		p.conditionalVarpbftLock.Wait()
	}
	defer p.conditionalVarpbftLock.Broadcast()

	// if this message is out of date, return.
	if ppmsg.SeqID < p.sequenceID || p.view.Load() != curView {
		return
	}

	flag := false
	if digest := getDigest(ppmsg.RequestMsg); string(digest) != string(ppmsg.Digest) {
		p.pl.Plog.Printf("S%dN%d : the digest is not consistent, so refuse to prepare. \n", p.ShardID, p.NodeID)
	} else if p.sequenceID < ppmsg.SeqID {
		p.requestPool[string(getDigest(ppmsg.RequestMsg))] = ppmsg.RequestMsg
		p.height2Digest[ppmsg.SeqID] = string(getDigest(ppmsg.RequestMsg))
		p.pl.Plog.Printf("S%dN%d : the Sequence id is not consistent, so refuse to prepare. \n", p.ShardID, p.NodeID)
	} else {
		// do your operation in this interface
		flag = p.ihm.HandleinPrePrepare(ppmsg)
		p.requestPool[string(getDigest(ppmsg.RequestMsg))] = ppmsg.RequestMsg
		p.height2Digest[ppmsg.SeqID] = string(getDigest(ppmsg.RequestMsg))
	}
	// if the message is true, broadcast the prepare message
	if flag {
		pre := message.Prepare{
			Digest:     ppmsg.Digest,
			SeqID:      ppmsg.SeqID,
			SenderNode: p.RunningNode,
		}
		prepareByte, err := json.Marshal(pre)
		if err != nil {
			log.Panic()
		}
		// broadcast
		msg_send := message.MergeMessage(message.CPrepare, prepareByte)
		networks.Broadcast(p.RunningNode.IPaddr, p.getNeighborNodes(), msg_send)
		networks.TcpDial(msg_send, p.RunningNode.IPaddr)
		p.pl.Plog.Printf("S%dN%d : has broadcast the prepare message \n", p.ShardID, p.NodeID)

		// Pbft stage add 1. It means that this round of pbft goes into the next stage, i.e., Prepare stage.
		p.pbftStage.Add(1)
	}
}

// Handle prepare messages here.
// If you want to do more operations in the prepare stage, you can implement the interface "ExtraOpInConsensus",
// and call the function: **ExtraOpInConsensus.HandleinPrepare**
func (p *PbftConsensusNode) handlePrepare(content []byte) {
	p.pl.Plog.Printf("S%dN%d : received the Prepare ...\n", p.ShardID, p.NodeID)
	// decode the message
	pmsg := new(message.Prepare)
	err := json.Unmarshal(content, pmsg)
	if err != nil {
		log.Panic(err)
	}

	curView := p.view.Load()
	p.pbftLock.Lock()
	defer p.pbftLock.Unlock()
	for p.pbftStage.Load() < 2 && pmsg.SeqID >= p.sequenceID && p.view.Load() == curView {
		p.conditionalVarpbftLock.Wait()
	}
	defer p.conditionalVarpbftLock.Broadcast()

	// if this message is out of date, return.
	if pmsg.SeqID < p.sequenceID || p.view.Load() != curView {
		return
	}

	if _, ok := p.requestPool[string(pmsg.Digest)]; !ok {
		p.pl.Plog.Printf("S%dN%d : doesn't have the digest in the requst pool, refuse to commit\n", p.ShardID, p.NodeID)
	} else if p.sequenceID < pmsg.SeqID {
		p.pl.Plog.Printf("S%dN%d : inconsistent sequence ID, refuse to commit\n", p.ShardID, p.NodeID)
	} else {
		// if needed more operations, implement interfaces
		p.ihm.HandleinPrepare(pmsg)

		p.set2DMap(true, string(pmsg.Digest), pmsg.SenderNode)
		cnt := len(p.cntPrepareConfirm[string(pmsg.Digest)])

		// if the node has received 2f messages (itself included), and it haven't committed, then it commit
		p.lock.Lock()
		defer p.lock.Unlock()
		if uint64(cnt) >= 2*p.malicious_nums+1 && !p.isCommitBordcast[string(pmsg.Digest)] {
			p.pl.Plog.Printf("S%dN%d : is going to commit\n", p.ShardID, p.NodeID)
			// generate commit and broadcast
			c := message.Commit{
				Digest:     pmsg.Digest,
				SeqID:      pmsg.SeqID,
				SenderNode: p.RunningNode,
			}
			commitByte, err := json.Marshal(c)
			if err != nil {
				log.Panic()
			}
			msg_send := message.MergeMessage(message.CCommit, commitByte)
			networks.Broadcast(p.RunningNode.IPaddr, p.getNeighborNodes(), msg_send)
			networks.TcpDial(msg_send, p.RunningNode.IPaddr)
			p.isCommitBordcast[string(pmsg.Digest)] = true
			p.pl.Plog.Printf("S%dN%d : commit is broadcast\n", p.ShardID, p.NodeID)

			p.pbftStage.Add(1)
		}
	}
}

// Handle commit messages here.
// If you want to do more operations in the commit stage, you can implement the interface "ExtraOpInConsensus",
// and call the function: **ExtraOpInConsensus.HandleinCommit**
func (p *PbftConsensusNode) handleCommit(content []byte) {
	// decode the message
	cmsg := new(message.Commit)
	err := json.Unmarshal(content, cmsg)
	if err != nil {
		log.Panic(err)
	}

	curView := p.view.Load()
	p.pbftLock.Lock()
	defer p.pbftLock.Unlock()
	for p.pbftStage.Load() < 3 && cmsg.SeqID >= p.sequenceID && p.view.Load() == curView {
		p.conditionalVarpbftLock.Wait()
	}
	defer p.conditionalVarpbftLock.Broadcast()

	if cmsg.SeqID < p.sequenceID || p.view.Load() != curView {
		return
	}

	p.pl.Plog.Printf("S%dN%d received the Commit from ...%d\n", p.ShardID, p.NodeID, cmsg.SenderNode.NodeID)
	p.set2DMap(false, string(cmsg.Digest), cmsg.SenderNode)
	cnt := len(p.cntCommitConfirm[string(cmsg.Digest)])

	p.lock.Lock()
	defer p.lock.Unlock()

	if uint64(cnt) >= 2*p.malicious_nums+1 && !p.isReply[string(cmsg.Digest)] {
		p.pl.Plog.Printf("S%dN%d : has received 2f + 1 commits ... \n", p.ShardID, p.NodeID)
		// if this node is left behind, so it need to requst blocks
		if _, ok := p.requestPool[string(cmsg.Digest)]; !ok {
			p.isReply[string(cmsg.Digest)] = true
			p.askForLock.Lock()
			// request the block
			sn := &shard.Node{
				NodeID:  uint64(p.view.Load()),
				ShardID: p.ShardID,
				IPaddr:  p.ip_nodeTable[p.ShardID][uint64(p.view.Load())],
			}
			orequest := message.RequestOldMessage{
				SeqStartHeight: p.sequenceID + 1,
				SeqEndHeight:   cmsg.SeqID,
				ServerNode:     sn,
				SenderNode:     p.RunningNode,
			}
			bromyte, err := json.Marshal(orequest)
			if err != nil {
				log.Panic()
			}

			p.pl.Plog.Printf("S%dN%d : is now requesting message (seq %d to %d) ... \n", p.ShardID, p.NodeID, orequest.SeqStartHeight, orequest.SeqEndHeight)
			msg_send := message.MergeMessage(message.CRequestOldrequest, bromyte)
			networks.TcpDial(msg_send, orequest.ServerNode.IPaddr)
		} else {
			// implement interface
			p.ihm.HandleinCommit(cmsg)
			p.isReply[string(cmsg.Digest)] = true
			p.pl.Plog.Printf("S%dN%d: this round of pbft %d is end \n", p.ShardID, p.NodeID, p.sequenceID)
			p.sequenceID += 1
		}
		// p.pl.Plog.Printf("当前提交阶段已经结束，现在进入阶段4，交易路由重定向阶段\n\n\n")

		//stage_4 := new(message.TxRedirectMsg)
		//bstage4, err := json.Marshal(stage_4)
		//if err != nil {
		//	log.Panic()
		//}
		/*
			msg := message.MergeMessage(message.CTxRedirect, bstage4)
			networks.Broadcast(p.RunningNode.IPaddr, p.getNeighborNodes(), msg)
			networks.TcpDial(msg, p.RunningNode.IPaddr)
			p.pl.Plog.Printf("S%dN%d : 交易重定向消息已经广播啦，现在进入阶段五ovo\n", p.ShardID, p.NodeID)
		*/
		/* for acaddr, nowacshardid := range p.CurChain.DirtyMap {

			p.pl.Plog.Printf("当前账户%s的分片ID是%d\n", acaddr, nowacshardid)
			// p.CurChain.Update_PartitionMap(acaddr, p.ShardID)
			// p.CurChain.Update_AccountShardID(acaddr, p.ShardID, nowacshardid)
			TempPool := new(message.TxRedirect)
			TempPool.ShardID = p.ShardID
			for _, txs := range p.CurChain.Txpool.TxQueue {
				if txs.Sender == acaddr {
					p.pl.Plog.Printf("当前账户%s的交易是%s\n", acaddr, txs.TxHash)
					// 此处添加交易路由重定向
					// 将交易加入临时池
					TempPool.Temppool.AddTx2Pool(txs)
					// 删除原来交易池中的交易
					p.CurChain.Txpool.DeleteTx(txs)
					TempPool.ReceiveTxShardID = nowacshardid
					TempPool.Isnil = false
					// 此处添加交易路由重定向结束
				}
			}
			// 此处添加交易路由重定向结束
			// 发送交易路由重定向消息
			if len(TempPool.Temppool.TxQueue) == 0 {
				p.pl.Plog.Printf("当前账户%s没有交易需要重定向\n", acaddr)
				TempPool.Isnil = true
			}
			btxredirect, err := json.Marshal(TempPool)
			if err != nil {
				log.Panic()
			}
			p.pl.Plog.Printf("S%dN%d :现在开始发送交易重定向消息 \n", p.ShardID, p.NodeID)
			mergemsg := message.MergeMessage(message.CTxRedirect, btxredirect)

			// 发送给所有分片
			networks.TcpDial(mergemsg, p.ip_nodeTable[nowacshardid][0])
		}

		// 新添加一个交易路由重定向阶段
		p.pbftStage.Add(1)
		p.lastCommitTime.Store(time.Now().UnixMilli()) */

		// if this node is a main node, then unlock the sequencelock
		/*
			if p.NodeID == uint64(p.view.Load()) {
				p.sequenceLock.Unlock()
				p.pl.Plog.Printf("S%dN%d get sequenceLock unlocked...\n", p.ShardID, p.NodeID)
			}
		*/
		// p.pl.Plog.Printf("当前分片%d已经收到了txredirect的消息，交易已经全部入库，现在重置阶段ovo\n\n", p.ShardID)
		params.ConsensusRoundForEvrShard[p.ShardID]++
		// p.pl.Plog.Printf("这里改变共识轮的大小, 当前阶段结束后分片的共识轮是: %d\n\n\n", params.ConsensusRoundForEvrShard[p.ShardID])
		p.pbftStage.Store(1)
		if p.NodeID == uint64(p.view.Load()) {
			p.sequenceLock.Unlock()
			p.pl.Plog.Printf("S%dN%d get sequenceLock unlocked...\n", p.ShardID, p.NodeID)
		}
	}

}

// this func is only invoked by the main node,
// if the request is correct, the main node will send
// block back to the message sender.
// now this function can send both block and partition
func (p *PbftConsensusNode) handleRequestOldSeq(content []byte) {
	if uint64(p.view.Load()) != p.NodeID {
		return
	}

	rom := new(message.RequestOldMessage)
	err := json.Unmarshal(content, rom)
	if err != nil {
		log.Panic()
	}
	p.pl.Plog.Printf("S%dN%d : received the old message requst from ...", p.ShardID, p.NodeID)
	rom.SenderNode.PrintNode()

	oldR := make([]*message.Request, 0)
	for height := rom.SeqStartHeight; height <= rom.SeqEndHeight; height++ {
		if _, ok := p.height2Digest[height]; !ok {
			p.pl.Plog.Printf("S%dN%d : has no this digest to this height %d\n", p.ShardID, p.NodeID, height)
			break
		}
		if r, ok := p.requestPool[p.height2Digest[height]]; !ok {
			p.pl.Plog.Printf("S%dN%d : has no this message to this digest %d\n", p.ShardID, p.NodeID, height)
			break
		} else {
			oldR = append(oldR, r)
		}
	}
	p.pl.Plog.Printf("S%dN%d : has generated the message to be sent\n", p.ShardID, p.NodeID)

	p.ihm.HandleReqestforOldSeq(rom)

	// send the block back
	sb := message.SendOldMessage{
		SeqStartHeight: rom.SeqStartHeight,
		SeqEndHeight:   rom.SeqEndHeight,
		OldRequest:     oldR,
		SenderNode:     p.RunningNode,
	}
	sbByte, err := json.Marshal(sb)
	if err != nil {
		log.Panic()
	}
	msg_send := message.MergeMessage(message.CSendOldrequest, sbByte)
	networks.TcpDial(msg_send, rom.SenderNode.IPaddr)
	p.pl.Plog.Printf("S%dN%d : send blocks\n", p.ShardID, p.NodeID)
}

// node requst blocks and receive blocks from the main node
func (p *PbftConsensusNode) handleSendOldSeq(content []byte) {
	som := new(message.SendOldMessage)
	err := json.Unmarshal(content, som)
	if err != nil {
		log.Panic()
	}
	p.pl.Plog.Printf("S%dN%d : has received the SendOldMessage message\n", p.ShardID, p.NodeID)

	// implement interface for new consensus
	p.ihm.HandleforSequentialRequest(som)
	beginSeq := som.SeqStartHeight
	for idx, r := range som.OldRequest {
		p.requestPool[string(getDigest(r))] = r
		p.height2Digest[uint64(idx)+beginSeq] = string(getDigest(r))
		p.isReply[string(getDigest(r))] = true
		p.pl.Plog.Printf("this round of pbft %d is end \n", uint64(idx)+beginSeq)
	}
	p.sequenceID = som.SeqEndHeight + 1
	if rDigest, ok1 := p.height2Digest[p.sequenceID]; ok1 {
		if r, ok2 := p.requestPool[rDigest]; ok2 {
			ppmsg := &message.PrePrepare{
				RequestMsg: r,
				SeqID:      p.sequenceID,
				Digest:     getDigest(r),
			}
			flag := false
			flag = p.ihm.HandleinPrePrepare(ppmsg)
			if flag {
				pre := message.Prepare{
					Digest:     ppmsg.Digest,
					SeqID:      ppmsg.SeqID,
					SenderNode: p.RunningNode,
				}
				prepareByte, err := json.Marshal(pre)
				if err != nil {
					log.Panic()
				}
				// broadcast
				msg_send := message.MergeMessage(message.CPrepare, prepareByte)
				networks.Broadcast(p.RunningNode.IPaddr, p.getNeighborNodes(), msg_send)
				p.pl.Plog.Printf("S%dN%d : has broadcast the prepare message \n", p.ShardID, p.NodeID)
			}
		}
	}

	p.askForLock.Unlock()
}

// 接收迁移计划消息函数体
func (p *PbftConsensusNode) handlePlanout(content []byte) {
	planout := new(message.PlanOutMsg)
	err := json.Unmarshal(content, planout)
	if err != nil {
		log.Panic()
	}
	// fmt.Println("received the planout message")
	p.pl.Plog.Printf("分片%d已经收到了下发的迁移计划喵，这里准备处理了喵~~\n", p.ShardID)
	p.pl.Plog.Printf("输出一下迁移计划喵~\n")

	for _, plan := range planout.PlanOuts.Plans {
		p.pl.Plog.Printf("ReceiverShardID: %d, AccountAddr: %s\n", plan.ReceiverShardID, plan.AccountAddr)
	}
	// 此处创建通知消息
	p.pl.Plog.Printf("分片%d准备创建迁移计划通知消息\n", p.ShardID)

	for _, val := range planout.PlanOuts.Plans {
		p.CreateTxInitInfo(int(p.ShardID), int(val.ReceiverShardID), val.AccountAddr)
	}

	p.pl.Plog.Printf("分片%d已经创建迁移计划通知消息完成\n\n\n", p.ShardID)
}

// 目标分片收到原分片发来的迁移计划通知消息，现在进行消息处理
func (p *PbftConsensusNode) handleTxinitCreateinfo(content []byte) {
	createtxinitinfo := new(message.TxinitCreate)
	err := json.Unmarshal(content, createtxinitinfo)
	if err != nil {
		log.Panic()
	}
	// p.pl.Plog.Printf("目的分片%d已经收到了迁移计划通知消息，这里准备创建替身账户喵~~\n", p.ShardID)
	// p.pl.Plog.Printf("输出一下迁移计划通知消息喵~\n")
	// p.pl.Plog.Printf("TransientTxAddr: %s, Nowtime: %s, SendersharedID: %d\n", createtxinitinfo.TransientTxAddr, createtxinitinfo.Nowtime, createtxinitinfo.SendershardID)
	// createtxinitinfo.
	p.pl.Plog.Print("创建消息已经放入对应分片监听池子, 接下来准备进行账户添加操作\n\n")
	p.notifyPool = append(p.notifyPool, *createtxinitinfo)
	/*
		p.CurChain.Update_PartitionMap(createtxinitinfo.TransientTxAddr, p.ShardID)
		p.CurChain.AddTransientAccount(createtxinitinfo.TransientTxAddr, createtxinitinfo.MigAccount_State, 0)
		// 此处创建txinit交易
		p.New_TxInit(createtxinitinfo.TransientTxAddr)
		p.pl.Plog.Print("创建成功了xd，现在检查一下状态\n\n")
		p.pl.Plog.Print(p.CurChain.FetchAccounts([]string{createtxinitinfo.TransientTxAddr})[0])
		p.pl.Plog.Print("状态检查结束\n\n")
	*/
	// 目的分片收到通知，立刻告知所有分片进行添加账户操作
	p.Informallshardcreatetransientaccount()
}

func (p *PbftConsensusNode) handleTxRedirect(content []byte) {
	msg := new(message.TxRedirectMsg)
	err := json.Unmarshal(content, msg)
	if err != nil {
		log.Panic()
	}

	// 遍历现有的交易池

	for acaddr, nowacshardid := range p.CurChain.DirtyMap {
		p.pl.Plog.Printf("当前账户%s的分片ID是%d\n", acaddr, nowacshardid)
		// p.CurChain.Update_PartitionMap(acaddr, p.ShardID)
		// p.CurChain.Update_AccountShardID(acaddr, p.ShardID, nowacshardid)
		TempPool := new(message.TxRedirect)
		TempPool.ShardID = p.ShardID
		for _, txs := range p.CurChain.Txpool.TxQueue {
			if txs.Sender == acaddr && !txs.Relayed {
				// p.pl.Plog.Printf("当前账户%s的交易是%s\n", acaddr, txs.TxHash)
				// 此处添加交易路由重定向
				// 将交易加入临时池
				TempPool.Temppool.AddTx2Pool(txs)
				// 删除原来交易池中的交易
				p.CurChain.Txpool.DeleteTx(txs)
				TempPool.ReceiveTxShardID = nowacshardid
				TempPool.Isnil = false
				// 此处添加交易路由重定向结束
			}
			if txs.Recipient == acaddr && txs.Relayed {
				TempPool.Temppool.AddTx2Pool(txs)
				p.CurChain.Txpool.DeleteTx(txs)
				TempPool.ReceiveTxShardID = nowacshardid
				TempPool.Isnil = false
			}
		}
		// 此处添加交易路由重定向结束
		// 发送交易路由重定向消息
		if len(TempPool.Temppool.TxQueue) == 0 {
			p.pl.Plog.Printf("当前账户%s没有交易需要重定向\n", acaddr)
			TempPool.Isnil = true
		}
		btxredirect, err := json.Marshal(TempPool)
		if err != nil {
			log.Panic()
		}
		p.pl.Plog.Printf("S%dN%d :现在开始发送交易重定向消息 \n", p.ShardID, p.NodeID)
		mergemsg := message.MergeMessage(message.CTxRedirectout, btxredirect)
		// 发送给所有分片
		networks.TcpDial(mergemsg, p.ip_nodeTable[nowacshardid][0])

	}

	// curView := p.view.Load()
	p.pbftLock.Lock()
	defer p.pbftLock.Unlock()

	p.pl.Plog.Printf("当前分片%d已经收到了txredirect的消息，交易已经全部入库，现在重置阶段ovo\n\n", p.ShardID)
	params.ConsensusRoundForEvrShard[p.ShardID]++
	p.pl.Plog.Printf("这里改变共识轮的大小, 当前阶段结束后分片的共识轮是: %d\n\n\n", params.ConsensusRoundForEvrShard[p.ShardID])
	p.pbftStage.Store(1)
	if p.NodeID == uint64(p.view.Load()) {
		p.sequenceLock.Unlock()
		p.pl.Plog.Printf("S%dN%d get sequenceLock unlocked...\n", p.ShardID, p.NodeID)
	}
}
