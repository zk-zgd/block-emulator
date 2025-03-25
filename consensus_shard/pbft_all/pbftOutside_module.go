package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/utils"
	"encoding/json"
	"log"
)

// This module used in the blockChain using transaction relaying mechanism.
// "Raw" means that the pbft only make block consensus.
type RawRelayOutsideModule struct {
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
			rrom.pbftNode.pl.Plog.Printf("分片%d收到Txreq请求，开始下发迁移计划\n\n", rrom.pbftNode.ShardID)
			migplan := &message.MigPlan{
				ReceiverShardID: 1,
				AccountAddr:     "32be343b94f860124dc4fee278fdcbd38c102d88",
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
			}
			rrom.pbftNode.pl.Plog.Printf("迁移计划已编写好，现在准备发送给原分片喵~\n\n\n")
			// rrom.pbftNode.pl.Plog.Print(acstate)
			// rrom.pbftNode.pl.Plog.Print("\n\n\n")
			planout := message.PlanOut{
				Plans: []*message.MigPlan{migplan},
			}

			planmsg := message.PlanOutMsg{
				PlanOuts: planout,
			}

			rByte, err := json.Marshal(planmsg)
			if err != nil {
				log.Panic()
			}
			msg_send := message.MergeMessage(message.CTxPlanOut, rByte)
			rrom.pbftNode.pl.Plog.Printf("分片%d收到Txreq请求，已经下发plan消息\n\n\n\n", rrom.pbftNode.ShardID)
			go networks.TcpDial(msg_send, rrom.pbftNode.ip_nodeTable[uint64(utils.Addr2Shard(tx.Sender))][0])
		}
	}
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
		if ok, _ := chain.TxProofVerify(tx.TxHash, &rwp.TxProofs[i]); !ok {
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

func (rrom *RawRelayOutsideModule) handleInjectTx(content []byte) {
	it := new(message.InjectTxs)
	err := json.Unmarshal(content, it)
	if err != nil {
		log.Panic(err)
	}
	rrom.pbftNode.CurChain.Txpool.AddTxs2Pool(it.Txs)
	// rrom.pbftNode.pl.Plog.Printf("现在分片ID是%d\n\n\n\n", rrom.pbftNode.ShardID)
	if rrom.pbftNode.ShardID == uint64(0) && !params.Cishu {
		rrom.pbftNode.pl.Plog.Printf("现在分片ID是%d\n\n\n\n", rrom.pbftNode.ShardID)
		rrom.pbftNode.pl.Plog.Printf("现在我要开始添加Txreq请求了！\n\n\n\n")
		rrom.pbftNode.CreateTxReq(int(rrom.pbftNode.ShardID))
		params.Cishu = true
	}
	rrom.pbftNode.pl.Plog.Printf("S%dN%d : has handled injected txs msg, txs: %d \n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID, len(it.Txs))
}
