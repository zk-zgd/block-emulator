package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/shard"
	"blockEmulator/utils"
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// set 2d map, only for pbft maps, if the first parameter is true, then set the cntPrepareConfirm map,
// otherwise, cntCommitConfirm map will be set
func (p *PbftConsensusNode) set2DMap(isPrePareConfirm bool, key string, val *shard.Node) {
	if isPrePareConfirm {
		if _, ok := p.cntPrepareConfirm[key]; !ok {
			p.cntPrepareConfirm[key] = make(map[*shard.Node]bool)
		}
		p.cntPrepareConfirm[key][val] = true
	} else {
		if _, ok := p.cntCommitConfirm[key]; !ok {
			p.cntCommitConfirm[key] = make(map[*shard.Node]bool)
		}
		p.cntCommitConfirm[key][val] = true
	}
}

// get neighbor nodes in a shard
func (p *PbftConsensusNode) getNeighborNodes() []string {
	receiverNodes := make([]string, 0)
	for _, ip := range p.ip_nodeTable[p.ShardID] {
		receiverNodes = append(receiverNodes, ip)
	}
	return receiverNodes
}

// get node ips of shard id=shardID
func (p *PbftConsensusNode) getNodeIpsWithinShard(shardID uint64) []string {
	receiverNodes := make([]string, 0)
	for _, ip := range p.ip_nodeTable[shardID] {
		receiverNodes = append(receiverNodes, ip)
	}
	return receiverNodes
}

func (p *PbftConsensusNode) writeCSVline(metricName []string, metricVal []string) {
	// Construct directory path
	dirpath := params.DataWrite_path + "pbft_shardNum=" + strconv.Itoa(int(p.pbftChainConfig.ShardNums))
	err := os.MkdirAll(dirpath, os.ModePerm)
	if err != nil {
		log.Panic(err)
	}

	// Construct target file path
	targetPath := fmt.Sprintf("%s/Shard%d%d.csv", dirpath, p.ShardID, p.pbftChainConfig.ShardNums)

	// Open file, create if it does not exist
	file, err := os.OpenFile(targetPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Panic(err)
	}
	defer file.Close()

	// Create CSV writer
	writer := csv.NewWriter(file)

	// Write header if the file is newly created
	fileInfo, err := file.Stat()
	if err != nil {
		log.Panic(err)
	}
	if fileInfo.Size() == 0 {
		if err := writer.Write(metricName); err != nil {
			log.Panic(err)
		}
		writer.Flush()
	}

	// Write data
	if err := writer.Write(metricVal); err != nil {
		log.Panic(err)
	}
	writer.Flush()
}

// get the digest of request
func getDigest(r *message.Request) []byte {
	b, err := json.Marshal(r)
	if err != nil {
		log.Panic(err)
	}
	hash := sha256.Sum256(b)
	return hash[:]
}

// calculate TCL
func computeTCL(txs []*core.Transaction, commitTS time.Time) int64 {
	ret := int64(0)
	for _, tx := range txs {
		ret += commitTS.Sub(tx.Time).Milliseconds()
	}
	return ret
}

// help to send Relay message to other shards.
func (p *PbftConsensusNode) RelayMsgSend() {
	if params.RelayWithMerkleProof != 0 {
		log.Panicf("Parameter Error: RelayWithMerkleProof should be 0, but RelayWithMerkleProof=%d", params.RelayWithMerkleProof)
	}

	for sid := uint64(0); sid < p.pbftChainConfig.ShardNums; sid++ {
		if sid == p.ShardID {
			continue
		}
		relay := message.Relay{
			Txs:           p.CurChain.Txpool.RelayPool[sid],
			SenderShardID: p.ShardID,
			SenderSeq:     p.sequenceID,
		}
		rByte, err := json.Marshal(relay)
		if err != nil {
			log.Panic()
		}
		msg_send := message.MergeMessage(message.CRelay, rByte)
		go networks.TcpDial(msg_send, p.ip_nodeTable[sid][0])
		p.pl.Plog.Printf("S%dN%d : sended relay txs to %d\n", p.ShardID, p.NodeID, sid)
	}
	p.CurChain.Txpool.ClearRelayPool()
}

// help to send RelayWithProof message to other shards.
func (p *PbftConsensusNode) RelayWithProofSend(block *core.Block) {
	if params.RelayWithMerkleProof != 1 {
		log.Panicf("Parameter Error: RelayWithMerkleProof should be 1, but RelayWithMerkleProof=%d", params.RelayWithMerkleProof)
	}
	for sid := uint64(0); sid < p.pbftChainConfig.ShardNums; sid++ {
		if sid == p.ShardID {
			continue
		}

		txHashes := make([][]byte, len(p.CurChain.Txpool.RelayPool[sid]))
		for i, tx := range p.CurChain.Txpool.RelayPool[sid] {
			txHashes[i] = tx.TxHash[:]
		}
		txProofs := chain.TxProofBatchGenerateOnBlock(txHashes, block)

		rwp := message.RelayWithProof{
			Txs:           p.CurChain.Txpool.RelayPool[sid],
			TxProofs:      txProofs,
			SenderShardID: p.ShardID,
			SenderSeq:     p.sequenceID,
		}
		rByte, err := json.Marshal(rwp)
		if err != nil {
			log.Panic()
		}
		msg_send := message.MergeMessage(message.CRelayWithProof, rByte)

		go networks.TcpDial(msg_send, p.ip_nodeTable[sid][0])
		p.pl.Plog.Printf("S%dN%d : sended relay txs & proofs to %d\n", p.ShardID, p.NodeID, sid)
	}
	p.CurChain.Txpool.ClearRelayPool()
}

// delete the txs in blocks. This list should be locked before calling this func.
func DeleteElementsInList(list []*core.Transaction, elements []*core.Transaction) []*core.Transaction {
	elementHashMap := make(map[string]bool)
	for _, element := range elements {
		elementHashMap[string(element.TxHash)] = true
	}

	removedCnt := 0
	for left, right := 0, 0; right < len(list); right++ {
		// if this tx should be deleted.
		if _, ok := elementHashMap[string(list[right].TxHash)]; ok {
			removedCnt++
		} else {
			list[left] = list[right]
			left++
		}
	}
	return list[:-removedCnt]
}

// 此处新建一个TXreq交易
func (p *PbftConsensusNode) CreateTxReq(shardid int) {
	// 创建一个新的交易
	txReq := core.NewTransaction(params.SenderAddr[shardid], params.SenderAddr[3], params.Init_Balance, 0, time.Now()) // txReq.TxHash = sha256.Sum256([]byte(fmt.Sprintf("%v", time.Now()))).[:]
	txReq.Is_Txreq = true
	hash := sha256.Sum256(txReq.Encode())
	txReq.TxHash = hash[:]
	// 添加交易到交易池
	p.CurChain.Txpool.AddTxs2Pool_Head([]*core.Transaction{txReq})
}

// 此处由原分片创建一个通知交易，用于通知目标分片迁移计划
func (p *PbftConsensusNode) CreateTxInitInfo(shardid int, targetShard int, accountaddr utils.Address) {
	createtxinitinfo := new(message.TxinitCreate)
	createtxinitinfo.SendershardID = uint64(shardid)
	accounts := p.CurChain.FetchAccounts([]string{string(accountaddr)})
	if len(accounts) == 0 {
		p.pl.Plog.Printf("没有这个账户，返回空值\n\n\n")
		return
	}
	createtxinitinfo.MigAccount_State = *accounts[0]
	// createtxinitinfo.MigAccount_State = *accounts[0]
	createtxinitinfo.Nowtime = time.Now()
	createtxinitinfo.TransientTxAddr = accountaddr
	p.pl.Plog.Printf("输出一下账户状态喵~\n")
	p.pl.Plog.Print(createtxinitinfo.MigAccount_State)
	p.pl.Plog.Print("\n")
	ctibyte, err := json.Marshal(createtxinitinfo)
	if err != nil {
		log.Panic(err)
	}
	cmsg := message.MergeMessage(message.CTxinitCreate, ctibyte)
	// 发送消息
	p.pl.Plog.Printf("i have send this msg to Desshard :%d\n\n\n\n\n", targetShard)
	go networks.TcpDial(cmsg, p.ip_nodeTable[uint64(targetShard)][0])
}
