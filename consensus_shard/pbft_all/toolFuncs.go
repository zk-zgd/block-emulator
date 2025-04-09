package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/query"
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

// 此处进行生成生成relaytxinit消息的处理哦，新增merkle证明捏-chx
func (p *PbftConsensusNode) TxinitRelayMsgSend(block *core.Block) {
	for sid := uint64(0); sid < p.pbftChainConfig.ShardNums; sid++ {
		if sid == p.ShardID {
			continue
		}

		txHashes := make([][]byte, len(p.CurChain.Txinitpool.RelayPool[sid]))
		if len(p.CurChain.Txinitpool.RelayPool[sid]) >= 0 {
			p.pl.Plog.Printf("当前TXinit的中继交易池有%d个交易\n\n\n", len(p.CurChain.Txinitpool.RelayPool[sid]))
		}
		for i, txinit := range p.CurChain.Txinitpool.RelayPool[sid] {
			txHashes[i] = txinit.TxHash[:]
		}
		txProofs := chain.TxInitProofBatchGenerateOnBlock(txHashes, block)

		rwp := message.Txinit_Relay{
			Txs:           p.CurChain.Txinitpool.RelayPool[sid],
			TxProofs:      txProofs,
			SenderShardID: p.ShardID,
			SenderSeq:     p.sequenceID,
		}
		rByte, err := json.Marshal(rwp)
		if err != nil {
			log.Panic()
		}
		msg_send := message.MergeMessage(message.CTxinitRelyay, rByte)
		go networks.TcpDial(msg_send, p.ip_nodeTable[sid][0])
		p.pl.Plog.Printf("S%dN%d : sended relay txs & proofs to %d\n", p.ShardID, p.NodeID, sid)
	}
	p.CurChain.Txinitpool.ClearRelayPool()
}

// delete the txs in blocks. This list should be locked before calling this func.
func DeleteElementsInList(list []*core.Transaction, elements []*core.Transaction) []*core.Transaction {
	// 用 map 存储要删除的交易哈希
	elementHashMap := make(map[string]bool)
	for _, element := range elements {
		elementHashMap[string(element.TxHash)] = true
	}

	// 双指针，原地修改 list
	left := 0
	for right := 0; right < len(list); right++ {
		if _, ok := elementHashMap[string(list[right].TxHash)]; !ok {
			list[left] = list[right]
			left++
		}
	}
	// 截断原切片，返回前 left 个元素
	return list[:left]
}

// 此处新建一个TXreq交易
func (p *PbftConsensusNode) CreateTxReq(shardid int) {
	// 创建一个新的交易
	txReq := core.NewTransaction(params.SenderAddr[shardid], params.SenderAddr[3], params.Init_Balance, 0, time.Now()) // txReq.TxHash = sha256.Sum256([]byte(fmt.Sprintf("%v", time.Now()))).[:]
	txReq.Is_Txreq = true
	hash := sha256.Sum256(txReq.Encode())
	txReq.TxHash = hash[:]
	// 添加交易到交易池
	p.pl.Plog.Printf("\n\n\n\n\n当前创建TXreq的分片是: %d\n\n\n\n\n", shardid)
	p.CurChain.Txpool.AddTxs2Pool_Head([]*core.Transaction{txReq})
}

// 此处由原分片创建一个通知交易，用于通知目标分片迁移计划
func (p *PbftConsensusNode) CreateTxInitInfo(shardid int, targetShard int, accountaddr utils.Address) {
	createtxinitinfo := new(message.TxinitCreate)
	createtxinitinfo.SendershardID = uint64(shardid)
	accounts := p.CurChain.FetchAccounts([]string{string(accountaddr)})
	if p.CurChain.Get_PartitionMap(accountaddr) != p.ShardID {
		if p.CurChain.Get_PartitionMap(accountaddr) != uint64(utils.Addr2Shard(accountaddr)) {
			p.pl.Plog.Print("当前账户已经被牵走了，不用管他\n\n")
			return
		}
	}
	if len(accounts) == 0 {
		p.pl.Plog.Printf("没有这个账户，返回空值\n\n\n")
		return
	}
	p.CurChain.AddDirtyMap(accountaddr, uint64(targetShard))
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
	params.Time = time.Now()
	p.pl.Plog.Print("\n\n初始时间是: ")
	p.pl.Plog.Println(params.Time.Second())
	// 发送消息
	p.pl.Plog.Printf("i have send this msg to Desshard :%d\n\n\n\n\n", targetShard)
	go networks.TcpDial(cmsg, p.ip_nodeTable[uint64(targetShard)][0])

}

// 此处创建一个广播交易Txinit
func (p *PbftConsensusNode) New_TxInit(transientaccountaddr utils.Address) {
	p.pl.Plog.Print("开始创建TXinit咯~\n\n")
	// 开始创建TXinit咯
	txinit := core.NewTxinitTransaction(params.TxinitSenderAddr[p.ShardID], transientaccountaddr, p.ShardID, time.Now())
	// 打印过渡账户状态
	p.pl.Plog.Print(txinit)
	p.CurChain.Txinitpool.AddTxInit2Pool(txinit)
}

func exportShardBlocks(shardID int, nodeID int) {
	// 创建存储目录
	dir := "ReadBlock"
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		log.Fatalf("无法创建目录: %v", err)
	}

	// 构建 CSV 文件路径
	fileName := fmt.Sprintf("%s/shard_%d_node_%d_blocks.csv", dir, shardID, nodeID)
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("无法创建CSV文件: %v", err)
	}
	defer file.Close()

	// 创建 CSV 写入器
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入 CSV 文件的表头
	header := []string{"BlockNumber", "BlockHash", "ParentHash", "TxHash", "Sender", "Recipient", "Value"}
	if err := writer.Write(header); err != nil {
		log.Fatalf("无法写入CSV表头: %v", err)
	}

	// 初始化数据库路径
	// dbPath := fmt.Sprintf("expTest/database/chainDB/S%d_N%d", shardID, nodeID)
	block := query.QueryBlock2(uint64(shardID), uint64(nodeID), 1)
	for _, tx := range block.Body {
		record := []string{
			strconv.FormatUint(block.Header.Number, 10),     // 区块号
			fmt.Sprintf("%x", block.Hash),                   // 区块哈希
			fmt.Sprintf("%x", block.Header.ParentBlockHash), // 父区块哈希
			fmt.Sprintf("%x", tx.TxHash),                    // 交易哈希
			tx.Sender,                                       // 发送者
			tx.Recipient,                                    // 接收者
			tx.Value.String(),                               // 交易金额
		}
		if err := writer.Write(record); err != nil {
			log.Fatalf("无法写入交易记录到CSV: %v", err)
		}
	}

	// 获取最新区块哈希
	/*
		newestHash, err := storage.GetNewestBlockHash()
		if err != nil {
			log.Fatalf("无法获取分片 %d 节点 %d 的最新区块哈希: %v", shardID, nodeID, err)
		}

		// 遍历分片中的所有区块
		currentHash := newestHash
		for currentHash != nil {
			// 获取当前区块
			block, err := storage.GetBlock(currentHash)
			if err != nil {
				log.Fatalf("无法获取分片 %d 节点 %d 的区块: %v", shardID, nodeID, err)
			}

			// 遍历区块中的交易
			for _, tx := range block.Body {
				// 将交易数据写入 CSV 文件
				record := []string{
					strconv.FormatUint(block.Header.Number, 10),     // 区块号
					fmt.Sprintf("%x", block.Hash),                   // 区块哈希
					fmt.Sprintf("%x", block.Header.ParentBlockHash), // 父区块哈希
					fmt.Sprintf("%x", tx.TxHash),                    // 交易哈希
					tx.Sender,                                       // 发送者
					tx.Recipient,                                    // 接收者
					tx.Value.String(),                               // 交易金额
				}
				if err := writer.Write(record); err != nil {
					log.Fatalf("无法写入交易记录到CSV: %v", err)
				}
			}

			// 获取父区块哈希
			currentHash = block.Header.ParentBlockHash
		}
	*/

	fmt.Printf("分片 %d 节点 %d 的区块数据已成功导出到 %s 文件中！\n", shardID, nodeID, fileName)
}

// 用于发送相关块的数据给主分片
/*
func (p *PbftConsensusNode) SendMessage2MShard(shardid uint64, nodeid uint64) {
	// 初始化
	dataset2mshard := new(message.Dataset2mshard)
	nowhash, err := p.CurChain.Storage.GetNewestBlockHash()
	if err != nil {
		log.Panic(err)
	}
	dataset2mshard.Blocks = make([]*core.Block, 6)
	// 读取五个块的数据
	for i := 0; i < 6; i++ {
		dataset2mshard.Blocks[i], err = p.CurChain.Storage.GetBlock(nowhash)
		if err != nil {
			p.pl.Plog.Println(i)
			for {
				dataset2mshard.Blocks[i], err = p.CurChain.Storage.GetBlock(nowhash)
				if err != nil {
					continue
				} else {
					break
				}
			}
			// log.Panic(err)
		}
		nowhash = dataset2mshard.Blocks[i].Header.ParentBlockHash
	}
	// 打包消息
	dataset2mshard.NodeID = p.NodeID
	dataset2mshard.ShardID = p.ShardID

	ctibyte, err := json.Marshal(dataset2mshard)
	if err != nil {
		log.Panic(err)
	}
	cmsg := message.MergeMessage(message.CDataSet2Mshard, ctibyte)
	networks.TcpDial(cmsg, p.ip_nodeTable[uint64(params.ShardNum)-1][0])
}
*/

func (p *PbftConsensusNode) Informallshardcreatetransientaccount() {
	// 告知所有分片进行准备
	for i := 0; i < params.ShardNum; i++ {
		ready2addaccount := new(message.Ready2AddAccount)
		ready2addaccount.ShardID = uint64(i)
		rbytes, err := json.Marshal(ready2addaccount)
		if err != nil {
			log.Panic()
		}
		rmsg := message.MergeMessage(message.CReady2AddAccount, rbytes)
		go networks.TcpDial(rmsg, p.ip_nodeTable[uint64(i)][0])
	}
}
