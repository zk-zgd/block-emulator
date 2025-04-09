package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/partition"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/utils"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"math/big"
	"os"
	"sync"
	"time"
)

type RelayCommitteeModule struct {
	csvPath      string
	dataTotalNum int
	nowDataNum   int
	batchDataNum int
	IpNodeTable  map[uint64]map[uint64]string
	sl           *supervisor_log.SupervisorLog
	Ss           *signal.StopSignal // to control the stop message sending

	// CLPA算法使用
	curEpoch            int32
	clpaLock            sync.Mutex
	clpaGraph           *partition.CLPAState
	modifiedMap         map[string]uint64
	clpaLastRunningTime time.Time
	clpaFreq            int
}

func NewRelayCommitteeModule(Ip_nodeTable map[uint64]map[uint64]string, Ss *signal.StopSignal, slog *supervisor_log.SupervisorLog, csvFilePath string, dataNum, batchNum int) *RelayCommitteeModule {
	cg := new(partition.CLPAState)
	cg.Init_CLPAState(0.5, 100, params.ShardNum-1)
	return &RelayCommitteeModule{
		csvPath:      csvFilePath,
		dataTotalNum: dataNum,
		batchDataNum: batchNum,
		nowDataNum:   0,
		IpNodeTable:  Ip_nodeTable,
		Ss:           Ss,
		sl:           slog,

		clpaGraph:           cg,
		modifiedMap:         make(map[string]uint64),
		clpaFreq:            params.ReconfigTimeGap,
		clpaLastRunningTime: time.Time{},
	}
}

// transfrom, data to transaction
// check whether it is a legal txs meesage. if so, read txs and put it into the txlist
func data2tx(data []string, nonce uint64) (*core.Transaction, bool) {
	if data[6] == "0" && data[7] == "0" && len(data[3]) > 16 && len(data[4]) > 16 && data[3] != data[4] {
		val, ok := new(big.Int).SetString(data[8], 10)
		if !ok {
			log.Panic("new int failed\n")
		}
		tx := core.NewTransaction(data[3][2:], data[4][2:], val, nonce, time.Now())
		return tx, true
	}
	return &core.Transaction{}, false
}

// 取账户
func (rthm *RelayCommitteeModule) fetchModifiedMap(key string) uint64 {
	if val, ok := rthm.modifiedMap[key]; !ok {
		return uint64(utils.Addr2Shard(key))
	} else {
		return val
	}
}

func (rthm *RelayCommitteeModule) HandleOtherMessage([]byte) {}

func (rthm *RelayCommitteeModule) txSending(txlist []*core.Transaction) {
	// the txs will be sent
	sendToShard := make(map[uint64][]*core.Transaction)

	for idx := 0; idx <= len(txlist); idx++ {
		if idx > 0 && (idx%params.InjectSpeed == 0 || idx == len(txlist)) {
			// send to shard
			for sid := uint64(0); sid < uint64(params.ShardNum-1); sid++ {
				it := message.InjectTxs{
					Txs:       sendToShard[sid],
					ToShardID: sid,
				}
				itByte, err := json.Marshal(it)
				if err != nil {
					log.Panic(err)
				}
				send_msg := message.MergeMessage(message.CInject, itByte)
				go networks.TcpDial(send_msg, rthm.IpNodeTable[sid][0])
			}
			sendToShard = make(map[uint64][]*core.Transaction)
			time.Sleep(time.Second)
		}
		if idx == len(txlist) {
			break
		}
		tx := txlist[idx]
		sendersid := rthm.fetchModifiedMap(tx.Sender)
		sendToShard[sendersid] = append(sendToShard[sendersid], tx)
	}
}

// read transactions, the Number of the transactions is - batchDataNum
func (rthm *RelayCommitteeModule) MsgSendingControl() {
	txfile, err := os.Open(rthm.csvPath)
	if err != nil {
		log.Panic(err)
	}
	defer txfile.Close()
	reader := csv.NewReader(txfile)
	txlist := make([]*core.Transaction, 0) // save the txs in this epoch (round)

	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panic(err)
		}
		if tx, ok := data2tx(data, uint64(rthm.nowDataNum)); ok {
			txlist = append(txlist, tx)
			rthm.nowDataNum++
		}

		// re-shard condition, enough edges
		if len(txlist) == int(rthm.batchDataNum) || rthm.nowDataNum == rthm.dataTotalNum {
			rthm.txSending(txlist)
			// reset the variants about tx sending
			txlist = make([]*core.Transaction, 0)
			rthm.Ss.StopGap_Reset()
		}

		if rthm.nowDataNum == rthm.dataTotalNum {
			break
		}
	}
}

func (rthm *RelayCommitteeModule) clpaMapSend(m map[string]uint64, reqshardID uint64) {
	// send partition modified Map message

	pm := new(message.PlanOutMsg)
	pm.PlanOuts.Plans = make([]*message.MigPlan, 0)
	pm.PlanOuts.ReqPlanShardID = reqshardID
	for addr, shardid := range m {
		msgplan := new(message.MigPlan)
		msgplan.AccountAddr = addr
		msgplan.ReceiverShardID = shardid
		pm.PlanOuts.Plans = append(pm.PlanOuts.Plans, msgplan)
	}
	rthm.sl.Slog.Println(pm.PlanOuts.Plans)
	pmByte, err := json.Marshal(pm)
	if err != nil {
		log.Panic()
	}
	// 发送的消息要变换成CPlanOut
	send_msg := message.MergeMessage(message.CTxPlanOut, pmByte)
	// send to worker shards

	go networks.TcpDial(send_msg, rthm.IpNodeTable[reqshardID][0])
	rthm.sl.Slog.Println("Supervisor: all partition map message has been sent. ")
}

func (rthm *RelayCommitteeModule) clpaReset() {
	rthm.clpaGraph = new(partition.CLPAState)
	rthm.clpaGraph.Init_CLPAState(0.5, 100, params.ShardNum)
	/*
		for key, val := range rthm.modifiedMap {
			rthm.clpaGraph.PartitionMap[partition.Vertex{Addr: key}] = int(val)
		}
	*/
}

// no operation here
func (rthm *RelayCommitteeModule) HandleBlockInfo(b *message.BlockInfoMsg) {

	rthm.sl.Slog.Printf("received from shard %d in epoch %d.\n", b.SenderShardID, b.Epoch)
	rthm.clpaLock.Lock()
	for _, tx := range b.InnerShardTxs {
		rthm.clpaGraph.AddEdge(partition.Vertex{Addr: tx.Sender}, partition.Vertex{Addr: tx.Recipient})
	}
	for _, r2tx := range b.Relay2Txs {
		rthm.clpaGraph.AddEdge(partition.Vertex{Addr: r2tx.Sender}, partition.Vertex{Addr: r2tx.Recipient})
	}
	rthm.clpaLock.Unlock()
	rthm.sl.Slog.Print("现在开始检查当前block中有无Txreq交易，如果有的话进行CLPA操作\n\n")
	for _, tx := range b.Relay2Txs {
		if tx.Is_Txreq {
			rthm.sl.Slog.Println("supervisor启动图划分算法")
			rthm.clpaLock.Lock()
			mmap, _ := rthm.clpaGraph.CLPA_Partition()
			rthm.sl.Slog.Println(mmap)
			rthm.clpaMapSend(mmap, uint64(utils.Addr2Shard(tx.Sender)))
			for key, val := range mmap {
				rthm.modifiedMap[key] = val
			}
			rthm.clpaReset()
			rthm.clpaLock.Unlock()

			rthm.clpaLastRunningTime = time.Now()
			rthm.sl.Slog.Println("Next CLPA epoch begins. ")
		}
	}
}
