// Supervisor is an abstract role in this simulator that may read txs, generate partition infos,
// and handle history data.

package supervisor

import (
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/supervisor/committee"
	"blockEmulator/supervisor/measure"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type Supervisor struct {
	// basic infos
	IPaddr       string // ip address of this Supervisor
	ChainConfig  *params.ChainConfig
	Ip_nodeTable map[uint64]map[uint64]string

	// tcp control
	listenStop bool
	tcpLn      net.Listener
	tcpLock    sync.Mutex
	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss *signal.StopSignal // to control the stop message sending

	// supervisor and committee components
	comMod committee.CommitteeModule

	// measure components
	testMeasureMods []measure.MeasureModule

	// diy, add more structures or classes here ...
}

func (d *Supervisor) NewSupervisor(ip string, pcc *params.ChainConfig, committeeMethod string, measureModNames ...string) {
	d.IPaddr = ip
	d.ChainConfig = pcc
	d.Ip_nodeTable = params.IPmap_nodeTable

	d.sl = supervisor_log.NewSupervisorLog()

	d.Ss = signal.NewStopSignal(3 * int(pcc.ShardNums))

	switch committeeMethod {
	case "CLPA_Broker":
		d.comMod = committee.NewCLPACommitteeMod_Broker(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize, params.ReconfigTimeGap)
	case "CLPA":
		d.comMod = committee.NewCLPACommitteeModule(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize, params.ReconfigTimeGap)
	case "Broker":
		d.comMod = committee.NewBrokerCommitteeMod(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize)
	default:
		d.comMod = committee.NewRelayCommitteeModule(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize)
	}

	d.testMeasureMods = make([]measure.MeasureModule, 0)
	for _, mModName := range measureModNames {
		switch mModName {
		case "TPS_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_avgTPS_Relay())
		case "TPS_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_avgTPS_Broker())
		case "TCL_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_TCL_Relay())
		case "TCL_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_TCL_Broker())
		case "CrossTxRate_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestCrossTxRate_Relay())
		case "CrossTxRate_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestCrossTxRate_Broker())
		case "TxNumberCount_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestTxNumCount_Relay())
		case "TxNumberCount_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestTxNumCount_Broker())
		case "Tx_Details":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestTxDetail())
		default:
		}
	}
}

// Supervisor received the block information from the leaders, and handle these
// message to measure the performances.
func (d *Supervisor) handleBlockInfos(content []byte) {
	bim := new(message.BlockInfoMsg)
	err := json.Unmarshal(content, bim)
	if err != nil {
		log.Panic()
	}
	// StopSignal check
	if bim.BlockBodyLength == 0 {
		d.Ss.StopGap_Inc()
	} else {
		d.Ss.StopGap_Reset()
	}

	d.comMod.HandleBlockInfo(bim)

	// 在这里处理数据库读取操作
	/*
		if bim.ConsensusRound > 0 && bim.ConsensusRound%5 == 0 {
			measure.ReadBoltDB(int(bim.NowShardID), 0)
		}
	*/

	// 在这里处理数据库读取操作--采用getblock的办法

	// measure update

	for _, measureMod := range d.testMeasureMods {
		measureMod.UpdateMeasureRecord(bim)
	}

	// add codes here ...
}

// 进行消息的读取:
/*
func (d *Supervisor) handleBlock2Supervisor(content []byte) {
	// 读取消息内容
	msg2supervisor := new(message.Msg2Supervisor)
	err := json.Unmarshal(content, msg2supervisor)
	if err != nil {
		log.Panic()
	}
	// 创建存储目录
	dir := "ReadBlock"
	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		log.Fatalf("无法创建目录: %v", err)
	}

	// 构建 CSV 文件路径
	fileName := fmt.Sprintf("%s/shard_%d_node_%d_blocks.csv", dir, msg2supervisor.ShardID, msg2supervisor.NodeID)

	// 检查文件是否存在
	var file *os.File
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		// 文件不存在，创建新文件
		file, err = os.Create(fileName)
		if err != nil {
			log.Fatalf("无法创建CSV文件: %v", err)
		}
		// 创建 CSV 写入器并写入表头
		writer := csv.NewWriter(file)
		header := []string{"BlockNumber", "BlockHash", "ParentHash", "TxHash", "Sender", "Recipient", "Value"}
		if err := writer.Write(header); err != nil {
			log.Fatalf("无法写入CSV表头: %v", err)
		}
		writer.Flush()
	} else {
		// 文件存在，追加模式打开
		file, err = os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("无法打开CSV文件: %v", err)
		}
	}
	defer file.Close()

	// 创建CSV写入器用于追加数据
	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, block := range msg2supervisor.Block {
		for _, tx := range block.Body {
			record := []string{
				strconv.FormatUint(block.Header.Number, 10),
				fmt.Sprintf("%x", block.Hash),
				fmt.Sprintf("%x", block.Header.ParentBlockHash),
				fmt.Sprintf("%x", tx.TxHash),
				tx.Sender,
				tx.Recipient,
				tx.Value.String(),
			}
			if err := writer.Write(record); err != nil {
				log.Fatalf("无法写入交易记录到CSV: %v", err)
			}
		}
	}
	writer.Flush()

	d.sl.Slog.Printf("Supervisor: received block from shard %d, node %d\n", msg2supervisor.ShardID, msg2supervisor.NodeID)
}*/

// read transactions from dataFile. When the number of data is enough,
// the Supervisor will do re-partition and send partitionMSG and txs to leaders.
func (d *Supervisor) SupervisorTxHandling() {
	d.comMod.MsgSendingControl()
	// TxHandling is end
	for !d.Ss.GapEnough() { // wait all txs to be handled
		time.Sleep(time.Second)
	}
	// send stop message
	stopmsg := message.MergeMessage(message.CStop, []byte("this is a stop message~"))
	d.sl.Slog.Println("Supervisor: now sending cstop message to all nodes")
	for sid := uint64(0); sid < d.ChainConfig.ShardNums; sid++ {
		for nid := uint64(0); nid < d.ChainConfig.Nodes_perShard; nid++ {
			networks.TcpDial(stopmsg, d.Ip_nodeTable[sid][nid])
		}
	}
	// make sure all stop messages are sent.
	time.Sleep(time.Duration(params.Delay+params.JitterRange+3) * time.Millisecond)

	d.sl.Slog.Println("Supervisor: now Closing")
	d.listenStop = true
	d.CloseSupervisor()
}

// handle message. only one message to be handled now
func (d *Supervisor) handleMessage(msg []byte) {
	msgType, content := message.SplitMessage(msg)
	switch msgType {
	case message.CBlockInfo:
		d.handleBlockInfos(content)
		// add codes for more functionality
	// case message.CBlockHash2Supervisor:
	// d.handleBlock2Supervisor(content)
	default:
		d.comMod.HandleOtherMessage(msg)
		for _, mm := range d.testMeasureMods {
			mm.HandleExtraMessage(msg)
		}
	}
}

func (d *Supervisor) handleClientRequest(con net.Conn) {
	defer con.Close()
	clientReader := bufio.NewReader(con)
	for {
		clientRequest, err := clientReader.ReadBytes('\n')
		switch err {
		case nil:
			d.tcpLock.Lock()
			d.handleMessage(clientRequest)
			d.tcpLock.Unlock()
		case io.EOF:
			log.Println("client closed the connection by terminating the process")
			return
		default:
			log.Printf("error: %v\n", err)
			return
		}
	}
}

func (d *Supervisor) TcpListen() {
	ln, err := net.Listen("tcp", d.IPaddr)
	if err != nil {
		log.Panic(err)
	}
	d.tcpLn = ln
	for {
		conn, err := d.tcpLn.Accept()
		if err != nil {
			return
		}
		go d.handleClientRequest(conn)
	}
}

// close Supervisor, and record the data in .csv file
func (d *Supervisor) CloseSupervisor() {
	d.sl.Slog.Println("Closing...")
	for _, measureMod := range d.testMeasureMods {
		d.sl.Slog.Println(measureMod.OutputMetricName())
		d.sl.Slog.Println(measureMod.OutputRecord())
		println()
	}
	networks.CloseAllConnInPool()
	d.tcpLn.Close()
}
