package params

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

var (
	// The following parameters can be set in main.go.
	// default values:
	NodesInShard = 4 // \# of Nodes in a shard.
	ShardNum     = 5 // \# of shards.
	// txreq专用地址
	SenderAddr = []string{"0000000000000000000000000000", "0000000000000000000000000001", "0000000000000000000000000002", "0000000000000000000000000003"} // The address of the sender. The sender is the node that sends the Txreq.
	// TXinit专用地址
	TxinitSenderAddr = []string{"1000000000000000000000000000", "1000000000000000000000000001", "1000000000000000000000000002", "1000000000000000000000000003"} // The address of the sender. The sender is the node that sends the Txinit.
	// The address of the receiver. The receiver is the node that receives the Txinit.
	TxinitSpecialReceiver = "12345678987654321"

	// Block represents a blockchain block

	// 记录txinitroot的变化

)
var Changetxinitroot []byte
var Cishu = false

// 添加一个txpool锁，如果当前DirtyMap中有账户，就将相关交易锁定，打包发送给目标分片，直到所有分片处理结束

// 每生成一个块就自增
var ConsensusRoundForEvrShard = []int{0, 0, 0, 0, 0}

var Time time.Time

// 迁移计划结构体
type MigPlan struct {
	AccountAddr     string // 账户地址
	ReceiverShardID uint64 // 账户所在分片
}

// 待下发的迁移计划（用在消息处理里面）
type PlanOut struct {
	ReqPlanShardID uint64     // 账户目的分片
	Plans          []*MigPlan // 一组迁移计划
}

var PlanOuts = PlanOut{
	ReqPlanShardID: 0,
	Plans:          make([]*MigPlan, 0),
}





// consensus layer & output file path
var (
	ConsensusMethod = 3 // ConsensusMethod an Integer, which indicates the choice ID of methods / consensuses. Value range: [0, 4), representing [CLPA_Broker, CLPA, Broker, Relay]"

	PbftViewChangeTimeOut = 2000000000000000000 // The view change threshold of pbft. If the process of PBFT is too slow, the view change mechanism will be triggered.

	Block_Interval = 5000 // The time interval for generating a new block

	MaxBlockSize_global = 2000  // The maximum number of transactions a block contains
	BlocksizeInBytes    = 20000 // The maximum size (in bytes) of block body
	UseBlocksizeInBytes = 0     // Use blocksizeInBytes as the blocksize measurement if '1'.

	InjectSpeed   = 160000 // The speed of transaction injection
	TotalDataSize = 160000 // The total number of txs to be injected
	TxBatchSize   = 160000 // The supervisor read a batch of txs then send them. The size of a batch is 'TxBatchSize'

	BrokerNum            = 10 // The # of Broker accounts used in Broker / CLPA_Broker.
	RelayWithMerkleProof = 0  // When using a consensus about "Relay", nodes will send Tx Relay with proof if "RelayWithMerkleProof" = 1

	ExpDataRootDir     = "expTest"                     // The root dir where the experimental data should locate.
	DataWrite_path     = ExpDataRootDir + "/result/"   // Measurement data result output path
	LogWrite_path      = ExpDataRootDir + "/log"       // Log output path
	DatabaseWrite_path = ExpDataRootDir + "/database/" // database write path

	MainShardDataRootDir = "ReadData" // 主分片进行访问数据库读取数据的根目录

	SupervisorAddr = "127.0.0.1:18800"                         // Supervisor ip address
	DatasetFile    = `./2000000to2999999_BlockTransaction.csv` // The raw BlockTransaction data path

	ReconfigTimeGap = 50 // The time gap between epochs. This variable is only used in CLPA / CLPA_Broker now.
)

// network layer
var (
	Delay       int // The delay of network (ms) when sending. 0 if delay < 0
	JitterRange int // The jitter range of delay (ms). Jitter follows a uniform distribution. 0 if JitterRange < 0.
	Bandwidth   int // The bandwidth limit (Bytes). +inf if bandwidth < 0
)

var (
	AddrNum  = 0
	Addrnum1 = 0
)

// read from file
type globalConfig struct {
	ConsensusMethod int `json:"ConsensusMethod"`

	PbftViewChangeTimeOut int `json:"PbftViewChangeTimeOut"`

	ExpDataRootDir string `json:"ExpDataRootDir"`

	BlockInterval int `json:"Block_Interval"`

	BlocksizeInBytes    int `json:"BlocksizeInBytes"`
	MaxBlockSizeGlobal  int `json:"BlockSize"`
	UseBlocksizeInBytes int `json:"UseBlocksizeInBytes"`

	InjectSpeed   int `json:"InjectSpeed"`
	TotalDataSize int `json:"TotalDataSize"`

	TxBatchSize          int    `json:"TxBatchSize"`
	BrokerNum            int    `json:"BrokerNum"`
	RelayWithMerkleProof int    `json:"RelayWithMerkleProof"`
	DatasetFile          string `json:"DatasetFile"`
	ReconfigTimeGap      int    `json:"ReconfigTimeGap"`

	Delay       int `json:"Delay"`
	JitterRange int `json:"JitterRange"`
	Bandwidth   int `json:"Bandwidth"`
}

func ReadConfigFile() {
	// read configurations from paramsConfig.json
	data, err := os.ReadFile("paramsConfig.json")
	if err != nil {
		log.Fatalf("Error reading file: %v", err)
	}
	var config globalConfig
	err = json.Unmarshal(data, &config)
	if err != nil {
		log.Fatalf("Error unmarshalling JSON: %v", err)
	}

	// output configurations
	fmt.Printf("Config: %+v\n", config)

	// set configurations to params
	// consensus params
	ConsensusMethod = config.ConsensusMethod

	PbftViewChangeTimeOut = config.PbftViewChangeTimeOut

	// data file params
	ExpDataRootDir = config.ExpDataRootDir
	DataWrite_path = ExpDataRootDir + "/result/"
	LogWrite_path = ExpDataRootDir + "/log"
	DatabaseWrite_path = ExpDataRootDir + "/database/"

	Block_Interval = config.BlockInterval

	MaxBlockSize_global = config.MaxBlockSizeGlobal
	BlocksizeInBytes = config.BlocksizeInBytes
	UseBlocksizeInBytes = config.UseBlocksizeInBytes

	InjectSpeed = config.InjectSpeed
	TotalDataSize = config.TotalDataSize
	TxBatchSize = config.TxBatchSize

	BrokerNum = config.BrokerNum
	RelayWithMerkleProof = config.RelayWithMerkleProof
	DatasetFile = config.DatasetFile

	ReconfigTimeGap = config.ReconfigTimeGap

	// network params
	Delay = config.Delay
	JitterRange = config.JitterRange
	Bandwidth = config.Bandwidth
}
