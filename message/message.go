package message

import (
	"blockEmulator/core"
	"blockEmulator/shard"
	"blockEmulator/utils"
	"time"
)

var prefixMSGtypeLen = 100

type MessageType string
type RequestType string

const (
	CPrePrepare        MessageType = "preprepare"
	CPrepare           MessageType = "prepare"
	CCommit            MessageType = "commit"
	CRequestOldrequest MessageType = "requestOldrequest"
	CSendOldrequest    MessageType = "sendOldrequest"
	CStop              MessageType = "stop"

	CRelay          MessageType = "relay"
	CRelayWithProof MessageType = "CRelay&Proof"
	CInject         MessageType = "inject"

	CBlockInfo MessageType = "BlockInfo"
	CSeqIDinfo MessageType = "SequenceID"

	// 主分片收到TxReq后下发迁移计划
	CTxPlanOut MessageType = "TxPlanOut"
	// 原分片创建TxInit消息头
	CTxinitCreate MessageType = "TxinitCreate"

	// Txinit中继交易的消息头
	CTxinitRelyay MessageType = "TxinitRelay"
	// 新的阶段4的Txredirect交易重定向消息的消息头
	CTxRedirect MessageType = "Txredirect"
	// 发送给其他分片的交易的消息头
	CTxRedirectout MessageType = "TxRedirectout"

	// 发送数据给主分片
	CDataSet2Mshard MessageType = "CDataSet2Mshard"

	// 发送块哈希给supervisor节点的消息头
	CBlockHash2Supervisor MessageType = "BlockHash2Supervisor"
)

var (
	BlockRequest RequestType = "Block"
	GraphRequest RequestType = "Graph" // 图划分请求

	// add more types
	// ...
)

// 迁移计划结构体
type MigPlan struct {
	AccountAddr     utils.Address     // 账户地址
	ReceiverShardID uint64            // 账户所在分片
	TransientState  core.AccountState // 目标分片创建替身账户需要用到的状态
}

// 待下发的迁移计划（用在消息处理里面）
type PlanOut struct {
	ReqPlanShardID uint64     // 账户目的分片
	Plans          []*MigPlan // 一组迁移计划
}

// 创建TxInit的消息体
type TxinitCreate struct {
	TransientTxAddr  utils.Address     // 过渡账户地址
	Nowtime          time.Time         // 当前时间
	SendershardID    uint64            // 发送分片ID
	MigAccount_State core.AccountState // 迁移账户状态（将用于替身账户的生成）
}

// 待下发的迁移计划（用在消息处理里面）
type PlanOutMsg struct {
	PlanOuts PlanOut
}

type RawMessage struct {
	Content []byte // the content of raw message, txs and blocks (most cases) included
}

type Request struct {
	RequestType RequestType
	Msg         RawMessage // request message
	ReqTime     time.Time  // request time
}

type PrePrepare struct {
	RequestMsg *Request // the request message should be pre-prepared
	Digest     []byte   // the digest of this request, which is the only identifier
	SeqID      uint64
}

type Prepare struct {
	Digest     []byte // To identify which request is prepared by this node
	SeqID      uint64
	SenderNode *shard.Node // To identify who send this message
}

type Commit struct {
	Digest     []byte // To identify which request is prepared by this node
	SeqID      uint64
	SenderNode *shard.Node // To identify who send this message
}

type Reply struct {
	MessageID  uint64
	SenderNode *shard.Node
	Result     bool
}

type RequestOldMessage struct {
	SeqStartHeight uint64
	SeqEndHeight   uint64
	ServerNode     *shard.Node // send this request to the server node
	SenderNode     *shard.Node
}

type SendOldMessage struct {
	SeqStartHeight uint64
	SeqEndHeight   uint64
	OldRequest     []*Request
	SenderNode     *shard.Node
}

type InjectTxs struct {
	Txs       []*core.Transaction
	ToShardID uint64
}

// data sent to the supervisor
type BlockInfoMsg struct {
	BlockBodyLength int
	InnerShardTxs   []*core.Transaction // txs which are innerShard
	Epoch           int

	ProposeTime   time.Time // record the propose time of this block (txs)
	CommitTime    time.Time // record the commit time of this block (txs)
	SenderShardID uint64

	// for transaction relay
	Relay1Txs []*core.Transaction // relay1 transactions in chain first time
	Relay2Txs []*core.Transaction // relay2 transactions in chain second time

	// for broker
	Broker1Txs []*core.Transaction // cross transactions at first time by broker
	Broker2Txs []*core.Transaction // cross transactions at second time by broker

	// 共识轮
	ConsensusRound int
	// 当前消息的分片号, 用于查询当前分片的数据库所用
	NowShardID uint64
}

type SeqIDinfo struct {
	SenderShardID uint64
	SenderSeq     uint64
}

func MergeMessage(msgType MessageType, content []byte) []byte {
	b := make([]byte, prefixMSGtypeLen)
	for i, v := range []byte(msgType) {
		b[i] = v
	}
	merge := append(b, content...)
	return merge
}

func SplitMessage(message []byte) (MessageType, []byte) {
	// fmt.Println(message[prefixMSGtypeLen:])

	msgTypeBytes := message[:prefixMSGtypeLen]
	msgType_pruned := make([]byte, 0)
	for _, v := range msgTypeBytes {
		if v != byte(0) {
			msgType_pruned = append(msgType_pruned, v)
		}
	}
	msgType := string(msgType_pruned)
	content := message[prefixMSGtypeLen:]
	return MessageType(msgType), content
}
