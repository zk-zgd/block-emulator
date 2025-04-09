package message

var (
	TransientAccountCreateReq    RequestType = "AccountCreateReq" // 创建替身账户的请求
	CTransientAccountCreateReady MessageType = "transientaccount" // 创建替身账户的准备消息
	CReady2AddAccount            MessageType = "toaddaccount"     // 准备过渡账户添加的消息头
)

type TransientAccountCreateReady struct {
	FromShard uint64
	NowSeqID  uint64
}

// 主分片通知所有分片准备好进行添加账户操作（同步）
type Ready2AddAccount struct {
	// content []byte
	ShardID uint64
}
