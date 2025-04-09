package measure

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"encoding/csv"
	"fmt"
	"log"
	"os"
)

func WriteMetricsToCSV(fileName string, colName []string, colVals [][]string) {
	// Construct directory path
	dirpath := params.DataWrite_path + "supervisor_measureOutput/"
	if err := os.MkdirAll(dirpath, os.ModePerm); err != nil {
		log.Panic(err)
	}

	// Construct target file path
	targetPath := dirpath + fileName + ".csv"

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
		if err := writer.Write(colName); err != nil {
			log.Panic(err)
		}
		writer.Flush()
	}

	// Write data
	for _, metricVal := range colVals {
		if err := writer.Write(metricVal); err != nil {
			log.Panic(err)
		}
		writer.Flush()
	}
}

func ReadBoltDB(shardID int, nodeID int, block core.Block) {
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
	// dbPath := fmt.Sprintf("expTest/database/ReadData/S%d_N%d", shardID, nodeID)
	// nowHash := storage.GetNewestBlockHash()
	// fmt.Print(query.QueryBlock2(uint64(shardID), uint64(nodeID), 10))
	// fmt.Print("\n\n\n\n")
	/*
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
	*/

}
