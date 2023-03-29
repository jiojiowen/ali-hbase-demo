package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"os"
	"strings"

	// hbase模块通过 thrift --gen go hbase.thrift 来生成
	"ali-hbase-demo/gen-go/hbase"
)

var (
	HOST     = os.Getenv("HOST")
	USER     = os.Getenv("USER")
	PASSWORD = os.Getenv("PASSWORD")
	FILE     = "./case-1.txt" // example: key,values
)

func main() {
	if HOST == "" || USER == "" || PASSWORD == "" {
		fmt.Fprintln(os.Stderr, "HOST, USER, PASSWORD must be set")
		return
	}
	err, client := createHbaseClient("default")
	if err != nil {
		fmt.Println(err)
	}
	client.get("test", "1")

	f, err := os.Open(FILE)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer f.Close()
	standardDate := make(map[string]string, 700)
	var rows []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		res := strings.Split(line, ",")
		if len(res) != 2 {
			fmt.Println("line format error")
		}
		standardDate[res[0]] = res[1]
		rows = append(rows, res[0])
	}
	if err := scanner.Err(); err != nil {
		fmt.Println(err)
		return
	}
	err, data := client.getMultiple("sha256", rows)
	if err != nil {
		fmt.Println(err)
		return
	}
	for k, v := range data {
		if v != standardDate[k] {
			fmt.Println("error")
			fmt.Println("[Mismatch]", k, v, standardDate[k])
		}
		fmt.Println("[ok]", k)
	}

}

//范围扫描
func (h *habseClient) scan(tableName string, startRow string, stopRow string) (err error) {
	tableInbytes := []byte(tableName)
	scan := &hbase.TScan{StartRow: []byte(startRow), StopRow: []byte(stopRow)}
	//根据每行的大小，caching的值一般设置为10到100之间
	caching := 10
	// 扫描的结果
	var scanResults []*hbase.TResult_
	for true {
		var lastResult *hbase.TResult_ = nil
		// getScannerResults会自动完成open,close 等scanner操作，HBase增强版必须使用此方法进行范围扫描
		currentResults, _ := h.client.GetScannerResults(context.Background(), tableInbytes, scan, int32(caching))
		for _, tResult := range currentResults {
			lastResult = tResult
			scanResults = append(scanResults, tResult)
		}
		// 如果一行都没有扫描出来，说明扫描已经结束，我们已经获得startRow和stopRow之间所有的result
		if lastResult == nil {
			break
		} else {
			// 如果此次扫描是有结果的，我们必须构造一个比当前最后一个result的行大的最小row，继续进行扫描，以便返回所有结果
			nextStartRow := createClosestRowAfter(lastResult.Row)
			scan = &hbase.TScan{StartRow: nextStartRow, StopRow: []byte(stopRow)}
		}
	}
	fmt.Println("Scan result:")
	fmt.Println(scanResults)
	return nil
}

// 此函数可以找到比当前row大的最小row，方法是在当前row后加入一个0x00的byte
// 从比当前row大的最小row开始scan，可以保证中间不会漏扫描数据
func createClosestRowAfter(row []byte) []byte {
	var nextRow []byte
	var i int
	for i = 0; i < len(row); i++ {
		nextRow = append(nextRow, row[i])
	}
	nextRow = append(nextRow, 0x00)
	return nextRow
}

// create Namespace
func (h *habseClient) createNS() (err error) {
	err = h.client.CreateNamespace(context.Background(), &hbase.TNamespaceDescriptor{Name: h.ns})
	if err != nil {
		fmt.Fprintln(os.Stderr, "error CreateNamespace:", err)
		return err
	}
	return nil
}

// create table
func (h *habseClient) createTable(tableName string) (err error) {
	table := hbase.TTableName{
		Ns:        []byte(h.ns),
		Qualifier: []byte(tableName),
	}

	err = h.client.CreateTable(context.Background(), &hbase.TTableDescriptor{TableName: &table, Columns: []*hbase.TColumnFamilyDescriptor{&hbase.TColumnFamilyDescriptor{Name: []byte("f")}}}, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error CreateTable:", err)
		return err
	}
	return nil
}

// 单行查询数据
func (h *habseClient) get(tableName string, row string) {
	tableInbytes := []byte(tableName)

	result, err := h.client.Get(context.Background(), tableInbytes, &hbase.TGet{Row: []byte(row)})
	if err != nil {
		fmt.Fprintln(os.Stderr, "error Get:", err)
	}
	if result.Row == nil {
		fmt.Fprintln(os.Stderr, "error Get:", "row is nil")
		return
	}
	k := string(result.Row)
	v := string(result.ColumnValues[0].Value)
	fmt.Println(k)
	fmt.Println(v)
}

// 批量单行查询数据
func (h *habseClient) getMultiple(tableName string, rows []string) (err error, data map[string]string) {
	data = make(map[string]string, 700)
	tableInbytes := []byte(tableName)
	var gets []*hbase.TGet
	for _, row := range rows {
		gets = append(gets, &hbase.TGet{Row: []byte(row)})
	}
	results, err := h.client.GetMultiple(context.Background(), tableInbytes, gets)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error GetMultiple:", err)
		return err, nil
	}
	for _, result := range results {
		if result.Row == nil {
			err = errors.New("error GetMultiple: row is nil")
			fmt.Fprintln(os.Stderr, err)
			return err, nil
		}
		k := string(result.Row)
		v := string(result.ColumnValues[0].Value)
		data[k] = v
	}
	return nil, data
}

// 插入数据
func (h *habseClient) put(tableName string, row string, value string) (err error) {
	tableInbytes := []byte(tableName)
	err = h.client.Put(context.Background(), tableInbytes, &hbase.TPut{Row: []byte(row), ColumnValues: []*hbase.TColumnValue{&hbase.TColumnValue{
		Family:    []byte("cf"),
		Qualifier: []byte("a"),
		Value:     []byte(value)}}})
	if err != nil {
		fmt.Fprintln(os.Stderr, "error Put:", err)
		return err
	}
	return nil
}

// 批量插入数据
func (h *habseClient) putMultiple(tableName string, data map[string]string) (err error) {
	tableInbytes := []byte(tableName)
	var puts []*hbase.TPut
	for row, value := range data {
		puts = append(puts, &hbase.TPut{
			Row: []byte(row),
			ColumnValues: []*hbase.TColumnValue{&hbase.TColumnValue{
				Family:    []byte("cf"),
				Qualifier: []byte("a"),
				Value:     []byte(value)}}})
	}

	err = h.client.PutMultiple(context.Background(), tableInbytes, puts)
	if err != nil {
		err = errors.New("error PutMultiple: " + err.Error())
		fmt.Fprintln(os.Stderr, "error PutMultiple:", err)
		return err
	}
	return nil
}

func createHbaseClient(ns string) (err error, client *habseClient) {
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	trans, err := thrift.NewTHttpClient(HOST)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error resolving address:", err)
		return err, nil
	}
	// 设置用户名密码
	httClient := trans.(*thrift.THttpClient)
	httClient.SetHeader("ACCESSKEYID", USER)
	httClient.SetHeader("ACCESSSIGNATURE", PASSWORD)
	c := hbase.NewTHBaseServiceClientFactory(trans, protocolFactory)
	if err := trans.Open(); err != nil {
		fmt.Fprintln(os.Stderr, "Error opening "+HOST, err)
		return err, nil
	}

	return nil, &habseClient{
		client: c,
		ns:     ns,
	}
}

type habseClient struct {
	client *hbase.THBaseServiceClient
	ns     string
}
