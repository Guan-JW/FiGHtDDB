package storage

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"gopkg.in/yaml.v3"
)

// site metadata
type SiteMeta struct {
	SiteName	string `yaml:"sitename"`
	Ip			string `yaml:"ip"`
	Port		int `yaml:"port"`
}

// etcd metadata
type EtcdMeta []string

// database metadata
type DbMeta struct {
	DbName		string `yaml:"dbname"`
	Port		int `yaml:"port"`
	User		string `yaml:"user"`
	Password	string `yaml:"password"`
	Sslmode		string `yaml:"sslmode"`
}

// table metadata
type Condition struct {
	Col		string	// column name
	Type	string	// column type
	Comp	string	// comparator
	Value	string	// value
}

type FragSchema struct {
	SiteName 	string
	Cols	 	[]string
	Conditions 	[]Condition
}

type TableMeta struct {
	TableName 	string
	FragNum	  	int
	FragSchema  []FragSchema
}

func toGoB64(t TableMeta) string {
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	err := e.Encode(t)
    if err != nil {
		log.Fatal("failed gob Encode: ", err)
	}
    return base64.StdEncoding.EncodeToString(b.Bytes())
}

func fromGoB64(str string, t *TableMeta) {
    by, err := base64.StdEncoding.DecodeString(str)
    if err != nil {
		log.Fatal("failed base64 Decode: ", err)
	}
    b := bytes.Buffer{}
    b.Write(by)
    d := gob.NewDecoder(&b)
    err = d.Decode(t)
    if err != nil {
		log.Fatal("failed gob Decode: ", err)
	}
}

// Config (to be transformed to meta in etcd)
type Config struct {
	EtcdEndpoints	[]string `yaml:"etcd"`
	SiteMetas		map[string]SiteMeta `yaml:"sites"`
	DbMetas		map[string]DbMeta `yaml:"dbs"`
}

var configs Config
var serverName string

func LoadConfig(_serverName string) {
	serverName = _serverName
	content, err := os.ReadFile("config/config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	err = yaml.Unmarshal(content, &configs)
	if err != nil {
		log.Fatalf("cannot unmarshal data: %v", err)
	}
}

func ServerPort() int {
	return configs.SiteMetas[serverName].Port
}

func ServerName() string {
	return serverName
}

func GetServerAddress(siteName string) string {
	addr := configs.SiteMetas[siteName].Ip + ":" + strconv.Itoa(configs.SiteMetas[siteName].Port)
	return addr
}

func GetLocalDbConnStr() (string, string, string, int, string) {
	return configs.DbMetas[serverName].DbName, configs.DbMetas[serverName].User, configs.DbMetas[serverName].Password,
		configs.DbMetas[serverName].Port, configs.DbMetas[serverName].Sslmode
}

func ResetTid() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: configs.EtcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	value := strconv.Itoa(0)
	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
	_, err = cli.Put(ctx, "tid", value)
	cancel()
	if err != nil {
		log.Fatal(err)
	}
}

func GetTid() int64 {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: configs.EtcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
	resp, err := cli.Get(ctx, "tid")
	cancel()
	if err != nil {
		log.Fatal(err)
	}

	if len(resp.Kvs) != 1 {
		log.Fatal("etcd doesn't contain tid")
	}
	
	tid, err := strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)
	if err != nil {
		log.Fatal(err)
	}

	value := fmt.Sprint(tid+1)
	ctx, cancel = context.WithTimeout(context.Background(), 5 * time.Second)
	_, err = cli.Put(ctx, "tid", value)
	cancel()
	if err != nil {
		log.Fatal(err)
	}
	return tid
}

func GetTableMeta(tableName string) (*TableMeta, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: configs.EtcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
	resp, err := cli.Get(ctx, "tables/" + tableName)
	cancel()
	if err != nil {
		log.Fatal(err)
	}

	if len(resp.Kvs) != 1 {
		log.Fatal("etcd doesn't contain meta for ", tableName)
		return nil, errors.New("etcd doesn't contain meta for " + tableName)
	}

	t := new(TableMeta)
	fromGoB64(string(resp.Kvs[0].Value), t)
	return t, nil
}

func StoreTableMeta(table *TableMeta) error {
	value := toGoB64(*table)

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   configs.EtcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
	_, err = cli.Put(ctx, "tables/" + table.TableName, value)
	cancel()
	if err != nil {
		log.Fatal(err)
		return err
	}

	return nil
}