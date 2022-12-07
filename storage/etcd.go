package storage

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)


var endpoints = []string{"10.77.50.208:2381", "10.77.50.209:2381", "10.77.50.211:2381"}

func toGoB64(t TableMeta) string {
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	err := e.Encode(t)
    if err != nil {
		log.Fatal("failed gob Encode: ", err)
	}
    return base64.StdEncoding.EncodeToString(b.Bytes())
}

func fromGoB64(str string) TableMeta {
	t := TableMeta{}
    by, err := base64.StdEncoding.DecodeString(str)
    if err != nil {
		log.Fatal("failed base64 Decode: ", err)
	}
    b := bytes.Buffer{}
    b.Write(by)
    d := gob.NewDecoder(&b)
    err = d.Decode(&t)
    if err != nil {
		log.Fatal("failed gob Decode: ", err)
	}
    return t
}

// func FetchSites(tableName string) ([]string, []int, []string, error) {
// 	cli, err := clientv3.New(clientv3.Config{
// 		Endpoints: endpoints,
// 		DialTimeout: 5 * time.Second,
// 	})
// 	if err != nil {
// 		log.Fatal(err)
// 		return nil, nil, nil, err
// 	}
// 	defer cli.Close()

// 	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
// 	resp, err := cli.Get(ctx, tableName)
// 	cancel()
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	if len(resp.Kvs) != 1 {
// 		log.Fatal("etcd doesn't contain meta for ", tableName)
// 		return nil, nil, nil, errors.New("etcd doesn't contain meta for " + tableName)
// 	}

// 	t := fromGoB64(string(resp.Kvs[0].Value))
// 	return t.Ips, t.Ports, t.SiteNames, nil
// }

func StoreTableMeta(t TableMeta) error {
	str := toGoB64(t)

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
	_, err = cli.Put(ctx, t.TableName, str)
	cancel()
	if err != nil {
		log.Fatal(err)
		return err
	}

	return nil
}