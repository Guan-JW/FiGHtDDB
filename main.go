package main

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"fmt"

	"github.com/FiGHtDDB/storage"
)


func test(resp *[][]byte) {
	*resp = append(*resp, make([]byte, 0))
	(*resp)[len(*resp)-1] = append((*resp)[len(*resp)-1], "1234"...)
	(*resp)[len(*resp)-1] = append((*resp)[len(*resp)-1], "5678"...)
}

func test2(resp []byte) {
	resp = append(resp, "1234"...)
	resp = append(resp, "5678"...)
}

type table struct {
	Ips []string
	Ports []int
}

func toGoB64(t table) string {
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	err := e.Encode(t)
    if err != nil { fmt.Println(`failed gob Encode`, err) }
    return base64.StdEncoding.EncodeToString(b.Bytes())
}

func fromGoB64(str string) table {
	m := table{}
    by, err := base64.StdEncoding.DecodeString(str)
    if err != nil { fmt.Println(`failed base64 Decode`, err); }
    b := bytes.Buffer{}
    b.Write(by)
    d := gob.NewDecoder(&b)
    err = d.Decode(&m)
    if err != nil { fmt.Println(`failed gob Decode`, err); }
    return m
}

func main() {
	var t storage.TableMeta
	t.TableName = "Publisher"
	t.Ips = append(t.Ips, "10.77.50.211")
	t.Ips = append(t.Ips, "10.77.50.208")
	t.Ips = append(t.Ips, "10.77.50.209")
	t.Ips = append(t.Ips, "10.77.50.209")
	t.Ports = append(t.Ports, 5556)
	t.Ports = append(t.Ports, 5556)
	t.Ports = append(t.Ports, 5556)
	t.Ports = append(t.Ports, 5557)
	t.SiteNames = append(t.SiteNames, "main")
	t.SiteNames = append(t.SiteNames, "segment1")
	t.SiteNames = append(t.SiteNames, "segment2")
	t.SiteNames = append(t.SiteNames, "segment3")

	err := storage.StoreTableMeta(t)
	if err != nil {
		fmt.Println(err)
	}
	ips, ports, siteNames, err := storage.FetchSites(t.TableName)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(ips)
	fmt.Println(ports)
	fmt.Println(siteNames)
}