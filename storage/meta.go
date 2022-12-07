package storage

import (
	"log"
	"os"

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

// Config (to be transformed to meta in etcd)
type Config struct {
	EtcdEndpoints	[]string `yaml:"etcd"`
	SiteMetas		map[string]SiteMeta `yaml:"sites"`
	DbMetas		map[string]DbMeta `yaml:"dbs"`
}

var configs Config
var ServerName string

func LoadConfig() {
	content, err := os.ReadFile("config/config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	err = yaml.Unmarshal(content, &configs)
	if err != nil {
		log.Fatalf("cannot unmarshal data: %v", err)
	}
	println(configs.SiteMetas["main"].Ip)
	println(configs.DbMetas["segment1"].Port)
}

func ServerPort() int {
	return configs.SiteMetas[ServerName].Port
}