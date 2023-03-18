package clusterConf

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

var GYamlConf *ConfStruct

type ConfStruct struct {
	SourceDB struct {
		Host             string `yaml:"host"`
		QueryPort        string `yaml:"query_port"`
		User             string `yaml:"user"`
		Password         string `yaml:"password"`
		SourceDatabase   string `yaml:"source_database"`
		ExternalDatabase string `yaml:"external_database"`
		ExternalPrefix   string `yaml:"external_prefix"`
	} `yaml:"source_conf"`

	TargetDB struct {
		Host           string `yaml:"host"`
		QueryPort      string `yaml:"query_port"`
		RpcPort        string `yaml:"rpc_port"`
		User           string `yaml:"user"`
		Password       string `yaml:"password"`
		TargetDatabase string `yaml:"target_database"`
	} `yaml:"target_conf"`

	ViewSync struct {
		VIEW         bool `yaml:"view"`
		MATERIALIZED bool `yaml:"materialized_view"`
	} `yaml:"view_sync"`
}

func InitConf(confFile string) {
	var confStruct ConfStruct

	GYamlConf = confStruct.GetConf(confFile)

}

func (cc *ConfStruct) GetConf(fileName string) *ConfStruct {

	yamlFile, err := ioutil.ReadFile(fileName)
	if err != nil {
		panic(err)
	}

	err = yaml.Unmarshal(yamlFile, cc)
	if err != nil {
		panic(err)
	}

	cc.SourceDB.ExternalDatabase = cc.SourceDB.ExternalPrefix + cc.SourceDB.SourceDatabase

	return cc
}
