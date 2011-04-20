package gostore

import (
	"gostore/log"
	"os"
	"json"
	"bytes"
	"strings"
)

type Config struct {
	Services []ConfigService

	Rings       []ConfigRing
	GlobalRing  uint8

	Nodes       []ConfigNode
	CurrentNode uint16
}

type ConfigRing struct {
	Id                uint8
	ReplicationFactor uint8
}

type ConfigNodeRing struct {
	RingId uint8
	Token  string
}

type ConfigNode struct {
	NodeId uint16

	NodeIP  string
	TCPPort uint16
	UDPPort uint16

	Rings []ConfigNodeRing
}

type ConfigService struct {
	Id           uint8
	Type         string // sync, fs
	CustomConfig map[string]interface{}
}


func (c Config) Save(path string) {
	bytes, err := json.Marshal(c)

	if err != nil {
		log.Error("Couldn't save config: %s", err)
	}

	fc, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
	_, err = fc.Write(bytes)

	if err != nil {
		log.Error("Couldn't write config: %s", err)
	}
}

func LoadConfig(path string) Config {
	config := new(Config)

	fc, err := os.OpenFile(path, os.O_RDONLY, 0777)
	if err != nil {
		log.Error("Couldn't load config file: %s", err)
	}

	buf := new(bytes.Buffer)
	buf.ReadFrom(fc)

	strOrigConfig := string(buf.Bytes())
	strFinConfig := ""
	lines := strings.Split(strOrigConfig, "\n", -1)
	for _, line := range lines {
		trimedLine := strings.TrimSpace(line)
		if len(trimedLine) < 2 || trimedLine[:2] != "//" {
			strFinConfig += trimedLine
		}
	}

	
	err = json.Unmarshal([]byte(strFinConfig), config)

	if err != nil {
		log.Fatal("Couldn't unmarshal config: %s %s", strFinConfig, err)
	}

	return *config
}
