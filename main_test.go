package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"regexp"
	"testing"
)

func TestRules(t *testing.T) {
	var cfg Config
	data, _ := ioutil.ReadFile("rules.cfg")
	json.Unmarshal(data, &cfg)
	for _, rule := range cfg.Rules {
		rule.in = regexp.MustCompile(rule.In)
	}

	packets := applyRules([]byte("nsq.topic.website_events.channel.nsq_to_file.message_count:1|c"), &cfg)
	log.Printf("%s", packets)
}
