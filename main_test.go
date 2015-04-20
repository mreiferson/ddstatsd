package main

import (
	"encoding/json"
	"io/ioutil"
	"reflect"
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

	packets := applyRules([]byte("nsq.topic.website_events.channel.nsq_to_file#ephemeral.message_count:1|c"), &cfg)
	if !reflect.DeepEqual(packets, [][]byte{
		[]byte("nsq.channel.message_count:1|c|#nsq_topic:website_events,nsq_channel:nsq_to_file__ephemeral,nsq_message_count"),
	}) {
		t.Fatalf("not equal %s", packets)
	}
}
