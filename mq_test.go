package mq_test

import (
	"testing"

	"github.com/JeneveBob/client-mq"
)

func TestDirectMsg(t *testing.T) {
	mqObj := mq.New("127.0.0.1", 5672, "username", "password", "vhost")

	assertDirectMsg := func(msg string, exchange string, routeKey string, queue string) {
		ret := mqObj.DirectMsg(msg, exchange, routeKey, queue)
		if nil != ret {
			t.Error(ret.Error())
		}
	}

	assertDirectMsg("testData1", "test-test1", "test1", "test1")
	assertDirectMsg("testData2", "test-test2", "test2", "test2")
	assertDirectMsg("testData3", "test-test1", "test1", "")
	assertDirectMsg("testData4", "test-test2", "test2", "")
}

func TestBroadcastMsg(t *testing.T) {
	mqObj := mq.New("127.0.0.1", 5672, "username", "password", "vhost")

	assertBroadcastMsg := func(msg string, exchange string, queue string) {
		ret := mqObj.BroadcastMsg(msg, exchange, queue)
		if nil != ret {
			t.Error(ret.Error())
		}
	}

	assertBroadcastMsg("testData1", "test-test3", "test3")
	assertBroadcastMsg("testData2", "test-test4", "test4")
	assertBroadcastMsg("testData3", "test-test4", "test5")
	assertBroadcastMsg("testData4", "test-test3", "")
	assertBroadcastMsg("testData5", "test-test4", "")
}
