package server

import (
	"github.com/BruceAko/godis/lib/utils"
	"github.com/BruceAko/godis/pubsub"
	"github.com/BruceAko/godis/redis/connection"
	"github.com/BruceAko/godis/redis/parser"
	"github.com/BruceAko/godis/redis/protocol/asserts"
	"testing"
)

func TestPublish(t *testing.T) {
	hub := pubsub.MakeHub()
	channel := utils.RandString(5)
	msg := utils.RandString(5)
	conn := connection.NewFakeConn()
	pubsub.Subscribe(hub, conn, utils.ToCmdLine(channel))
	conn.Clean() // clean subscribe success
	pubsub.Publish(hub, utils.ToCmdLine(channel, msg))
	data := conn.Bytes()
	ret, err := parser.ParseOne(data)
	if err != nil {
		t.Error(err)
		return
	}
	asserts.AssertMultiBulkReply(t, ret, []string{
		"message",
		channel,
		msg,
	})

	// unsubscribe
	pubsub.UnSubscribe(hub, conn, utils.ToCmdLine(channel))
	conn.Clean()
	pubsub.Publish(hub, utils.ToCmdLine(channel, msg))
	data = conn.Bytes()
	if len(data) > 0 {
		t.Error("expect no msg")
	}

	// unsubscribe all
	pubsub.Subscribe(hub, conn, utils.ToCmdLine(channel))
	pubsub.UnSubscribe(hub, conn, utils.ToCmdLine())
	conn.Clean()
	pubsub.Publish(hub, utils.ToCmdLine(channel, msg))
	data = conn.Bytes()
	if len(data) > 0 {
		t.Error("expect no msg")
	}
}
