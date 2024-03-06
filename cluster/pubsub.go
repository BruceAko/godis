package cluster

import (
	"github.com/BruceAko/godis/interface/redis"
	"github.com/BruceAko/godis/lib/logger"
	"github.com/BruceAko/godis/redis/protocol"
)

const (
	relayPublish = "_publish"
	publish      = "publish"
)

var (
	publishRelayCmd = []byte(relayPublish)
	publishCmd      = []byte(publish)
)

// Publish broadcasts msg to all peers in cluster when receive publish command from client
func Publish(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	var count int64 = 0
	results := cluster.broadcast(c, args)
	for _, val := range results {
		if errReply, ok := val.(protocol.ErrorReply); ok {
			logger.Error("publish occurs error: " + errReply.Error())
		} else if intReply, ok := val.(*protocol.IntReply); ok {
			count += intReply.Code
		}
	}
	return protocol.MakeIntReply(count)
}

// onRelayedPublish receives publish command from peer, just publish to local subscribing clients, do not relay to peers
func onRelayedPublish(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	args[0] = publishCmd
	return cluster.db.Exec(c, args) // let local db.hub handle publish
}

// Subscribe puts the given connection into the given channel
func Subscribe(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	return cluster.db.Exec(c, args) // let local db.hub handle subscribe
}

// UnSubscribe removes the given connection from the given channel
func UnSubscribe(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	return cluster.db.Exec(c, args) // let local db.hub handle subscribe
}
