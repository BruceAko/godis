package cluster

import (
	"github.com/BruceAko/godis/interface/redis"
	"github.com/BruceAko/godis/redis/protocol"
)

// FlushDB removes all data in current database
func FlushDB(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	replies := cluster.broadcast(c, args)
	var errReply protocol.ErrorReply
	for _, v := range replies {
		if protocol.IsErrorReply(v) {
			errReply = v.(protocol.ErrorReply)
			break
		}
	}
	if errReply == nil {
		return &protocol.OkReply{}
	}
	return protocol.MakeErrReply("error occurs: " + errReply.Error())
}

// FlushAll removes all data in cluster
func FlushAll(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	return FlushDB(cluster, c, args)
}
