package cluster

import (
	"github.com/BruceAko/godis/redis/connection"
	"github.com/BruceAko/godis/redis/protocol/asserts"
	"math/rand"
	"strconv"
	"testing"
)

func TestRollback(t *testing.T) {
	// rollback uncommitted transaction
	conn := new(connection.FakeConn)
	FlushAll(testNodeA, conn, toArgs("FLUSHALL"))
	txID := rand.Int63()
	txIDStr := strconv.FormatInt(txID, 10)
	keys := []string{"a", "b"}
	groupMap := testNodeA.groupBy(keys)
	args := []string{txIDStr, "DEL"}
	args = append(args, keys...)
	testNodeA.Exec(conn, toArgs("SET", "a", "a"))
	ret := execPrepare(testNodeA, conn, makeArgs("Prepare", args...))
	asserts.AssertNotError(t, ret)
	requestRollback(testNodeA, conn, txID, groupMap)
	ret = testNodeA.Exec(conn, toArgs("GET", "a"))
	asserts.AssertBulkReply(t, ret, "a")

	// rollback committed transaction
	FlushAll(testNodeA, conn, toArgs("FLUSHALL"))
	txID = rand.Int63()
	txIDStr = strconv.FormatInt(txID, 10)
	args = []string{txIDStr, "DEL"}
	args = append(args, keys...)
	testNodeA.Exec(conn, toArgs("SET", "a", "a"))
	ret = execPrepare(testNodeA, conn, makeArgs("Prepare", args...))
	asserts.AssertNotError(t, ret)
	_, err := requestCommit(testNodeA, conn, txID, groupMap)
	if err != nil {
		t.Errorf("del failed %v", err)
		return
	}
	ret = testNodeA.Exec(conn, toArgs("GET", "a"))
	asserts.AssertNullBulk(t, ret)
	requestRollback(testNodeA, conn, txID, groupMap)
	ret = testNodeA.Exec(conn, toArgs("GET", "a"))
	asserts.AssertBulkReply(t, ret, "a")
}
