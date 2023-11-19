package redis

import (
	"context"
	"fmt"
	"github.com/abstract-base-method/gographer"
	"github.com/redis/go-redis/v9"
	"testing"
)

func clearRedis(db int) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   db,
	})
	rdb.FlushAll(ctx)
}

func Test_RetrieveNode(t *testing.T) {
	db := 2
	clearRedis(db)

	node := gographer.Node{
		Id: "node-1",
		Data: map[string]string{
			"stuff": "things",
			"dev":   "test",
		},
	}

	graph, err := NewRedisGraph(&redis.Options{Addr: "localhost:6379", DB: db})
	if err != nil {
		t.Fatal("failed to create graph instance", err)
	}

	id, update, err := graph.StoreNode(node)
	if err != nil {
		t.Fatal("failed to store node", err)
	}

	if update {
		t.Fatal("node was updated when should be new", err)
	}

	if id != "node:node-1" {
		t.Fatal("node key was wrong")
	}

	retrievedNode, err := graph.RetrieveNode("node-1", map[string]string{})
	if err != nil {
		t.Fatal("failed to retrieve node", err)
	}
	if retrievedNode == nil {
		t.Fatal("node was not retrieved", err)
	}

	if retrievedNode.Data.(map[string]interface{})["stuff"] != "things" {
		t.Fatal("failed to retrieve stuff key")
	}
	if retrievedNode.Data.(map[string]interface{})["dev"] != "test" {
		t.Fatal("failed to retrieve dev key")
	}

	clearRedis(db)
}

func Test_RelatedNodes(t *testing.T) {
	db := 3
	clearRedis(3)

	node1 := gographer.Node{
		Id: "node-1",
		Data: map[string]string{
			"stuff": "things",
			"dev":   "test",
		},
	}
	node2 := gographer.Node{
		Id: "node-2",
		Data: map[string]string{
			"stuff": "things",
			"dev":   "test",
		},
	}
	node3 := gographer.Node{
		Id: "node-3",
		Data: map[string]string{
			"stuff": "things",
			"dev":   "test",
		},
	}
	relationParent := gographer.Relation{
		Host:   node1.Id,
		Target: node2.Id,
		Metadata: map[string]string{
			"env": "dev",
		},
	}
	relationChild := gographer.Relation{
		Host:   node3.Id,
		Target: node1.Id,
		Metadata: map[string]string{
			"env": "prod",
		},
	}
	nodes := []gographer.Node{node1, node2, node3}
	relations := []gographer.Relation{relationParent, relationChild}

	graph, err := NewRedisGraph(&redis.Options{
		Addr: "localhost:6379",
		DB:   db,
	})
	if err != nil {
		t.Error("failed to create graph instance", err)
	}

	for _, node := range nodes {
		id, update, err := graph.StoreNode(node)
		if err != nil {
			t.Error("failed to store node", err)
		}

		if update {
			t.Error("node was updated when should be new", err)
		}

		if id != fmt.Sprintf("node:%s", node.Id) {
			t.Error("node key was wrong")
		}
	}
	for _, relation := range relations {
		id, update, err := graph.StoreRelation(relation)
		if err != nil {
			t.Error("failed to store node", err)
		}

		if update {
			t.Error("node was updated when should be new", err)
		}

		if id != nodeIdToParentRelationKey(relation.Host) {
			t.Error("node key was wrong")
		}
	}

	allRelationsChan := graph.RelatedNodes(node1.Id)
	allRelations := make([]gographer.Relation, 0)
	for msg := range allRelationsChan {
		allRelations = append(allRelations, msg)
	}

	if len(allRelations) != 2 {
		t.Fatal("did not get 2 relations from DB", len(allRelations))
	}

	foundChild := false
	foundParent := false
	for _, relation := range allRelations {
		if relation.Host == node1.Id {
			foundParent = true
		}
		if relation.Host == node3.Id {
			foundChild = true
		}
	}

	if foundChild == false {
		t.Fatal("failed to retrieve child relationship")
	}
	if foundParent == false {
		t.Fatal("failed to retrieve parent relationship")
	}

	clearRedis(db)
}
