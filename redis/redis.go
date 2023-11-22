package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/abstract-base-method/gographer"
	"github.com/charmbracelet/log"
	"github.com/redis/go-redis/v9"
	"reflect"
	"strings"
	"sync"
)

func NewRedisGraph(options *redis.Options) (gographer.Graph, error) {
	return SingleDependencyGraph{
		ctx:   context.Background(),
		redis: redis.NewClient(options),
	}, nil
}

type SingleDependencyGraph struct {
	redis *redis.Client
	ctx   context.Context
}

func (r SingleDependencyGraph) StoreNode(node gographer.Node) (identifier string, result bool, err error) {
	key := nodeIdToNodeKey(node.Id)
	count, err := r.redis.Exists(r.ctx, key).Result()
	if err != nil {
		return "", false, err
	}

	if count == 1 {
		result = true
	} else {
		result = false
	}

	data, err := json.Marshal(node)
	if err != nil {
		return "", false, err
	}

	if result {
		log.Debugf("updating node %s", key)
	} else {
		log.Debugf("creating node %s", key)
	}

	return key, result, r.redis.Set(r.ctx, key, data, 0).Err()
}

func (r SingleDependencyGraph) StoreRelation(relation gographer.Relation) (identifier string, result bool, err error) {
	parentKey := nodeIdToParentRelationKey(relation.Host)
	childKey := nodeIdToChildRelationKey(relation.Target)

	count, err := r.redis.Exists(r.ctx, parentKey).Result()
	if err != nil {
		return "", false, err
	}

	relationData := make(map[string]string)
	if count == 1 {
		result = true
	} else {
		result = false
	}

	relationData[fmt.Sprintf("target:%s", relation.Target)] = relation.Target

	for k, v := range relation.Metadata {
		relationData[fmt.Sprintf("meta:%s:%s", relation.Target, k)] = v
	}

	err = r.redis.HSet(r.ctx, parentKey, relationData).Err()
	if err != nil {
		return "", false, err
	}

	if result {
		log.Debugf("updating relation %s", parentKey)
	} else {
		log.Debugf("created relation %s", parentKey)
	}

	if err = r.redis.SAdd(r.ctx, childKey, relation.Host).Err(); err != nil {
		return parentKey, result, err
	}

	return parentKey, result, nil
}

func (r SingleDependencyGraph) RetrieveNode(id string, typeOf interface{}) (node *gographer.Node, err error) {
	key := nodeIdToNodeKey(id)

	count, err := r.redis.Exists(r.ctx, key).Result()

	if count != 1 {
		return nil, errors.New("invalid node ID")
	}

	raw, err := r.redis.Get(r.ctx, key).Result()

	if err != nil {
		return nil, err
	}

	node = &gographer.Node{
		Id:   nodeKeyToNodeId(id),
		Data: reflect.New(reflect.TypeOf(typeOf).Elem()),
	}

	err = json.Unmarshal([]byte(raw), node)

	if err != nil {
		return nil, err
	}

	return node, nil
}

func (r SingleDependencyGraph) DeleteNode(id string) (err error) {
	nodeId := nodeKeyToNodeId(id)
	relations := r.RelatedNodes(nodeId)
	for relation := range relations {
		if relation.Host == nodeId {
			err = r.DeleteRelation(nodeId, nodeKeyToNodeId(relation.Target))
		} else {
			err = r.DeleteRelation(nodeKeyToNodeId(relation.Host), nodeId)
		}
		if err != nil {
			return err
		}
	}

	err = r.redis.Del(r.ctx, nodeIdToNodeKey(id)).Err()

	if err != nil {
		return err
	}
	return nil
}

func (r SingleDependencyGraph) DeleteRelation(host string, target string) (err error) {
	parentRelationKey := nodeIdToParentRelationKey(host)
	childRelationKey := nodeIdToChildRelationKey(target)
	parentKeysToDelete := make([]string, 0)

	data, err := r.redis.HGetAll(r.ctx, parentRelationKey).Result()
	for k := range data {
		if k == fmt.Sprintf("target:%s", nodeKeyToNodeId(target)) {
			parentKeysToDelete = append(parentKeysToDelete, k)
		}
		if strings.HasPrefix("meta:%s:", nodeKeyToNodeId(target)) {
			parentKeysToDelete = append(parentKeysToDelete, k)
		}
	}
	_, err = r.redis.HDel(r.ctx, parentRelationKey, parentKeysToDelete...).Result()
	if err != nil {
		return err
	}

	_, err = r.redis.SRem(r.ctx, childRelationKey, nodeKeyToNodeId(host)).Result()
	if err != nil {
		return err
	}

	return nil
}

func (r SingleDependencyGraph) RelatedNodes(nodeId string) <-chan gographer.Relation {
	returnChan := make(chan gographer.Relation)
	var wg sync.WaitGroup
	wg.Add(2)

	go func(relations <-chan gographer.Relation, final <-chan gographer.Relation) {
		for msg := range relations {
			returnChan <- msg
		}
		wg.Done()
	}(r.ChildNodes(nodeId), returnChan)
	go func(relations <-chan gographer.Relation, final <-chan gographer.Relation) {
		for msg := range relations {
			returnChan <- msg
		}
		wg.Done()
	}(r.ParentNodes(nodeId), returnChan)
	go func(c chan gographer.Relation, group *sync.WaitGroup) {
		wg.Wait()
		close(c)
	}(returnChan, &wg)

	return returnChan
}

func (r SingleDependencyGraph) ChildNodes(nodeId string) <-chan gographer.Relation {
	if strings.HasPrefix(nodeId, "node:") {
		nodeId = strings.TrimPrefix(nodeId, "node:")
	}
	resultChan := make(chan gographer.Relation)

	relationData, err := r.redis.HGetAll(r.ctx, nodeIdToParentRelationKey(nodeId)).Result()
	if err != nil {
		close(resultChan)
		return resultChan
	}

	go func(relation map[string]string, results chan gographer.Relation) {
		targets := make(map[string]map[string]string)
		for k, v := range relation {
			if strings.HasPrefix(k, "target:") {
				if _, exists := targets[v]; !exists {
					targets[v] = make(map[string]string)
				}
			} else if strings.HasPrefix(k, "meta:") {
				metaKeyElements := strings.Split(strings.TrimPrefix(k, "meta:"), ":")
				target := metaKeyElements[0]
				metaKey := metaKeyElements[1]
				if _, exists := targets[target]; exists {
					targets[target][metaKey] = v
				} else {
					targets[target] = make(map[string]string)
					targets[target][metaKey] = v
				}
			}
		}

		for target, metadata := range targets {
			results <- gographer.NewRelationWithMetadata(nodeId, target, metadata)
		}
		close(results)
	}(relationData, resultChan)

	return resultChan
}

func (r SingleDependencyGraph) ParentNodes(nodeId string) <-chan gographer.Relation {
	parents := make(chan gographer.Relation)
	ids, err := r.redis.SMembers(r.ctx, nodeIdToChildRelationKey(nodeId)).Result()
	if err != nil {
		log.Error("failed to retrieve parents", "error", err)
	}

	go func(parentNodes []string, results chan gographer.Relation) {
		for _, parentId := range ids {
			if relationData, err := r.redis.HGetAll(r.ctx, nodeIdToParentRelationKey(parentId)).Result(); err != nil {
				log.Error("failed to retrieve parent relation key", "error", err, "key", nodeIdToParentRelationKey(parentId))
			} else {
				relation := gographer.Relation{
					Host:     nodeKeyToNodeId(parentId),
					Target:   nodeKeyToNodeId(nodeId),
					Metadata: map[string]string{},
				}
				for k, v := range relationData {
					if strings.HasPrefix(k, fmt.Sprintf("meta:%s:", nodeKeyToNodeId(nodeId))) {
						metaKey := strings.TrimPrefix(k, fmt.Sprintf("meta:%s:", nodeKeyToNodeId(nodeId)))
						relation.Metadata[metaKey] = v
					}
				}
				results <- relation
			}
		}
		close(results)
	}(ids, parents)

	return parents
}

func (r SingleDependencyGraph) NodesMatchingMetadata(nodeId string, key string, value string) <-chan gographer.Relation {
	returnChan := make(chan gographer.Relation)
	go func(relations <-chan gographer.Relation, results chan gographer.Relation) {
		for relation := range relations {
			if val, ok := relation.Metadata[key]; ok {
				if val == value {
					results <- relation
				}
			}
		}
		close(results)
	}(r.RelatedNodes(nodeKeyToNodeId(nodeId)), returnChan)
	return returnChan
}

func nodeIdToNodeKey(id string) (key string) {
	if strings.HasPrefix(id, "node:") {
		key = id
	} else {
		key = fmt.Sprintf("node:%s", id)
	}
	return key
}

func nodeKeyToNodeId(key string) (id string) {
	if strings.HasPrefix(key, "node:") {
		id = strings.TrimPrefix(key, "node:")
	} else {
		id = key
	}
	return id
}

func nodeIdToParentRelationKey(id string) (key string) {
	if strings.HasPrefix(id, "childrenOf:") {
		key = id
	} else {
		key = fmt.Sprintf("childrenOf:%s", id)
	}
	return key
}

func nodeIdToChildRelationKey(id string) (key string) {
	if strings.HasPrefix(id, "parentOf:") {
		key = id
	} else {
		key = fmt.Sprintf("parentOf:%s", id)
	}
	return key
}
