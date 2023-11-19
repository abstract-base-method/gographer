package redis

import (
	"context"
	"github.com/abstract-base-method/gographer"
	"github.com/charmbracelet/log"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"math/rand"
	"sync"
	"testing"
	"time"
)

const NumberOfNodes = 100_000
const NumberOfPotentialStarterRelations = 10
const largeGraphdb = 4

var originId *string

func Benchmark_large_graph(b *testing.B) {
	b.StopTimer()
	clearRedis(4)
	var graph, err = NewRedisGraph(&redis.Options{
		Addr:       "localhost:6379",
		ClientName: "example",
		DB:         largeGraphdb,
	})

	initGraph(graph)

	if err != nil {
		panic(err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: largeGraphdb})
	keyspace, _ := rdb.Keys(context.Background(), "*").Result()

	// don't count data generation for graph traversal
	b.ResetTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		startOfWalkTime := time.Now()
		relations := make([]gographer.Relation, 0)
		for rel := range graph.RelatedNodes(*originId) {
			relations = append(relations, rel)
		}
		completionDuration := time.Since(startOfWalkTime).Milliseconds()

		log.Info(
			"graph traversal completed",
			"relationsTraversed",
			len(relations),
			"graphTraversalMillis",
			completionDuration,
			"keyspace",
			len(keyspace),
		)
	}

	clearRedis(4)
}

func initGraph(graph gographer.Graph) {
	if origin, err := generateData(graph, NumberOfNodes, NumberOfPotentialStarterRelations); err != nil {
		panic(err)
	} else {
		originId = &origin
	}
	log.Info("data generation for benchmark complete")
}

func generateData(graph gographer.Graph, nodeCount int, maxRelationsPerNode int) (origin string, err error) {
	log.Info("generating keyspace")
	originNode := gographer.Node{
		Id: "origin",
		Data: map[string]string{
			"random": uuid.NewString(),
		},
	}
	originId, _, err := graph.StoreNode(originNode)

	nodes := make([]string, 0)
	nodes = append(nodes, originId)
	var wg sync.WaitGroup

	for i := 0; i < nodeCount; i++ {
		node := gographer.NewNode(map[string]int{
			"seed": i,
		})
		id, _, err := graph.StoreNode(node)

		nodes = append(nodes, id)

		relations := rand.Intn(maxRelationsPerNode)
		wg.Add(1)
		go func(redisGraph gographer.Graph, numRelations int, group *sync.WaitGroup) {
			defer group.Done()
			relatedNodes := make([]string, 0)
			for r := 0; r < relations; r++ {
				relatedNode := nodes[rand.Intn(len(nodes))]
				if !containsString(relatedNodes, relatedNode) {
					topology := rand.Intn(2)
					switch topology {
					case 0:
						_, _, err = redisGraph.StoreRelation(gographer.NewRelationWithMetadata(node.Id, relatedNode, generateRandomMap()))
					case 1:
						_, _, err = redisGraph.StoreRelation(gographer.NewRelationWithMetadata(relatedNode, node.Id, generateRandomMap()))
					default:
						panic("unknown random int for topology")
					}

					if err != nil {
						panic(err)
					}

					addToOrigin := rand.Intn(2)
					if addToOrigin == 1 {
						_, _, err = redisGraph.StoreRelation(gographer.NewRelationWithMetadata(originId, node.Id, generateRandomMap()))
						if err != nil {
							panic(err)
						}
						_, _, err = redisGraph.StoreRelation(gographer.NewRelationWithMetadata(node.Id, originId, generateRandomMap()))
						if err != nil {
							panic(err)
						}
					}
				}
			}
		}(graph, relations, &wg)

	}

	wg.Wait()

	log.Info("keyspace generation complete")

	return originId, err
}

func containsString(slice []string, str string) bool {
	for _, item := range slice {
		if item == str {
			return true
		}
	}
	return false
}

// Predefined slices of phrases
var one = []string{
	"Quantum leap ",
	"Beam me up ",
	"Hailing frequencies open ",
	"Resistance is futile ",
	"Set phasers to ",
	"Warp speed ",
	"Live long and ",
}

var two = []string{
	"when dealing with AI.",
	"in the cloud.",
	"on the final frontier.",
	"against the Borg.",
	"for stunning innovation.",
	"towards the unknown.",
	"prosper in code.",
}

var three = []string{
	"The dilithium crystals are charged.",
	"The holodeck awaits your imagination.",
	"The prime directive is non-interference.",
	"Androids do dream of electric sheep.",
	"The replicator is the ultimate maker's tool.",
	"Boldly go where no one has gone before.",
	"Keep your tribbles in check.",
}

var keys = []string{
	"alpha",
	"beta",
	"charlie",
	"delta",
	"echo",
}

// getRandomMap generates a map of random keys with sentences combining phrases from slices one, two, and three
func generateRandomMap() map[string]string {
	wordMap := make(map[string]string)
	for i := 0; i < 10; i++ {
		key := keys[rand.Intn(len(keys))]
		value := one[rand.Intn(len(one))] + two[rand.Intn(len(two))] + " " + three[rand.Intn(len(three))]
		wordMap[key] = value
	}
	return wordMap
}
