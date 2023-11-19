package gographer

import (
	"github.com/google/uuid"
	"strings"
)

type NodeData any

type Node struct {
	Id   string
	Data NodeData
}

func NewNode(data NodeData) Node {
	return Node{
		Id:   uuid.NewString(),
		Data: data,
	}
}

type Relation struct {
	Host     string
	Target   string
	Metadata map[string]string
}

func NewRelation(host string, target string) Relation {
	return NewRelationWithMetadata(host, target, map[string]string{})
}

func NewRelationWithMetadata(host string, target string, metadata map[string]string) Relation {
	if strings.HasPrefix(host, "node:") {
		host = strings.TrimPrefix(host, "node:")
	}
	if strings.HasPrefix(target, "node:") {
		target = strings.TrimPrefix(target, "node:")
	}

	return Relation{
		Host:     host,
		Target:   target,
		Metadata: metadata,
	}
}
