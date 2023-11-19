package gographer

type Graph interface {
	Store
	Researcher
}

type Store interface {
	StoreNode(node Node) (identifier string, result bool, err error)
	StoreRelation(relation Relation) (identifier string, result bool, err error)
	RetrieveNode(id string, typeOf interface{}) (node *Node, err error)
	DeleteNode(id string) (err error)
	DeleteRelation(host string, target string) (err error)
}

type Researcher interface {
	RelatedNodes(nodeId string) <-chan Relation
	ChildNodes(nodeId string) <-chan Relation
	ParentNodes(nodeId string) <-chan Relation
	NodesMatchingMetadata(nodeId string, key string, value string) <-chan Relation
}
