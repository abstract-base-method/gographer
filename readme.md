# GoGrapher
###### A Simple Graph Storage System for GoLang

This is a hobby project to make an efficient graph overlay of pluggable storage systems.
That means your graph data isn't tied to a single implementation, and you can control more about graph traversal and retrieval. 
For instance, one could implement a `gographer#Store` that interacts with a slower backend (such as a database) and make a `gographer#Researcher` that maps relationships in a fast cache like redis.
There are some implementations already here, like `./redis` which offers really efficient storage of graph data with fast retrieval times.
