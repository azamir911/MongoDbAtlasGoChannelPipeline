package main

import (
	"github.com/mongodb-forks/digest"
	"go.mongodb.org/atlas/mongodbatlas"
	"log"
)

func Client() *mongodbatlas.Client {
	t := digest.NewTransport("uxbqhcfh", "9360a0dc-9e45-44ff-8e92-bdfe0a8a0a54")
	tc, err := t.Client()
	//client, err := opsmngr.New(tc)
	checkError(err)

	//return client
	client := mongodbatlas.NewClient(tc)

	return client
}

func checkError(err error) {
	if err != nil {
		s := err.Error()
		log.Fatalf(s)
	}
}
