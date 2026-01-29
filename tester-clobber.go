package main

import "github.com/samucap/poly-asian-data/internal/services"

func main() {
	reqs, err := services.PlyMktService.BuildTempRequests([]string{"accounts", "userPositions"})
}