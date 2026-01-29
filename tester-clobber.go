package main

import "github.com/samucap/poly-asian-data/internal/services"

func main() {
	reqs, err := services.PlyMktService.BuildTempRequests([]string{"accounts"})
	// TODO: need to get all accounts, save them to the db
	// then query db for accounts by collateralVolume > 100000
	// then build requests for those accounts
	// - userPositions

	// TODO: need to fetch all events?active=true,closed=false
	// then build requests for those events
	// - for each market in event:
	// 	- aggregate market.clobTokenIds to make a request for enrichedOrderFilleds
	// 	- orderbook
	// 	- getSpread
	// 	- pricesHistory
	// 	- getMarketTradeEvents
}