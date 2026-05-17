## Notes

    2. need to make code more functional by abstracting generic functionalities better, i.e. need to write abstract db processes into db.go file, and export functions to be used by other cmds; same for workerpool, http client, db client actions... need to refactor how pipeline's created? cuz like the idea i had in mind was that pipelines will have phases and each phase will be comprised of workerpool to concurrently process tasks assigned to that phase,
        or workerpool... i guess I don't have it so wrong... but I mean, need to tidy things up
    3. Need to define workflows/pipelines and schedules
    4. Required processes:
        1. "./cmd/sports-sync/main.go":
            - updates sports, tags, teams: in case missing from polymarket
            - daily?
        2. events/markets sync for each category:
            - scan all events (in ground.go right now it's only ranking events and markets, but their categorization's independent.. I realize that I prolly don't need to save top events as I can always just request from the api
            for free, so I guess I only save top markets? looks like saving 600 markets minimum.. might need to revise that cuz imma have to fetch prices-history, orderbook, trades for 600 * for each category.. shouldn't need that many)
                - rank markets by i.e. volume, liquidity, spread, volatility
                - note top markets
                - fetch prices-history, orderbook, trades for top markets

            - Aggregate statistical data like i.e. average prices, volumes, liquidity, spread, volatility, and stuff for the treemap by market cap (if avail)


    - Fees apply only to 15-minute crypto, 5-minute crypto, NCAAB, and Serie A markets

# TODO

    - implement cleaning functions like normalizing dates, timestamps or whatever
    - implement ./cmd/main.go as entrypoint to all other cmds and make it into cli tool with cobra?
