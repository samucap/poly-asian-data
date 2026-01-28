package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
)

type Tag struct {
	ID string `json:"id"`
    Label string `json:"label"`
}

func main() {
	baseURL := "https://gamma-api.polymarket.com/tags"
	limit := 1000 // Try larger limit
	offset := 0
	found := false
    count := 0

	for {
		params := url.Values{}
		params.Add("limit", strconv.Itoa(limit))
		params.Add("offset", strconv.Itoa(offset))
		resp, err := http.Get(baseURL + "?" + params.Encode())
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()

		var tags []Tag
		if err := json.NewDecoder(resp.Body).Decode(&tags); err != nil {
			panic(err)
		}

		if len(tags) == 0 {
			break
		}
        count += len(tags)

		for _, t := range tags {
			if t.ID == "899" {
				fmt.Printf("FOUND TAG 899: %s\n", t.Label)
				found = true
			}
            if t.ID == "100088" {
                fmt.Printf("FOUND TAG 100088: %s\n", t.Label)
            }
		}

		offset += limit
        fmt.Printf("Fetched %d tags...\r", count)
        if count > 50000 { break } // Safety break
	}
    fmt.Println()
	fmt.Printf("Total tags fetched: %d\n", count)
	if !found {
		fmt.Println("TAG 899 NOT FOUND in public list")
	}
}
