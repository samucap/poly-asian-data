package services

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/fetcher"
)

type PlyMktMarket struct {
	ID                          string          `json:"id"`
	EventID                     string          `json:"eventId"` // Added for relationship
	Question                    string          `json:"question"`
	ConditionID                 string          `json:"conditionId"`
	Slug                        string          `json:"slug"`
	TwitterCardImage            string          `json:"twitterCardImage"`
	ResolutionSource            string          `json:"resolutionSource"`
	EndDate                     time.Time       `json:"endDate" time_format:"2026-03-31T12:00:00Z"`
	Category                    string          `json:"category"`
	AmmType                     string          `json:"ammType"`
	Liquidity                   string          `json:"liquidity"` // String to preserve precision
	SponsorName                 string          `json:"sponsorName"`
	SponsorImage                string          `json:"sponsorImage"`
	StartDate                   time.Time       `json:"startDate" time_format:"2026-03-31T12:00:00Z"`
	XAxisValue                  string          `json:"xAxisValue"`
	YAxisValue                  string          `json:"yAxisValue"`
	DenominationToken           string          `json:"denominationToken"`
	Fee                         string          `json:"fee"` // String to preserve precision
	Image                       string          `json:"image"`
	Icon                        string          `json:"icon"`
	LowerBound                  string          `json:"lowerBound"`
	UpperBound                  string          `json:"upperBound"`
	Description                 string          `json:"description"`
	Outcomes                    string          `json:"outcomes"`
	OutcomePrices               string          `json:"outcomePrices"`
	Volume                      string          `json:"volume"` // String to preserve precision
	Active                      bool            `json:"active"`
	MarketType                  string          `json:"marketType"`
	FormatType                  string          `json:"formatType"`
	LowerBoundDate              string          `json:"lowerBoundDate"`
	UpperBoundDate              string          `json:"upperBoundDate"`
	Closed                      bool            `json:"closed"`
	MarketMakerAddress          string          `json:"marketMakerAddress"`
	CreatedBy                   int64           `json:"createdBy"`
	UpdatedBy                   int64           `json:"updatedBy"`
	CreatedAt                   time.Time       `json:"createdAt" time_format:"2025-03-26T14:44:31.96609Z"`
	UpdatedAt                   time.Time       `json:"updatedAt" time_format:"2025-03-26T14:44:31.96609Z"`
	ClosedTime                  string          `json:"closedTime"`
	WideFormat                  bool            `json:"wideFormat"`
	New                         bool            `json:"new"`
	MailchimpTag                string          `json:"mailchimpTag"`
	Featured                    bool            `json:"featured"`
	Archived                    bool            `json:"archived"`
	ResolvedBy                  string          `json:"resolvedBy"`
	Restricted                  bool            `json:"restricted"`
	MarketGroup                 int64           `json:"marketGroup"`
	GroupItemTitle              string          `json:"groupItemTitle"`
	GroupItemThreshold          string          `json:"groupItemThreshold"`
	QuestionID                  string          `json:"questionID"`
	UmaEndDate                  string          `json:"umaEndDate"`
	EnableOrderBook             bool            `json:"enableOrderBook"`
	OrderPriceMinTickSize       float64           `json:"orderPriceMinTickSize"`
	OrderMinSize                float64           `json:"orderMinSize"`
	UmaResolutionStatus         string          `json:"umaResolutionStatus"`
	CurationOrder               int64           `json:"curationOrder"`
	VolumeNum                   float64           `json:"volumeNum"`
	LiquidityNum                float64           `json:"liquidityNum"`
	EndDateIso                  string          `json:"endDateIso"`
	StartDateIso                string          `json:"startDateIso"`
	UmaEndDateIso               string          `json:"umaEndDateIso"`
	HasReviewedDates            bool            `json:"hasReviewedDates"`
	ReadyForCron                bool            `json:"readyForCron"`
	CommentsEnabled             bool            `json:"commentsEnabled"`
	Volume24hr                  float64           `json:"volume24hr"`
	Volume1wk                   float64           `json:"volume1wk"`
	Volume1mo                   float64           `json:"volume1mo"`
	Volume1yr                   float64           `json:"volume1yr"`
	GameStartTime               string          `json:"gameStartTime"`
	SecondsDelay                int64           `json:"secondsDelay"`
	ClobTokenIds                string          `json:"clobTokenIds"`
	DisqusThread                string          `json:"disqusThread"`
	ShortOutcomes               string          `json:"shortOutcomes"`
	TeamAID                     string          `json:"teamAID"`
	TeamBID                     string          `json:"teamBID"`
	UmaBond                     string          `json:"umaBond"`
	UmaReward                    string          `json:"umaReward"`
	FpmmLive                    bool            `json:"fpmmLive"`
	Volume24hrAmm               float64           `json:"volume24hrAmm"`
	Volume1wkAmm                float64           `json:"volume1wkAmm"`
	Volume1moAmm                float64           `json:"volume1moAmm"`
	Volume1yrAmm                float64           `json:"volume1yrAmm"`
	Volume24hrClob              float64           `json:"volume24hrClob"`
	Volume1wkClob               float64           `json:"volume1wkClob"`
	Volume1moClob               float64           `json:"volume1moClob"`
	Volume1yrClob               float64           `json:"volume1yrClob"`
	VolumeAmm                   float64           `json:"volumeAmm"`
	VolumeClob                  float64           `json:"volumeClob"`
	LiquidityAmm                float64           `json:"liquidityAmm"`
	LiquidityClob               float64           `json:"liquidityClob"`
	MakerBaseFee                int64           `json:"makerBaseFee"`
	TakerBaseFee                int64           `json:"takerBaseFee"`
	CustomLiveness              int64           `json:"customLiveness"`
	AcceptingOrders             bool            `json:"acceptingOrders"`
	NotificationsEnabled        bool            `json:"notificationsEnabled"`
	Score                       int64           `json:"score"`
	ImageOptimized              ImageOptimized  `json:"imageOptimized"`
	IconOptimized               ImageOptimized  `json:"iconOptimized"`
	Creator                     string          `json:"creator"`
	Ready                       bool            `json:"ready"`
	Funded                      bool            `json:"funded"`
	PastSlugs                   string          `json:"pastSlugs"`
	ReadyTimestamp              time.Time       `json:"readyTimestamp"`
	FundedTimestamp             time.Time       `json:"fundedTimestamp"`
	AcceptingOrdersTimestamp    time.Time       `json:"acceptingOrdersTimestamp"`
	Competitive                 float64           `json:"competitive"`
	RewardsMinSize              float64           `json:"rewardsMinSize"`
	RewardsMaxSpread            float64           `json:"rewardsMaxSpread"`
	Spread                      float64           `json:"spread"`
	AutomaticallyResolved       bool            `json:"automaticallyResolved"`
	OneDayPriceChange           float64           `json:"oneDayPriceChange"`
	OneHourPriceChange          float64           `json:"oneHourPriceChange"`
	OneWeekPriceChange          float64           `json:"oneWeekPriceChange"`
	OneMonthPriceChange         float64           `json:"oneMonthPriceChange"`
	OneYearPriceChange          float64           `json:"oneYearPriceChange"`
	LastTradePrice              float64           `json:"lastTradePrice"`
	BestBid                     float64           `json:"bestBid"`
	BestAsk                     float64           `json:"bestAsk"`
	AutomaticallyActive         bool            `json:"automaticallyActive"`
	ClearBookOnStart            bool            `json:"clearBookOnStart"`
	ChartColor                  string          `json:"chartColor"`
	SeriesColor                 string          `json:"seriesColor"`
	ShowGmpSeries               bool            `json:"showGmpSeries"`
	ShowGmpOutcome              bool            `json:"showGmpOutcome"`
	ManualActivation            bool            `json:"manualActivation"`
	NegRiskOther                bool            `json:"negRiskOther"`
	GameID                      string          `json:"gameId"`
	GroupItemRange              string          `json:"groupItemRange"`
	SportsMarketType            string          `json:"sportsMarketType"`
	Line                        float64         `json:"line"`
	UmaResolutionStatuses       string          `json:"umaResolutionStatuses"`
	PendingDeployment           bool            `json:"pendingDeployment"`
	Deploying                   bool            `json:"deploying"`
	DeployingTimestamp          time.Time       `json:"deployingTimestamp"`
	ScheduledDeploymentTimestamp time.Time      `json:"scheduledDeploymentTimestamp"`
	RfqEnabled                  bool            `json:"rfqEnabled"`
	EventStartTime              time.Time       `json:"eventStartTime"`
}

type ImageOptimized struct {
	ID                        string `json:"id"`
	ImageURLSource            string `json:"imageUrlSource"`
	ImageURLOptimized         string `json:"imageUrlOptimized"`
	ImageSizeKbSource         int    `json:"imageSizeKbSource"`
	ImageSizeKbOptimized      int    `json:"imageSizeKbOptimized"`
	ImageOptimizedComplete    bool   `json:"imageOptimizedComplete"`
	ImageOptimizedLastUpdated string `json:"imageOptimizedLastUpdated"`
	RelID                     int    `json:"relID"`
	Field                     string `json:"field"`
	Relname                   string `json:"relname"`
}

type PlyMktEvent struct {
	ID                           string              `json:"id"`
	Ticker                       string              `json:"ticker"`
	Slug                         string              `json:"slug"`
	Title                        string              `json:"title"`
	Subtitle                     string              `json:"subtitle"`
	Description                  string              `json:"description"`
	ResolutionSource             string              `json:"resolutionSource"`
	StartDate                    time.Time           `json:"startDate"`
	CreationDate                 time.Time           `json:"creationDate"`
	EndDate                      time.Time           `json:"endDate"`
	Image                        string              `json:"image"`
	Icon                         string              `json:"icon"`
	Active                       bool                `json:"active"`
	Closed                       bool                `json:"closed"`
	Archived                     bool                `json:"archived"`
	New                          bool                `json:"new"`
	Featured                     bool                `json:"featured"`
	Restricted                   bool                `json:"restricted"`
	Liquidity                    float64             `json:"liquidity"`
	Volume                       float64             `json:"volume"`
	OpenInterest                 float64             `json:"openInterest"`
	SortBy                       string              `json:"sortBy"`
	Category                     string              `json:"category"`
	Subcategory                  string              `json:"subcategory"`
	IsTemplate                   bool                `json:"isTemplate"`
	TemplateVariables            string              `json:"templateVariables"`
	PublishedAt                  string              `json:"published_at"`
	CreatedBy                    string              `json:"createdBy"`
	UpdatedBy                    string              `json:"updatedBy"`
	CreatedAt                    time.Time           `json:"createdAt"`
	UpdatedAt                    time.Time           `json:"updatedAt"`
	CommentsEnabled              bool                `json:"commentsEnabled"`
	Competitive                  float64             `json:"competitive"`
	Volume24hr                   float64             `json:"volume24hr"`
	Volume1wk                    float64             `json:"volume1wk"`
	Volume1mo                    float64             `json:"volume1mo"`
	Volume1yr                    float64             `json:"volume1yr"`
	FeaturedImage                string              `json:"featuredImage"`
	DisqusThread                 string              `json:"disqusThread"`
	ParentEvent                  string              `json:"parentEvent"`
	EnableOrderBook              bool                `json:"enableOrderBook"`
	LiquidityAmm                 float64             `json:"liquidityAmm"`
	LiquidityClob                float64             `json:"liquidityClob"`
	NegRisk                      bool                `json:"negRisk"`
	NegRiskMarketID              string              `json:"negRiskMarketID"`
	NegRiskFeeBips               int                 `json:"negRiskFeeBips"`
	CommentCount                 int                 `json:"commentCount"`
	ImageOptimized               ImageOptimized      `json:"imageOptimized"`
	IconOptimized                ImageOptimized      `json:"iconOptimized"`
	FeaturedImageOptimized       ImageOptimized      `json:"featuredImageOptimized"`
	SubEvents                    []string            `json:"subEvents"`
	Markets                      []*PlyMktMarket      `json:"markets"`
	Categories                   string              `json:"categories"`
	Collections                  string              `json:"collections"`
	Tags                         []*PlyMktTag        `json:"tags"`
	Cyom                         bool                `json:"cyom"`
	ClosedTime                   time.Time           `json:"closedTime"`
	ShowAllOutcomes              bool                `json:"showAllOutcomes"`
	ShowMarketImages             bool                `json:"showMarketImages"`
	AutomaticallyResolved        bool                `json:"automaticallyResolved"`
	EnableNegRisk                bool                `json:"enableNegRisk"`
	AutomaticallyActive          bool                `json:"automaticallyActive"`
	EventDate                    string              `json:"eventDate"`
	StartTime                    time.Time           `json:"startTime"`
	EventWeek                    int                 `json:"eventWeek"`
	SeriesSlug                   string              `json:"seriesSlug"`
	Score                        string              `json:"score"`
	Elapsed                      string              `json:"elapsed"`
	Period                       string              `json:"period"`
	Live                         bool                `json:"live"`
	Ended                        bool                `json:"ended"`
	FinishedTimestamp            time.Time           `json:"finishedTimestamp"`
	GmpChartMode                 string              `json:"gmpChartMode"`
	TweetCount                   int                 `json:"tweetCount"`
	Chats                        []*Chat              `json:"chats"`
	FeaturedOrder                int                 `json:"featuredOrder"`
	EstimateValue                bool                `json:"estimateValue"`
	CantEstimate                 bool                `json:"cantEstimate"`
	EstimatedValue               string              `json:"estimatedValue"`
	SpreadsMainLine              float64             `json:"spreadsMainLine"`
	TotalsMainLine               float64             `json:"totalsMainLine"`
	CarouselMap                  string              `json:"carouselMap"`
	PendingDeployment            bool                `json:"pendingDeployment"`
	Deploying                    bool                `json:"deploying"`
	DeployingTimestamp           time.Time           `json:"deployingTimestamp"`
	ScheduledDeploymentTimestamp time.Time           `json:"scheduledDeploymentTimestamp"`
	GameStatus                   string              `json:"gameStatus"`
}

type EventCreator struct {
	ID            string    `json:"id"`
	CreatorName   string    `json:"creatorName"`
	CreatorHandle string    `json:"creatorHandle"`
	CreatorURL    string    `json:"creatorUrl"`
	CreatorImage  string    `json:"creatorImage"`
	CreatedAt     time.Time `json:"createdAt"`
	UpdatedAt     time.Time `json:"updatedAt"`
}

type Template struct {
	ID               string `json:"id"`
	EventTitle       string `json:"eventTitle"`
	EventSlug        string `json:"eventSlug"`
	EventImage       string `json:"eventImage"`
	MarketTitle      string `json:"marketTitle"`
	Description      string `json:"description"`
	ResolutionSource string `json:"resolutionSource"`
	NegRisk          bool   `json:"negRisk"`
	SortBy           string `json:"sortBy"`
	ShowMarketImages bool   `json:"showMarketImages"`
	SeriesSlug       string `json:"seriesSlug"`
	Outcomes         string `json:"outcomes"`
}

type Chat struct {
	ID           string    `json:"id"`
	ChannelID    string    `json:"channelId"`
	ChannelName  string    `json:"channelName"`
	ChannelImage string    `json:"channelImage"`
	Live         bool      `json:"live"`
	StartTime    time.Time `json:"startTime"`
	EndTime      time.Time `json:"endTime"`
}

type Category struct {
	ID             string    `json:"id"`
	Label          string    `json:"label"`
	ParentCategory string    `json:"parentCategory"`
	Slug           string    `json:"slug"`
	PublishedAt    string    `json:"publishedAt"`
	CreatedBy      string    `json:"createdBy"`
	UpdatedBy      string    `json:"updatedBy"`
	CreatedAt      time.Time `json:"createdAt"`
	UpdatedAt      time.Time `json:"updatedAt"`
}

type PlyMktTag struct {
	ID          string    `json:"id"`
	Label       string    `json:"label"`
	Slug        string    `json:"slug"`
	ForceShow   bool      `json:"forceShow"`
	PublishedAt string    `json:"publishedAt"`
	CreatedBy   int       `json:"createdBy"`
	UpdatedBy   int       `json:"updatedBy"`
	CreatedAtPly time.Time `json:"createdAt"`
	UpdatedAtPly time.Time `json:"updatedAt"`
	ForceHide   bool      `json:"forceHide"`
	ParentTagID string
	SportSlug   string
	SportID     string
}

type PlyMktUserPosition struct {
	ID        string `json:"id"`
	User      struct {
		ID string `json:"id"`
	} `json:"user"`
	Market struct {
		ID string `json:"id"`
	} `json:"market"`
	OutcomeIndex string `json:"outcomeIndex"` // Often BigInt in subgraph, check if string
	Quantity     string `json:"quantity"`     // Net quantity
	TotalBought  string `json:"totalBought"`
	TotalSold    string `json:"totalSold"`
	NetValue     string `json:"netValue"` // Might need computation if not in subgraph directly
}

type PlyMktOrderbook struct {
	Timestamp string          `json:"timestamp"` // Derived or from snapshot time
	TokenID   string          `json:"token_id"`
	Bids      []OrderbookItem `json:"bids"`
	Asks      []OrderbookItem `json:"asks"`
	Spread    float64         `json:"spread"` // Computed
}

type PositionSnapshot struct {
	SnapshotTime  time.Time
	AccountID     string
	MarketID      string
	OutcomeIndex  int
	NetQuantity   float64
	NetValue      float64
	DeltaQuantity float64
	DeltaValue    float64
	IsSignal      bool
}

type OrderbookItem struct {
	Price string `json:"price"`
	Size  string `json:"size"`
}

type PlyMktPriceHistory struct {
	Timestamp int64   `json:"t"` // Unix timestamp key from CLOB usually
	Price     float64 `json:"p"` 
	High      float64 `json:"h"` // Keeping for compat if needed, but primary is t, p
	Low       float64 `json:"l"`
	Close     float64 `json:"c"`
	Volume    float64 `json:"v"`
	MarketID  string  `json:"-"`
	TokenID   string  `json:"-"`
}

type PlyMktPricePoint struct {
	Timestamp int64       `json:"t"`
	Price     interface{} `json:"p"` // Can be string or float
}

// API Endpoints
const (
	GammaAPIURL = "https://gamma-api.polymarket.com"
	ClobAPIURL  = "https://clob.polymarket.com"
)

// =============================================================================
// Request/Response Types
// =============================================================================

var (
	ErrRequestFailed = errors.New("request failed")
)

type ReqDetails struct {
	URL      string
	Method   string
	Headers  map[string]string
	Body     io.Reader
	Params   url.Values
	Paginated bool
}

// Response represents an HTTP response.
type RespDetails struct {
	URL        string
	StatusCode int
	Body       []byte
	Headers    http.Header
	Duration   time.Duration
	Err        error
}

// Stats tracks request statistics.
type Stats struct {
	RequestsCompleted atomic.Int64
	RequestsFailed    atomic.Int64
	BytesFetched      atomic.Int64
	TotalDuration     atomic.Int64
	RetryCount        atomic.Int64
}

type PlyMktService struct {
	Logger *slog.Logger
	Cfg    *config.Config
	Stats  *Stats
	Ctx    context.Context
}

func (ply *PlyMktService) SyncSubgraph(ctx context.Context) (*RespDetails, error) {
	// Group 1: Clob/Orderbook-Centric Strategies
	// 	(Latency Arb, Market Making, Asymmetric Scalp, Maker Rebates)
	// ordersMatchedEvents
	// enrichedOrderFilleds

	// Group 3: Transaction/Position-Centric Strategies
	//	(Copy Trading, Stat Arb, Wallet Basket, Merges/Splits/Redemptions)
	// marketProfits
	// merges
	// splits
	// redemptions
	return nil, nil
}
type PlyMktTeam struct {
	ID           int    `json:"id"`
	Name         string `json:"name"`
	League       string `json:"league"`
	Record       string `json:"record"`
	Logo         string `json:"logo"`
	Abbreviation string `json:"abbreviation"`
	Alias        string `json:"alias"`
	ProviderID   int    `json:"providerID"`
	Color        string `json:"color"`
	CreatedAtPly string `json:"createdAt"`
	UpdatedAtPly string `json:"updatedAt"`
	SportSlug    string `json:"sportSlug"`
	SportID      string
}

// PlyMktSport represents an item from the /sports API (Leagues)
type PlyMktSport struct {
	Sport      string `json:"sport"`
	Image      string `json:"image"`
	Resolution string `json:"resolution"`
	Ordering   string `json:"ordering"`
	Tags       string `json:"tags"`
	Series     string `json:"series"`
	Slug       string `json:"slug"`
	SportSlug  string // The parent Sport Category Slug
	SportID    string
}

// PlyMktSportCategory represents a row in the 'sports' table (Categories like Football, Basketball)
type PlyMktSportCategory struct {
	Slug         string
	PrimaryTagID string
	ID           string
}

type targetDetails struct {
	path   string
	params map[string]string
	body   string
}

func (ply *PlyMktService) GetDiscoveryReqs(ctx context.Context) ([]*fetcher.Request, error) {
	gammaEndpoint, ok := config.DefaultEndpoints["gamma"].(string)
	if !ok {
		return nil, fmt.Errorf("gamma endpoint not configured")
	}
	
	// Base URL for events
	u, err := url.Parse(gammaEndpoint)
	if err != nil {
		return nil, err
	}
	u = u.JoinPath("/events")
	
	// Query Params: active=true, closed=false (implied by active?), archived=false
	// User said "active events".
	// We'll return an initial request that triggers pagination in the Processor/Fetcher loop.
	limit := 100
	offset := 0
	
	var reqs []*fetcher.Request
	// Initial request
	q := u.Query()
	q.Set("limit", fmt.Sprintf("%d", limit))
	q.Set("offset", fmt.Sprintf("%d", offset))
	q.Set("active", "true")
	q.Set("closed", "false")
	q.Set("archived", "false")
	q.Set("order", "volume")
	q.Set("ascending", "false")
	
	fullURL := *u
	fullURL.RawQuery = q.Encode()
	
	reqs = append(reqs, &fetcher.Request{
		URL: fullURL.String(),
		Method: "GET",
		Headers: map[string]string{"Content-Type": "application/json"},
	})
	
	return reqs, nil
}

func (ply *PlyMktService) GetSportsReqs(ctx context.Context) ([]*fetcher.Request, error) {
	//sportsCats := map[string]int {
	//	"football": 0,
	//	"basketball": 0,
	//	"hockey": 0,
	//	"tennis": 0,
	//	"esports": 0,
	//	"baseball": 0,
	//	"soccer": 0,
	//	"cricket": 0,
	//	"rugby": 0,
	//	"golf": 0,
	//	"ufc": 0,
	//	"f1": 0,
	//	"chess": 0,
	//	"boxing": 0,
	//	"pickleball": 0
	//}
	gammaEndpoint, ok := config.DefaultEndpoints["gamma"].(string)
	if !ok {
		return nil, fmt.Errorf("gamma endpoint not configured")
	}
	baseUrl, err := url.Parse(gammaEndpoint)
	if err != nil {
		return nil, err
	}

	limit := 500
	defaultOffset := 0
	targets := map[string]targetDetails{
		"tags": {
			path: "/tags",
		},
		"sports": {
			path: "/sports",
		},
		"teams": {
			path: "/teams",
		},
	}

	var reqs []*fetcher.Request

	for _, target := range targets {
		currQuery := url.Values{}
		if target.path == "/tags" {
			limit = 300
		} else if target.path == "/teams" {
			limit = 500
		}

		currQuery.Add("limit", fmt.Sprintf("%d", limit))
		currQuery.Add("offset", fmt.Sprintf("%d", defaultOffset))
		fullURL := baseUrl.JoinPath(target.path)
		fullURL.RawQuery = currQuery.Encode()

		r := &fetcher.Request{
			URL:     fullURL.String(),
			Headers: map[string]string{"Content-Type": "application/json"},
			Method:  "GET",
			Params:  fullURL.Query(),
		}
		reqs = append(reqs, r)
		defaultOffset += limit
	}

	return reqs, nil
}

var SubgraphEntities = map[string]string{
	"ordersMatchedEvents": `ordersMatchedEvents(first: $first, where: { id_gt: $lastId }, orderBy: id, orderDirection: asc) {
		id
		takerAmountFilled
		makerAmountFilled
		makerAssetID
		takerAssetID
		timestamp
	}`,
	"orderFilledEvents": `orderFilledEvents(first: $first, where: { id_gt: $lastId }, orderBy: id, orderDirection: asc) {
		id
		fee
		maker {
			id
		}
		makerAssetId
		makerAmountFilled
		taker {
			id
		}
		takerAmountFilled
		takerAssetId
		timestamp
		transactionHash
	}`,
	// enrichedOrderFilleds: Default query for full sync. Use BuildEnrichedOrderFilledsQuery() for market-filtered queries.
	"enrichedOrderFilleds": `enrichedOrderFilleds(first: $first, where: { id_gt: $lastId }, orderBy: id, orderDirection: asc) {
		id
		timestamp
		maker {
			id
		}
		taker {
			id
		}
		size
		side
		price
		market {
			id
		}
	}`,
	"accounts": `accounts(first: $first, where: { id_gt: $lastId }) {
		id
		creationTimestamp
		lastSeenTimestamp
		lastTradedTimestamp
		collateralVolume
		numTrades
		profit
		scaledCollateralVolume
		scaledProfit
	}`,
	"conditions": `condition(id: $conditionId) {
		id
		oracle
		questionId
		outcomeSlotCount
		creator
		createTimestamp
		resolveTimestamp
		payoutNumerators
		payouts
		fixedProductMarketMakers {
			id
		}
	}`,
	"fpmms": `fpmms(first: $first, where: { id_gt: $lastId }, orderBy: id, orderDirection: asc) {
		conditionId,
		id
	}`,
	"userPositions": `userPositions(
		first: $first
		where: {user: $userId, id_gt: $lastId}
		orderBy: amount
		orderDirection: asc
		subgraphError: allow
	) {
		id
		user
		tokenId
		amount
		realizedPnl
		avgPrice
		totalBought
	}`,
}

// BuildEnrichedOrderFilledsQuery builds the enrichedOrderFilleds GraphQL query.
// If marketIds is non-empty, adds a market_in filter; otherwise queries all markets.
func BuildEnrichedOrderFilledsQuery(marketIds []string) string {
	whereClause := "{ id_gt: $lastId }"
	if len(marketIds) > 0 {
		whereClause = "{ id_gt: $lastId, market_in: $marketIds }"
	}
	
	return fmt.Sprintf(`enrichedOrderFilleds(
		first: $first
		where: %s
		orderBy: timestamp
		orderDirection: desc
	) {
		id
		timestamp
		maker {
			id
		}
		taker {
			id
		}
		size
		side
		price
		market {
			id
		}
	}`, whereClause)
}

type PlyMktCondition struct {
    ID                   string     `json:"id"`
    Oracle               string     `json:"oracle"`               // Bytes → string
    OutcomeSlotCount     int        `json:"outcomeSlotCount"`
    PayoutDenominator    string     `json:"payoutDenominator"`
    PayoutNumerators     []string   `json:"payoutNumerators"`
    Payouts              []string   `json:"payouts"`              // Final redemption multipliers
    QuestionId           string     `json:"questionId"`           // Bytes → string
    ResolutionHash       string     `json:"resolutionHash"`
    ResolutionTimestamp  string     `json:"resolutionTimestamp"`
}

type PlyMktAccount struct {
    ID                     string             `json:"id"`
    CreationTimestamp      string             `json:"creationTimestamp"`
    LastSeenTimestamp      string             `json:"lastSeenTimestamp"`
    CollateralVolume       string             `json:"collateralVolume"`
    NumTrades              string             `json:"numTrades"`
    ScaledCollateralVolume string             `json:"scaledCollateralVolume"`
    LastTradedTimestamp    string             `json:"lastTradedTimestamp"`
    Profit                 string             `json:"profit"`
    ScaledProfit           string             `json:"scaledProfit"`
}

type PlyMktEnrichedOrderFilledEvent struct {
	ID        string `json:"id"`    // Tx hash
	Price     string `json:"price"` // Fill price
	Side      string `json:"side"`  // Maker or taker
	Size      string `json:"size"`  // Fill size
	Maker     struct {
		ID string `json:"id"`
	} `json:"maker"`
	Taker struct {
		ID string `json:"id"`
	} `json:"taker"`
	Market struct {
		ID string `json:"id"`
	} `json:"market"`
	Timestamp string `json:"timestamp"`
}

type PlyMktOrderFilledEvent struct {
	ID                string `json:"id"`
	MakerAssetID      string `json:"makerAssetID"` // Token ID
	TakerAssetID      string `json:"takerAssetID"`
	MakerAmountFilled string `json:"makerAmountFilled"` // Filled amount
	TakerAmountFilled string `json:"takerAmountFilled"`
	Maker             struct {
		ID string `json:"id"`
	} `json:"maker"`
	Taker struct {
		ID string `json:"id"`
	} `json:"taker"`
	Fee             string `json:"fee"`
	Timestamp       string `json:"timestamp"`
	TransactionHash string `json:"transactionHash"`
}

var targets = map[string]struct{
	path string
	entity string
}{
	"conditions": {
		path: "/81Dm16JjuFSrqz813HysXoUPvzTwE7fsfPk2RTf66nyC", // Example path, or use ID from stub
		entity: "conditions",
	},
	"orderFilledEvents": {
		path: "/81Dm16JjuFSrqz813HysXoUPvzTwE7fsfPk2RTf66nyC",
		entity: "orderFilledEvents",
	},
	"enrichedOrderFilleds": {
		path: "/81Dm16JjuFSrqz813HysXoUPvzTwE7fsfPk2RTf66nyC",
		entity: "enrichedOrderFilleds",
	},
	"accounts": {
		path: "/81Dm16JjuFSrqz813HysXoUPvzTwE7fsfPk2RTf66nyC",
		entity: "accounts",
	},
	"fpmms": {
		path: "/6c58N5U4MtQE2Y8njfVrrAfRykzfqajMGeTMEvMmskVz", 
		entity: "fpmms",
	},
	"userPositions": {
		path: "/6c58N5U4MtQE2Y8njfVrrAfRykzfqajMGeTMEvMmskVz", 
		entity: "userPositions",
	},
}

func (ply *PlyMktService) GetPriceHistoryReq(tokenID string, fidelity int, startTs int64) (*fetcher.Request, error) {
	clobEndpoint, ok := config.DefaultEndpoints["clob"].(string)
	if !ok || clobEndpoint == "" {
		return nil, fmt.Errorf("clob endpoint not configured")
	}

	u, err := url.Parse(clobEndpoint)
	if err != nil {
		return nil, err
	}
	u = u.JoinPath("/prices-history")
	
	q := u.Query()
	q.Set("market", tokenID)
	q.Set("fidelity", fmt.Sprintf("%d", fidelity))
	if startTs > 0 {
		q.Set("startTs", fmt.Sprintf("%d", startTs))
	}
	u.RawQuery = q.Encode()

	return &fetcher.Request{
		URL:    u.String(),
		Method: "GET",
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Metadata: map[string]string{
			"Type":     "price_history",
			"Expected": "history_points",
			"TokenID":  tokenID,
		},
	}, nil
}

func (ply *PlyMktService) BuildTempRequests(targetsList []string) ([]*fetcher.Request, error) {
	var reqs []*fetcher.Request
	for _, target := range targetsList {
		if req, err := ply.gqlRequestBuilder(target, ply.Cfg.Services.PlyMkt.Endpoints["subgraph"].(string), targets[target].path, SubgraphEntities[target]); err != nil {
			return nil, err
		} else {
			reqs = append(reqs, req)
		}
	}

	return reqs, nil
}

func (ply *PlyMktService) GetSubgraphReqs(ctx context.Context, targetsList []string) ([]*fetcher.Request, error) {
    // We need the host. 
    host := ply.Cfg.Services.PlyMkt.Endpoints["subgraph"].(string) // generic placeholder
	var reqs []*fetcher.Request
    
	for _, target := range targetsList {
		if req, err := ply.gqlRequestBuilder(target, host, targets[target].path, SubgraphEntities[target]); err != nil {
			return nil, err
		} else {
			reqs = append(reqs, req)
		}
	}

	return reqs, nil
}

func (ply *PlyMktService) gqlRequestBuilder(key, host, path, entityQuery string) (*fetcher.Request, error) {
	u, _ := url.Parse(host)
	u = u.JoinPath(path)
	
	// Cursor pagination: id_gt instead of skip
	fullQuery := fmt.Sprintf(`query MyQuery($first: Int, $lastId: String) {
		%s
	}`, entityQuery)
	
	// Initial Body: first=1000, lastId=""
	bodyData := map[string]any{
		"query": fullQuery,
		"variables": map[string]any{
			"first": 1000,
			"lastId": "",
		},
	}
	
	bodyBytes, err := json.Marshal(bodyData)
	if err != nil {
		ply.Logger.Error("failed to marshal body", slog.String("entity", key))
		return nil, err
	}

	return &fetcher.Request{
		URL: u.String(),
		Method: "POST",
		Headers: map[string]string{
			"Content-Type": "application/json",
			"Authorization": fmt.Sprintf("Bearer %s", ply.Cfg.SubgraphAPIKey),
		},
		Body: bytes.NewReader(bodyBytes),
		Metadata: map[string]string{
			"Type": "subgraph",
			"Entity": key,
			"GraphqlQuery": fullQuery,
			"CursorPagination": "true", // Flag for Processor/Fetcher
		},
	}, nil
}

// TODO: need to implement this sports sync using the pipeline, but need to
// verify pagination tests if existent

// TODO: need to implement a pipeline to fetch all this subgraph stuff
// query := `query FetchClobData($first: Int = 10, $skip: Int = 0) {
//   ordersMatchedEvents(
//     first: $first
//     skip: $skip
//     orderBy: timestamp
//     orderDirection: asc
//   ) {
//     id
//     takerAmountFilled
//     makerAmountFilled
//     makerAssetID
//     takerAssetID
//   }
//{
// orderbooks(first: 10, orderBy: id, orderDirection: desc) {
//   id
//   buysQuantity
//   collateralVolume
//   scaledCollateralVolume
//   sellsQuantity
//   tradesQuantity
//   lastActiveDay
//   scaledCollateralSellVolume
//   scaledCollateralBuyVolume
//   collateralBuyVolume
//   collateralSellVolume	
// }
//}
//   transactions(
//     orderBy: timestamp
//     orderDirection: desc
//     first: $first
//     skip: $skip
//   ) {
//     feeAmount
//     id
//     market {
//       id
//     }
//     outcomeIndex
//     outcomeTokensAmount
//     tradeAmount
//     type
//     timestamp
//   }
//   enrichedOrderFilleds(
//     first: $first
//     skip: $skip
//     orderBy: timestamp
//     orderDirection: desc
//   ) {
//     id
//     timestamp
//     maker {
//       id
//     }
//     taker {
//       id
//     }
//     price
//     side
//     size
//   }
//{
//  accounts {
//    id
//    creationTimestamp
//    lastSeenTimestamp
//    collateralVolume
//    lastTradedTimestamp
//    numTrades
//    profit
//    scaledCollateralVolume
//    scaledProfit
//  }
//}
//	
//{
//  orderFilledEvents(first: 10, orderBy: id, orderDirection: asc, skip: 10) {
//    fee
//    id
//    makerAssetId
//    makerAmountFilled
//    takerAmountFilled
//    takerAssetId
//    timestamp
//  }
//}

//query FetchPLData($first: Int = 10, $skip: Int = 0) {
//	marketProfits
//	merges
//	splits
//	redemptions
//	transactions
//}
//{
//{
//  conditions {
//    id
//    payouts
//    questionId
//    resolutionTimestamp
//  }
//}
// activity subgraph https://gateway.thegraph.com/api/[api-key]/subgraphs/id/4LkKSgkqijUccYMYMYUPtjXswrdK3xipPMfs3fa7gfef
//{
//  fixedProductMarketMakers(first: 10, orderBy: id, orderDirection: asc, skip: 10) {
//    id
//  }
//}

//
//{
//  merges(first: 5, orderBy: id, orderDirection: asc, skip: 10) {
//    id
//    timestamp
//    stakeholder
//    condition
//    amount
//  }
//}

//
//{
//  negRiskConversions(first: 10, orderBy: id, orderDirection: asc, skip: 10) {
//    amount
//    id
//    indexSet
//    negRiskMarketId
//    questionCount
//    stakeholder
//    timestamp
//  }
//}

//
//{
//  negRiskEvents(first: 10, orderBy: id, orderDirection: asc, skip: 10) {
//    id
//  }
//}
//
//{
//  positions(first: 10, orderBy: id, orderDirection: asc) {
//    id
//    condition
//    outcomeIndex
//  }
//}

//
//{
//  redemptions {
//    id
//    condition
//    indexSets
//    payout
//    redeemer
//    timestamp
//  }
//}
//
//{	
//  splits {
//    id
//    timestamp
//    stakeholder
//    condition
//    amount
//  }
//}	