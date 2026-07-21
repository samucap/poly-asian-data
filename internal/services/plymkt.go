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
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/fetcher"
)

type PlyMktMarket struct {
	ID                           string         `json:"id"`
	Question                     string         `json:"question"`
	ConditionID                  string         `json:"conditionId"`
	Slug                         string         `json:"slug"`
	TwitterCardImage             string         `json:"twitterCardImage"`
	ResolutionSource             string         `json:"resolutionSource"`
	EndDate                      time.Time      `json:"endDate" time_format:"2026-03-31T12:00:00Z"`
	Category                     string         `json:"category"`
	AmmType                      string         `json:"ammType"`
	Liquidity                    string         `json:"liquidity"` // String to preserve precision
	SponsorName                  string         `json:"sponsorName"`
	SponsorImage                 string         `json:"sponsorImage"`
	StartDate                    time.Time      `json:"startDate" time_format:"2026-03-31T12:00:00Z"`
	XAxisValue                   string         `json:"xAxisValue"`
	YAxisValue                   string         `json:"yAxisValue"`
	DenominationToken            string         `json:"denominationToken"`
	Fee                          string         `json:"fee"` // String to preserve precision
	Image                        string         `json:"image"`
	Icon                         string         `json:"icon"`
	LowerBound                   string         `json:"lowerBound"`
	UpperBound                   string         `json:"upperBound"`
	Description                  string         `json:"description"`
	Outcomes                     string         `json:"outcomes"`
	OutcomePrices                string         `json:"outcomePrices"`
	Volume                       string         `json:"volume"` // String to preserve precision
	Active                       bool           `json:"active"`
	MarketType                   string         `json:"marketType"`
	FormatType                   string         `json:"formatType"`
	LowerBoundDate               string         `json:"lowerBoundDate"`
	UpperBoundDate               string         `json:"upperBoundDate"`
	Closed                       bool           `json:"closed"`
	MarketMakerAddress           string         `json:"marketMakerAddress"`
	CreatedBy                    int64          `json:"createdBy"`
	UpdatedBy                    int64          `json:"updatedBy"`
	CreatedAt                    time.Time      `json:"createdAt" time_format:"2025-03-26T14:44:31.96609Z"`
	UpdatedAt                    time.Time      `json:"updatedAt" time_format:"2025-03-26T14:44:31.96609Z"`
	ClosedTime                   string         `json:"closedTime"`
	WideFormat                   bool           `json:"wideFormat"`
	New                          bool           `json:"new"`
	MailchimpTag                 string         `json:"mailchimpTag"`
	Featured                     bool           `json:"featured"`
	Archived                     bool           `json:"archived"`
	ResolvedBy                   string         `json:"resolvedBy"`
	Restricted                   bool           `json:"restricted"`
	MarketGroup                  int64          `json:"marketGroup"`
	GroupItemTitle               string         `json:"groupItemTitle"`
	GroupItemThreshold           string         `json:"groupItemThreshold"`
	QuestionID                   string         `json:"questionID"`
	UmaEndDate                   string         `json:"umaEndDate"`
	EnableOrderBook              bool           `json:"enableOrderBook"`
	OrderPriceMinTickSize        float64        `json:"orderPriceMinTickSize"`
	OrderMinSize                 float64        `json:"orderMinSize"`
	UmaResolutionStatus          string         `json:"umaResolutionStatus"`
	CurationOrder                int64          `json:"curationOrder"`
	VolumeNum                    float64        `json:"volumeNum"`
	LiquidityNum                 float64        `json:"liquidityNum"`
	EndDateIso                   string         `json:"endDateIso"`
	StartDateIso                 string         `json:"startDateIso"`
	UmaEndDateIso                string         `json:"umaEndDateIso"`
	HasReviewedDates             bool           `json:"hasReviewedDates"`
	ReadyForCron                 bool           `json:"readyForCron"`
	CommentsEnabled              bool           `json:"commentsEnabled"`
	Volume24hr                   float64        `json:"volume24hr"`
	Volume1wk                    float64        `json:"volume1wk"`
	Volume1mo                    float64        `json:"volume1mo"`
	Volume1yr                    float64        `json:"volume1yr"`
	GameStartTime                string         `json:"gameStartTime"`
	SecondsDelay                 int64          `json:"secondsDelay"`
	ClobTokenIds                 string         `json:"clobTokenIds"`
	DisqusThread                 string         `json:"disqusThread"`
	ShortOutcomes                string         `json:"shortOutcomes"`
	TeamAID                      string         `json:"teamAID"`
	TeamBID                      string         `json:"teamBID"`
	UmaBond                      string         `json:"umaBond"`
	UmaReward                    string         `json:"umaReward"`
	FpmmLive                     bool           `json:"fpmmLive"`
	Volume24hrAmm                float64        `json:"volume24hrAmm"`
	Volume1wkAmm                 float64        `json:"volume1wkAmm"`
	Volume1moAmm                 float64        `json:"volume1moAmm"`
	Volume1yrAmm                 float64        `json:"volume1yrAmm"`
	Volume24hrClob               float64        `json:"volume24hrClob"`
	Volume1wkClob                float64        `json:"volume1wkClob"`
	Volume1moClob                float64        `json:"volume1moClob"`
	Volume1yrClob                float64        `json:"volume1yrClob"`
	VolumeAmm                    float64        `json:"volumeAmm"`
	VolumeClob                   float64        `json:"volumeClob"`
	LiquidityAmm                 float64        `json:"liquidityAmm"`
	LiquidityClob                float64        `json:"liquidityClob"`
	MakerBaseFee                 int64          `json:"makerBaseFee"`
	TakerBaseFee                 int64          `json:"takerBaseFee"`
	CustomLiveness               int64          `json:"customLiveness"`
	AcceptingOrders              bool           `json:"acceptingOrders"`
	NotificationsEnabled         bool           `json:"notificationsEnabled"`
	Score                        int64          `json:"score"`
	ImageOptimized               ImageOptimized `json:"imageOptimized"`
	IconOptimized                ImageOptimized `json:"iconOptimized"`
	Creator                      string         `json:"creator"`
	Ready                        bool           `json:"ready"`
	Funded                       bool           `json:"funded"`
	PastSlugs                    string         `json:"pastSlugs"`
	ReadyTimestamp               time.Time      `json:"readyTimestamp"`
	FundedTimestamp              time.Time      `json:"fundedTimestamp"`
	AcceptingOrdersTimestamp     time.Time      `json:"acceptingOrdersTimestamp"`
	Competitive                  float64        `json:"competitive"`
	RewardsMinSize               float64        `json:"rewardsMinSize"`
	RewardsMaxSpread             float64        `json:"rewardsMaxSpread"`
	Spread                       float64        `json:"spread"`
	AutomaticallyResolved        bool           `json:"automaticallyResolved"`
	OneDayPriceChange            float64        `json:"oneDayPriceChange"`
	OneHourPriceChange           float64        `json:"oneHourPriceChange"`
	OneWeekPriceChange           float64        `json:"oneWeekPriceChange"`
	OneMonthPriceChange          float64        `json:"oneMonthPriceChange"`
	OneYearPriceChange           float64        `json:"oneYearPriceChange"`
	LastTradePrice               float64        `json:"lastTradePrice"`
	BestBid                      float64        `json:"bestBid"`
	BestAsk                      float64        `json:"bestAsk"`
	AutomaticallyActive          bool           `json:"automaticallyActive"`
	ClearBookOnStart             bool           `json:"clearBookOnStart"`
	ChartColor                   string         `json:"chartColor"`
	SeriesColor                  string         `json:"seriesColor"`
	ShowGmpSeries                bool           `json:"showGmpSeries"`
	ShowGmpOutcome               bool           `json:"showGmpOutcome"`
	ManualActivation             bool           `json:"manualActivation"`
	NegRiskOther                 bool           `json:"negRiskOther"`
	GameID                       string         `json:"gameId"`
	GroupItemRange               string         `json:"groupItemRange"`
	SportsMarketType             string         `json:"sportsMarketType"`
	Line                         float64        `json:"line"`
	UmaResolutionStatuses        string         `json:"umaResolutionStatuses"`
	PendingDeployment            bool           `json:"pendingDeployment"`
	Deploying                    bool           `json:"deploying"`
	DeployingTimestamp           time.Time      `json:"deployingTimestamp"`
	ScheduledDeploymentTimestamp time.Time      `json:"scheduledDeploymentTimestamp"`
	RfqEnabled                   bool           `json:"rfqEnabled"`
	EventStartTime               time.Time      `json:"eventStartTime"`
	OpenInterest                 float64        `json:"-" db:"oi"` // Set from /oi endpoint, not from Gamma JSON
	LastFetched                  time.Time      `json:"-" db:"last_fetched"`
	ComputedScore                float64
	EventID                      string `json:"-" db:"event_id"`
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
	ID                           string          `json:"id"`
	Ticker                       string          `json:"ticker"`
	Slug                         string          `json:"slug"`
	Title                        string          `json:"title"`
	Subtitle                     string          `json:"subtitle"`
	Description                  string          `json:"description"`
	ResolutionSource             string          `json:"resolutionSource"`
	StartDate                    time.Time       `json:"startDate"`
	CreationDate                 time.Time       `json:"creationDate"`
	EndDate                      time.Time       `json:"endDate"`
	Image                        string          `json:"image"`
	Icon                         string          `json:"icon"`
	Active                       bool            `json:"active"`
	Closed                       bool            `json:"closed"`
	Archived                     bool            `json:"archived"`
	New                          bool            `json:"new"`
	Featured                     bool            `json:"featured"`
	Restricted                   bool            `json:"restricted"`
	Liquidity                    float64         `json:"liquidity"`
	Volume                       float64         `json:"volume"`
	OpenInterest                 float64         `json:"openInterest"`
	SortBy                       string          `json:"sortBy"`
	Category                     string          `json:"category"`
	Subcategory                  string          `json:"subcategory"`
	IsTemplate                   bool            `json:"isTemplate"`
	TemplateVariables            string          `json:"templateVariables"`
	PublishedAt                  string          `json:"published_at"`
	CreatedBy                    string          `json:"createdBy"`
	UpdatedBy                    string          `json:"updatedBy"`
	CreatedAt                    time.Time       `json:"createdAt"`
	UpdatedAt                    time.Time       `json:"updatedAt"`
	CommentsEnabled              bool            `json:"commentsEnabled"`
	Competitive                  float64         `json:"competitive"`
	Volume24hr                   float64         `json:"volume24hr"`
	Volume1wk                    float64         `json:"volume1wk"`
	Volume1mo                    float64         `json:"volume1mo"`
	Volume1yr                    float64         `json:"volume1yr"`
	FeaturedImage                string          `json:"featuredImage"`
	DisqusThread                 string          `json:"disqusThread"`
	ParentEvent                  string          `json:"parentEvent"`
	EnableOrderBook              bool            `json:"enableOrderBook"`
	LiquidityAmm                 float64         `json:"liquidityAmm"`
	LiquidityClob                float64         `json:"liquidityClob"`
	NegRisk                      bool            `json:"negRisk"`
	NegRiskMarketID              string          `json:"negRiskMarketID"`
	NegRiskFeeBips               int             `json:"negRiskFeeBips"`
	CommentCount                 int             `json:"commentCount"`
	ImageOptimized               ImageOptimized  `json:"imageOptimized"`
	IconOptimized                ImageOptimized  `json:"iconOptimized"`
	FeaturedImageOptimized       ImageOptimized  `json:"featuredImageOptimized"`
	SubEvents                    []string        `json:"subEvents"`
	Markets                      []*PlyMktMarket `json:"markets"`
	Categories                   string          `json:"categories"`
	Collections                  string          `json:"collections"`
	Tags                         []*PlyMktTag    `json:"tags"`
	Cyom                         bool            `json:"cyom"`
	ClosedTime                   time.Time       `json:"closedTime"`
	ShowAllOutcomes              bool            `json:"showAllOutcomes"`
	ShowMarketImages             bool            `json:"showMarketImages"`
	AutomaticallyResolved        bool            `json:"automaticallyResolved"`
	EnableNegRisk                bool            `json:"enableNegRisk"`
	AutomaticallyActive          bool            `json:"automaticallyActive"`
	EventDate                    string          `json:"eventDate"`
	StartTime                    time.Time       `json:"startTime"`
	EventWeek                    int             `json:"eventWeek"`
	SeriesSlug                   string          `json:"seriesSlug"`
	Score                        string          `json:"score"`
	Elapsed                      string          `json:"elapsed"`
	Period                       string          `json:"period"`
	Live                         bool            `json:"live"`
	Ended                        bool            `json:"ended"`
	FinishedTimestamp            time.Time       `json:"finishedTimestamp"`
	GmpChartMode                 string          `json:"gmpChartMode"`
	TweetCount                   int             `json:"tweetCount"`
	Chats                        []*Chat         `json:"chats"`
	FeaturedOrder                int             `json:"featuredOrder"`
	EstimateValue                bool            `json:"estimateValue"`
	CantEstimate                 bool            `json:"cantEstimate"`
	EstimatedValue               string          `json:"estimatedValue"`
	SpreadsMainLine              float64         `json:"spreadsMainLine"`
	TotalsMainLine               float64         `json:"totalsMainLine"`
	CarouselMap                  string          `json:"carouselMap"`
	PendingDeployment            bool            `json:"pendingDeployment"`
	Deploying                    bool            `json:"deploying"`
	DeployingTimestamp           time.Time       `json:"deployingTimestamp"`
	ScheduledDeploymentTimestamp time.Time       `json:"scheduledDeploymentTimestamp"`
	GameStatus                   string          `json:"gameStatus"`
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
	ID           string    `json:"id"`
	Label        string    `json:"label"`
	Slug         string    `json:"slug"`
	ForceShow    bool      `json:"forceShow"`
	PublishedAt  string    `json:"publishedAt"`
	CreatedBy    int       `json:"createdBy"`
	UpdatedBy    int       `json:"updatedBy"`
	CreatedAtPly time.Time `json:"createdAt"`
	UpdatedAtPly time.Time `json:"updatedAt"`
	ForceHide    bool      `json:"forceHide"`
	// ActiveEventsCount is Gamma's count of active events for this tag (related-tags / tag payloads).
	// Used to validate our attribution; not a metric we recompute into TotalMarkets.
	ActiveEventsCount int `json:"activeEventsCount"`
	// Derived or from API; used by processor/saver for sport link and hierarchy
	SportSlug    string  `json:"sportSlug"`
	SportID      string  `json:"-"`
	ParentTagID  string  `json:"parentTagId"`
	TotalVol     float64 `json:"totalVol"`
	TotalVol24hr float64 `json:"totalVol24hr"`
	TotalLiq     float64 `json:"totalLiq"`
	TotalMarkets int     `json:"totalMarkets"`
}

type PlyMktUserPosition struct {
	ID          string `json:"id"`
	User        string `json:"user"`
	TokenID     string `json:"tokenId"`
	Amount      string `json:"amount"` // Net quantity
	AvgPrice    string `json:"avgPrice"`
	RealizedPnl string `json:"realizedPnl"`
	TotalBought string `json:"totalBought"`
}

type PlyMktOrderbook struct {
	Timestamp string          `json:"timestamp"` // Derived or from snapshot time
	TokenID   string          `json:"token_id"`
	AssetID   string          `json:"asset_id"` // From batch API response
	Market    string          `json:"market"`   // Market identifier from batch API
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
	TokenID   string  `json:"-"  db:"token_id"`     // clobTokenId (PK part 1)
	Timestamp int64   `json:"t"  db:"timestamp"`    // Unix seconds (PK part 2)
	Price     float64 `json:"p"  db:"price"`        // Probability/price [0,1]
	MarketID  string  `json:"-"  db:"market_id"`    // Gamma market conditionId for joins
	Fidelity  int     `json:"-"  db:"fidelity_min"` // Resolution in minutes (e.g. 5, 60)
	UpdatedAt int64   `json:"-"  db:"updated_at"`   // Unix seconds when row was last upserted
}

type PlyMktTrade struct {
	ProxyWallet           string  `json:"proxyWallet"`
	Side                  string  `json:"side"`
	Asset                 string  `json:"asset"`
	ConditionId           string  `json:"conditionId"`
	Size                  float64 `json:"size"`
	Price                 float64 `json:"price"`
	Timestamp             int64   `json:"timestamp"`
	Title                 string  `json:"title"`
	Slug                  string  `json:"slug"`
	Icon                  string  `json:"icon"`
	EventSlug             string  `json:"eventSlug"`
	Outcome               string  `json:"outcome"`
	OutcomeIndex          int     `json:"outcomeIndex"`
	Name                  string  `json:"name"`
	Pseudonym             string  `json:"pseudonym"`
	Bio                   string  `json:"bio"`
	ProfileImage          string  `json:"profileImage"`
	ProfileImageOptimized string  `json:"profileImageOptimized"`
	TransactionHash       string  `json:"transactionHash"`
}

// API Endpoints
const (
	GammaAPIURL = "https://gamma-api.polymarket.com"
	ClobAPIURL  = "https://clob.polymarket.com"
)

// =============================================================================
// Request/Response Types
// =============================================================================

var ErrRequestFailed = errors.New("request failed")

type ReqDetails struct {
	URL       string
	Method    string
	Headers   map[string]string
	Body      io.Reader
	Params    url.Values
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

// =============================================================================
// Data API (Leaderboard & Holders)
// =============================================================================

type PlyMktLeaderboardEntry struct {
	Rank          string  `json:"rank"`
	ProxyWallet   string  `json:"proxyWallet"`
	UserName      string  `json:"userName"`
	Vol           float64 `json:"vol"`
	Pnl           float64 `json:"pnl"`
	ProfileImage  string  `json:"profileImage"`
	XUsername     string  `json:"xUsername"`
	VerifiedBadge bool    `json:"verifiedBadge"`
	// Additional fields we might want to track metadata
	Period string `json:"-"`
}

type PlyMktLeaderboardParams struct {
	Category   string
	TimePeriod string
	OrderBy    string
	Limit      int
	Offset     int
	User       string
}

type PlyMktHolderEntry struct {
	ProxyWallet           string  `json:"proxyWallet"`
	Pseudonym             string  `json:"pseudonym"`
	Name                  string  `json:"name"` // Added based on feedback
	Amount                float64 `json:"amount"`
	Bio                   string  `json:"bio"`
	ProfileImage          string  `json:"profileImage"`
	ProfileImageOptimized string  `json:"profileImageOptimized"`
	DisplayUsernamePublic bool    `json:"displayUsernamePublic"`
	OutcomeIndex          int     `json:"outcomeIndex"`
	Asset                 string  `json:"asset"`
}

type PlyMktHolderToken struct {
	Token   string              `json:"token"`
	Holders []PlyMktHolderEntry `json:"holders"`
}

type PlyMktUser struct {
	ProxyWallet   string  `json:"proxyWallet"`
	Username      string  `json:"username"`
	Name          string  `json:"name"`
	Bio           string  `json:"bio"`
	ProfileImage  string  `json:"profileImage"`
	XUsername     string  `json:"xUsername"`
	VerifiedBadge bool    `json:"verifiedBadge"`
	Vol           float64 `json:"vol"`
	Pnl           float64 `json:"pnl"`
	Rank          int     `json:"rank"`
}

type PlyMktHolderRecord struct {
	TokenID     string    `json:"tokenID"`
	ProxyWallet string    `json:"proxyWallet"`
	Amount      float64   `json:"amount"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

// PlyMktHoldersBundle groups users and holders for atomic saving
type PlyMktHoldersBundle struct {
	Users   []PlyMktUser
	Holders []PlyMktHolderRecord
}

func (ply *PlyMktService) GetLeaderboardReqs(ctx context.Context, params PlyMktLeaderboardParams) ([]*fetcher.Request, error) {
	dataEndpoint, ok := config.DefaultEndpoints["data"].(string)
	if !ok {
		// Fallback or error? Config should be loaded.
		// If using injected cfg, try that.
		if ply.Cfg != nil {
			if ep, ok := ply.Cfg.Services.PlyMkt.Endpoints["data"].(string); ok {
				dataEndpoint = ep
			}
		}
		if dataEndpoint == "" {
			dataEndpoint = "https://data-api.polymarket.com" // Default fallback
		}
	}

	u, err := url.Parse(dataEndpoint)
	if err != nil {
		return nil, err
	}
	u = u.JoinPath("/v1/leaderboard")

	q := u.Query()
	if params.Category != "" {
		q.Set("category", params.Category)
	}
	if params.TimePeriod != "" {
		q.Set("timePeriod", params.TimePeriod)
	}
	if params.OrderBy != "" {
		q.Set("orderBy", params.OrderBy)
	}
	if params.Limit > 0 {
		q.Set("limit", fmt.Sprintf("%d", params.Limit))
	}
	if params.Offset > 0 {
		q.Set("offset", fmt.Sprintf("%d", params.Offset))
	}
	if params.User != "" {
		q.Set("user", params.User)
	}
	u.RawQuery = q.Encode()

	req := &fetcher.Request{
		URL:    u.String(),
		Method: "GET",
		Headers: map[string]string{
			"User-Agent": "PolyAsianData/1.0",
			"Accept":     "application/json",
		},
		Metadata: map[string]string{
			"IsLeaderboard": "true",
		},
	}

	return []*fetcher.Request{req}, nil
}

func (ply *PlyMktService) GetHoldersReqs(ctx context.Context, marketIDs []string) ([]*fetcher.Request, error) {
	dataEndpoint, ok := config.DefaultEndpoints["data"].(string)
	if !ok {
		if ply.Cfg != nil {
			if ep, ok := ply.Cfg.Services.PlyMkt.Endpoints["data"].(string); ok {
				dataEndpoint = ep
			}
		}
		if dataEndpoint == "" {
			dataEndpoint = "https://data-api.polymarket.com"
		}
	}

	u, err := url.Parse(dataEndpoint)
	if err != nil {
		return nil, err
	}
	u = u.JoinPath("/holders")

	// Limit is hardcapped at 20 per request/token? OR "limit=20" in query.
	// API docs say "market: comma-separated list of condition IDs".
	// Gamma market ID usually IS the condition ID or close mapping?
	// Note: API doc says "condition IDs". Our `PlyMktMarket.ConditionID`.

	// We'll chunk to be safe, e.g. 20 markets per request?
	// User didn't specify batch size, but usually 20-50 is safe for comma lists.
	// Let's use 20.

	var reqs []*fetcher.Request
	chunkSize := 20

	for i := 0; i < len(marketIDs); i += chunkSize {
		end := i + chunkSize
		if end > len(marketIDs) {
			end = len(marketIDs)
		}
		chunk := marketIDs[i:end]

		q := u.Query()
		q.Set("market", strings.Join(chunk, ","))
		q.Set("limit", "20") // Max per token

		// Copy URL
		reqURL := *u
		reqURL.RawQuery = q.Encode()

		reqs = append(reqs, &fetcher.Request{
			URL:    reqURL.String(),
			Method: "GET",
			Headers: map[string]string{
				"User-Agent": "PolyAsianData/1.0",
				"Accept":     "application/json",
			},
			Metadata: map[string]string{
				"IsHolders": "true",
			},
		})
	}

	return reqs, nil
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
	limit := 300
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
		URL:     fullURL.String(),
		Method:  "GET",
		Headers: map[string]string{"Content-Type": "application/json"},
	})

	return reqs, nil
}

func (ply *PlyMktService) GetSportsReqs(ctx context.Context) ([]*fetcher.Request, error) {
	gammaEndpoint, ok := config.DefaultEndpoints["gamma"].(string)
	if !ok {
		return nil, fmt.Errorf("gamma endpoint not configured")
	}
	baseUrl, err := url.Parse(gammaEndpoint)
	if err != nil {
		return nil, err
	}

	// Order matters: tags first (sports.primary_tag_id FK references tags), then sports, then teams
	targets := []struct {
		path  string
		limit int
	}{
		{path: "/tags", limit: 300},
		{path: "/sports", limit: 500},
		{path: "/teams", limit: 500},
	}

	var reqs []*fetcher.Request
	offset := 0
	for _, target := range targets {
		currQuery := url.Values{}
		currQuery.Add("limit", fmt.Sprintf("%d", target.limit))
		currQuery.Add("offset", fmt.Sprintf("%d", offset))
		fullURL := baseUrl.JoinPath(target.path)
		fullURL.RawQuery = currQuery.Encode()

		r := &fetcher.Request{
			URL:     fullURL.String(),
			Headers: map[string]string{"Content-Type": "application/json"},
			Method:  "GET",
			Params:  fullURL.Query(),
		}
		reqs = append(reqs, r)
		offset += target.limit
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
	"accounts": `accounts(first: $first,
		where: {
			id_gt: $lastId,
			scaledCollateralVolume_gte: $scaledCollateralVolumeGte
		},
		orderBy: scaledCollateralVolume,
		orderDirection: desc) {
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
		where: {id_gt: $lastId, user: $userId}
		orderBy: id
		orderDirection: desc
	) {
		id
		user
		tokenId
		amount
		avgPrice
		realizedPnl
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
	ID                  string   `json:"id"`
	Oracle              string   `json:"oracle"` // Bytes → string
	OutcomeSlotCount    int      `json:"outcomeSlotCount"`
	PayoutDenominator   string   `json:"payoutDenominator"`
	PayoutNumerators    []string `json:"payoutNumerators"`
	Payouts             []string `json:"payouts"`    // Final redemption multipliers
	QuestionId          string   `json:"questionId"` // Bytes → string
	ResolutionHash      string   `json:"resolutionHash"`
	ResolutionTimestamp string   `json:"resolutionTimestamp"`
}

type PlyMktAccount struct {
	ID                     string `json:"id"`
	CreationTimestamp      string `json:"creationTimestamp"`
	LastSeenTimestamp      string `json:"lastSeenTimestamp"`
	CollateralVolume       string `json:"collateralVolume"`
	NumTrades              string `json:"numTrades"`
	ScaledCollateralVolume string `json:"scaledCollateralVolume"`
	LastTradedTimestamp    string `json:"lastTradedTimestamp"`
	Profit                 string `json:"profit"`
	ScaledProfit           string `json:"scaledProfit"`
}

type PlyMktEnrichedOrderFilledEvent struct {
	ID    string   `json:"id"`    // Tx hash + order hash
	Price string   `json:"price"` // Fill price
	Side  string   `json:"side"`  // Maker or taker
	Size  string   `json:"size"`  // Fill size
	Maker struct { // added liquidity by posting order side = "buy" (they took liquidity)
		ID string `json:"id"`
	} `json:"maker"`
	Taker struct { // removed liquidity by taking order side = "sell" (they added liquidity)
		ID string `json:"id"`
	} `json:"taker"`
	Market struct {
		ID string `json:"id"`
	} `json:"market"`
	Timestamp       string `json:"timestamp"`
	TransactionHash string `json:"transactionHash"`
	OrderHash       string `json:"orderHash"`
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

var targets = map[string]struct {
	path   string
	entity string
}{
	"conditions": {
		path:   "/81Dm16JjuFSrqz813HysXoUPvzTwE7fsfPk2RTf66nyC", // Example path, or use ID from stub
		entity: "conditions",
	},
	"orderFilledEvents": {
		path:   "/81Dm16JjuFSrqz813HysXoUPvzTwE7fsfPk2RTf66nyC",
		entity: "orderFilledEvents",
	},
	"enrichedOrderFilleds": {
		path:   "/81Dm16JjuFSrqz813HysXoUPvzTwE7fsfPk2RTf66nyC",
		entity: "enrichedOrderFilleds",
	},
	"accounts": {
		path:   "/81Dm16JjuFSrqz813HysXoUPvzTwE7fsfPk2RTf66nyC",
		entity: "accounts",
	},
	"fpmms": {
		path:   "/6c58N5U4MtQE2Y8njfVrrAfRykzfqajMGeTMEvMmskVz",
		entity: "fpmms",
	},
	"userPositions": {
		path:   "/6c58N5U4MtQE2Y8njfVrrAfRykzfqajMGeTMEvMmskVz",
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
	q.Set("interval", "max") // Use 'max' to get all available history
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
		// Default vars stub
		vars := map[string]any{
			"first":  1000,
			"lastId": "",
		}
		// Need to construct URL stub using logic or just pass a placeholder if this function isn't used
		// It seems this function is a legacy backup? Let's fix signature anyway.
		// It was calling gqlRequestBuilder(target, host, path, entity) BEFORE I changed signature
		// to gqlRequestBuilder(key, fullURL, entityQuery, vars).

		host := ply.Cfg.Services.PlyMkt.Endpoints["subgraph"].(string)
		u, _ := url.Parse(host)
		u = u.JoinPath(targets[target].path)

		if req, err := ply.gqlRequestBuilder(target, u.String(), SubgraphEntities[target], vars); err != nil {
			return nil, err
		} else {
			reqs = append(reqs, req)
		}
	}

	return reqs, nil
}

// GetSubgraphReqsWithOpts generates subgraph requests with optional configuration.
// accountsMinVolume: if non-empty, used as scaledCollateralVolume_gte filter for accounts query.
func (ply *PlyMktService) GetSubgraphReqs(ctx context.Context, targetsList []string, startIds map[string]string) ([]*fetcher.Request, error) {
	return ply.GetSubgraphReqsWithOpts(ctx, targetsList, startIds, "")
}

// GetSubgraphReqsWithOpts generates subgraph requests with optional volume threshold.
func (ply *PlyMktService) GetSubgraphReqsWithOpts(ctx context.Context, targetsList []string, startIds map[string]string, accountsMinVolume string) ([]*fetcher.Request, error) {
	// Determine Host
	host := ply.Cfg.Services.PlyMkt.Endpoints["subgraph"].(string)
	useGateway := ply.Cfg.SubgraphAPIKey != ""

	var reqs []*fetcher.Request

	for _, target := range targetsList {
		// Construct URL
		path := targets[target].path
		var endpointURL string

		if useGateway && len(ply.Cfg.SubgraphAPIKey) > 0 {
			// Gateway Format: https://gateway.thegraph.com/api/[KEY]/subgraphs/id/[ID]
			// Path contains slash? remove strict leading slash if double
			cleanPath := strings.TrimPrefix(path, "/")
			endpointURL = fmt.Sprintf("https://gateway.thegraph.com/api/%s/subgraphs/id/%s", ply.Cfg.SubgraphAPIKey, cleanPath)
		} else {
			// Legacy / Hosted Service Format: [HOST]/[PATH]
			u, _ := url.Parse(host)
			u = u.JoinPath(path)
			endpointURL = u.String()
		}

		// Default variables
		vars := map[string]any{
			"first":  1000,
			"lastId": "",
		}

		// Add filter for accounts - use provided threshold or default to $100
		if target == "accounts" {
			if accountsMinVolume != "" {
				vars["scaledCollateralVolumeGte"] = accountsMinVolume
			} else {
				vars["scaledCollateralVolumeGte"] = "100" // Default $100 minimum
			}
		}

		// Check for resume ID
		if startIds != nil {
			if sid, ok := startIds[target]; ok && sid != "" {
				vars["lastId"] = sid
				ply.Logger.Info("resuming sync", slog.String("target", target), slog.String("start_id", sid))
			}
		}

		if req, err := ply.gqlRequestBuilder(target, endpointURL, SubgraphEntities[target], vars); err != nil {
			return nil, err
		} else {
			// Cap accounts sync to 10 pages (10,000 accounts)
			if target == "accounts" {
				req.Metadata["MaxPages"] = "20"
				req.Metadata["CurrentPage"] = "1"
			}
			reqs = append(reqs, req)
		}
	}

	return reqs, nil
}

// GetTop500AccountsReq returns a request to fetch the top 500 accounts by scaledCollateralVolume.
// Use the response to calculate the average volume as a threshold for subsequent queries.
func (ply *PlyMktService) GetTop500AccountsReq(ctx context.Context) (*fetcher.Request, error) {
	host := ply.Cfg.Services.PlyMkt.Endpoints["subgraph"].(string)
	useGateway := ply.Cfg.SubgraphAPIKey != ""
	path := targets["accounts"].path
	var endpointURL string

	if useGateway && len(ply.Cfg.SubgraphAPIKey) > 0 {
		cleanPath := strings.TrimPrefix(path, "/")
		endpointURL = fmt.Sprintf("https://gateway.thegraph.com/api/%s/subgraphs/id/%s", ply.Cfg.SubgraphAPIKey, cleanPath)
	} else {
		u, _ := url.Parse(host)
		u = u.JoinPath(path)
		endpointURL = u.String()
	}

	// Query for top 100 accounts without any volume filter
	query := `accounts(first: 500, orderBy: scaledCollateralVolume, orderDirection: desc) {
        id
        scaledCollateralVolume
    }`

	vars := map[string]any{}

	fullQuery := fmt.Sprintf(`query Top500Accounts {
        %s
    }`, query)

	return ply.buildGQLRequest(endpointURL, "top500accounts", fullQuery, vars, false)
}

// CalculateAvgVolume calculates the average scaledCollateralVolume from a list of accounts.
// Returns the average as a string suitable for use in subsequent queries.
func CalculateAvgVolume(accounts []PlyMktAccount) string {
	if len(accounts) == 0 {
		return "100" // Fallback default
	}

	var total float64
	for _, acc := range accounts {
		// Parse the scaledCollateralVolume string to float
		if vol, err := parseScaledVolume(acc.ScaledCollateralVolume); err == nil {
			total += vol
		}
	}

	avg := total / float64(len(accounts))
	// Return as integer string (truncated)
	return fmt.Sprintf("%.0f", avg)
}

func parseScaledVolume(s string) (float64, error) {
	var val float64
	_, err := fmt.Sscanf(s, "%f", &val)
	return val, err
}

// GetWhalePositionsReqs generates requests for specific user IDs.
func (ply *PlyMktService) GetWhalePositionsReqs(ctx context.Context, userIDs []string) ([]*fetcher.Request, error) {
	if len(userIDs) == 0 {
		return nil, nil
	}

	// Determine URL
	host := ply.Cfg.Services.PlyMkt.Endpoints["subgraph"].(string)
	useGateway := ply.Cfg.SubgraphAPIKey != ""
	path := targets["userPositions"].path
	var endpointURL string
	if useGateway && len(ply.Cfg.SubgraphAPIKey) > 0 {
		cleanPath := strings.TrimPrefix(path, "/")
		endpointURL = fmt.Sprintf("https://gateway.thegraph.com/api/%s/subgraphs/id/%s", ply.Cfg.SubgraphAPIKey, cleanPath)
	} else {
		u, _ := url.Parse(host)
		u = u.JoinPath(path)
		endpointURL = u.String()
	}

	query := `userPositions(
		first: $first
		where: { id_gt: $lastId, user_in: $userIds }
		orderBy: id
		orderDirection: asc
		subgraphError: allow
	) {
		id
		user
		tokenId
		amount
		avgPrice
		realizedPnl
		totalBought
	}`

	// Variables
	vars := map[string]any{
		"first":   1000,
		"lastId":  "",
		"userIds": userIDs,
	}

	fullQuery := fmt.Sprintf(`query MyQuery($first: Int, $lastId: String, $userIds: [String!]) {
		%s
	}`, query)

	req, err := ply.buildGQLRequest(endpointURL, "userPositions", fullQuery, vars, true)
	if err != nil {
		return nil, err
	}

	// Tag as filtered so processor knows to maintain filters on next page
	req.Metadata["UserFiltered"] = "true"
	// Store user IDs in metadata? Too large.
	// We handle pagination logic in Processor by checking if "variables" has userIds.
	// But `fetcher.Request` Body is a Reader. We can't easily read it back without parsing.
	// We might need to store the user IDs in metadata as JSON string if < 4KB?
	// 100 addresses * 42 bytes = 4200 bytes. Borderline.
	// Better: The processor, when seeing "UserFiltered", should look at the *Response*'s request body?
	// Or we just rely on infinite scroll logic which grabs the same query/vars from the previous request body JSON?
	// The generic pagination logic in `processor.go` uses `resp.Request` but might not preserve complex variables if they aren't in Metadata.
	// WAIT. `processor.go` pagination logic:
	// It parses `limit` and `offset` from URL QUERY.
	// BUT for Subgraph, it uses `id_gt` in BODY.
	// See `processSubgraph` in `processor.go`:
	/*
		// Build Next Request if Cursor Pagination is enabled ...
		fullQuery := resp.Request.Metadata["GraphqlQuery"]
		bodyData := map[string]any{
			"query": fullQuery,
			"variables": map[string]any{
				"first": 1000,
				"lastId": lastID,
			},
		}
	*/
	// IT HARDCODES VARIABLES!
	// It obliterates `$userIds`.

	// I need to fix `processor.go` to preserve existing variables.
	// But `fetcher.Request` doesn't expose them struct-wise.
	// I should pass json serialized variables in Metadata for easier restoration?
	// OR use `json.Unmarshal` on `OriginalRequest.Body` (we need a copy of body).
	// `fetcher.Response.Request.Body` is likely exhausted/closed.
	// We have `OriginalRequest` in `Output`. Processor receives `resp`. `resp.Request` is the original *Request object*.

	// `fetcher.go` might not copy the body for us into the Response object.
	// But wait, `processor.go` Re-Marshals `bodyData`.

	// FIX: I will pass the Variables map as a JSON string in Metadata `GraphqlVariables`.

	varsBytes, _ := json.Marshal(vars)
	req.Metadata["GraphqlVariables"] = string(varsBytes)

	return []*fetcher.Request{req}, nil
}

func (ply *PlyMktService) gqlRequestBuilder(key, fullURL, entityQuery string, vars map[string]any) (*fetcher.Request, error) {
	// Build variable declarations dynamically based on vars map
	varDecls := []string{}
	for varName, varValue := range vars {
		var gqlType string
		switch varValue.(type) {
		case int, int64, int32:
			gqlType = "Int"
		case string:
			gqlType = "String"
		case []string:
			gqlType = "[String!]"
		case bool:
			gqlType = "Boolean"
		case float64, float32:
			gqlType = "Float"
		default:
			gqlType = "String" // Default fallback
		}
		varDecls = append(varDecls, fmt.Sprintf("$%s: %s", varName, gqlType))
	}

	// Sort for consistent ordering
	sort.Strings(varDecls)
	varDeclStr := strings.Join(varDecls, ", ")

	// Wrapper for query
	fullQuery := fmt.Sprintf(`query MyQuery(%s) {
		%s
	}`, varDeclStr, entityQuery)

	return ply.buildGQLRequest(fullURL, key, fullQuery, vars, true)
}

func (ply *PlyMktService) buildGQLRequest(fullURL, key, query string, vars map[string]any, cursorPagination bool) (*fetcher.Request, error) {
	bodyData := map[string]any{
		"query":     query,
		"variables": vars,
	}

	bodyBytes, err := json.Marshal(bodyData)
	if err != nil {
		ply.Logger.Error("failed to marshal body", slog.String("entity", key))
		return nil, err
	}

	cursorStr := "false"
	if cursorPagination {
		cursorStr = "true"
	}

	// Serialize variables to Metadata for pagination persistence
	varsBytes, _ := json.Marshal(vars)

	return &fetcher.Request{
		URL:    fullURL,
		Method: "POST",
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Body: bytes.NewReader(bodyBytes),
		Metadata: map[string]string{
			"Type":             "subgraph",
			"Entity":           key,
			"GraphqlQuery":     query,
			"GraphqlVariables": string(varsBytes),
			"CursorPagination": cursorStr,
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

//	{
//	 redemptions {
//	   id
//	   condition
//	   indexSets
//	   payout
//	   redeemer
//	   timestamp
//	 }
//	}
//
// {
//
//	 splits {
//	   id
//	   timestamp
//	   stakeholder
//	   condition
//	   amount
//	 }
//	}	// GetWhaleFillsReqs generates requests for trade history of specific user IDs.
//
// It fetches EnrichedOrderFilledEvents where the user is either the Maker or the Taker.
func (ply *PlyMktService) GetWhaleFillsReqs(ctx context.Context, userIDs []string) ([]*fetcher.Request, error) {
	if len(userIDs) == 0 {
		return nil, nil
	}

	// Determine URL
	host := ply.Cfg.Services.PlyMkt.Endpoints["subgraph"].(string)
	useGateway := ply.Cfg.SubgraphAPIKey != ""
	path := targets["enrichedOrderFilleds"].path
	var endpointURL string
	if useGateway && len(ply.Cfg.SubgraphAPIKey) > 0 {
		cleanPath := strings.TrimPrefix(path, "/")
		endpointURL = fmt.Sprintf("https://gateway.thegraph.com/api/%s/subgraphs/id/%s", ply.Cfg.SubgraphAPIKey, cleanPath)
	} else {
		u, _ := url.Parse(host)
		u = u.JoinPath(path)
		endpointURL = u.String()
	}

	// Query Template
	// Note: We use orderBy: id for stable pagination.
	// We already have "enrichedOrderFilleds" in SubgraphEntities but that's a generic one.
	// We construct filtering queries manually here.

	queryTemplate := `enrichedOrderFilleds(
        first: $first
        where: { id_gt: $lastId, %s: $userIds }
        orderBy: id
        orderDirection: asc
    ) {
        id
        timestamp
        maker { id }
        taker { id }
        size
        side
        price
        market { id }
    }`

	vars := map[string]any{
		"first":   500,
		"lastId":  "",
		"userIds": userIDs,
	}

	var reqs []*fetcher.Request

	// Request 1: As Maker
	queryMaker := fmt.Sprintf(queryTemplate, "maker_in")
	fullQueryMaker := fmt.Sprintf(`query WhalesAsMakers($first: Int, $lastId: String, $userIds: [String!]) {
        %s
    }`, queryMaker)

	reqMaker, err := ply.buildGQLRequest(endpointURL, "enrichedOrderFilleds", fullQueryMaker, vars, true)
	if err != nil {
		return nil, err
	}
	// Tag for processor
	reqMaker.Metadata["UserFiltered"] = "true"
	reqs = append(reqs, reqMaker)

	// Request 2: As Taker
	queryTaker := fmt.Sprintf(queryTemplate, "taker_in")
	fullQueryTaker := fmt.Sprintf(`query WhalesAsTakers($first: Int, $lastId: String, $userIds: [String!]) {
        %s
    }`, queryTaker)

	// Important: We need a fresh map for vars to avoid reference issues if buildGQL modifies it (it shouldn't, but safer)
	varsTaker := map[string]any{
		"first":   500,
		"lastId":  "",
		"userIds": userIDs,
	}

	reqTaker, err := ply.buildGQLRequest(endpointURL, "enrichedOrderFilleds", fullQueryTaker, varsTaker, true)
	if err != nil {
		return nil, err
	}
	reqTaker.Metadata["UserFiltered"] = "true"
	reqs = append(reqs, reqTaker)

	return reqs, nil
}

// GetMarketFillsReqs builds targeted requests for enriched order fills for specific markets.
func (ply *PlyMktService) GetMarketFillsReqs(ctx context.Context, marketIDs []string) ([]*fetcher.Request, error) {
	if len(marketIDs) == 0 {
		return nil, nil
	}

	host := ply.Cfg.Services.PlyMkt.Endpoints["subgraph"].(string)
	useGateway := ply.Cfg.SubgraphAPIKey != ""
	path := targets["enrichedOrderFilleds"].path
	var endpointURL string

	if useGateway && len(ply.Cfg.SubgraphAPIKey) > 0 {
		cleanPath := strings.TrimPrefix(path, "/")
		endpointURL = fmt.Sprintf("https://gateway.thegraph.com/api/%s/subgraphs/id/%s", ply.Cfg.SubgraphAPIKey, cleanPath)
	} else {
		u, _ := url.Parse(host)
		u = u.JoinPath(path)
		endpointURL = u.String()
	}

	// Batch markets into groups of 50
	batchSize := 50
	var reqs []*fetcher.Request

	for i := 0; i < len(marketIDs); i += batchSize {
		end := i + batchSize
		if end > len(marketIDs) {
			end = len(marketIDs)
		}
		chunk := marketIDs[i:end]

		queryContent := BuildEnrichedOrderFilledsQuery(chunk)

		fullQuery := fmt.Sprintf(`query ActiveMarketFills($first: Int, $lastId: String, $marketIds: [String!]) {
            %s
        }`, queryContent)

		vars := map[string]any{
			"first":     500,
			"lastId":    "",
			"marketIds": chunk,
		}

		req, err := ply.buildGQLRequest(endpointURL, "enrichedOrderFilleds", fullQuery, vars, true)
		if err != nil {
			return nil, err
		}
		reqs = append(reqs, req)
	}

	return reqs, nil
}

type PlyMktOrderbookSnapshot struct {
	Time          time.Time `json:"-" db:"time"`
	MarketID      string    `json:"-" db:"market_id"`
	TokenID       string    `json:"-" db:"token_id"`
	BestBid       float64   `json:"-" db:"best_bid"`
	BestAsk       float64   `json:"-" db:"best_ask"`
	Imbalance     float64   `json:"-" db:"imbalance"`
	TotalBidDepth float64   `json:"-" db:"total_bid_depth"`
	TotalAskDepth float64   `json:"-" db:"total_ask_depth"`
	DepthJSON     []byte    `json:"-" db:"depth_json"`
	RawJSON       []byte    `json:"-" db:"raw_response_json"`
	NegRisk       bool      `json:"-" db:"neg_risk"`
	Timestamp     string    `json:"-" db:"timestamp"`
}

type PlyMktMarketOI struct {
	Market string  `json:"market"`
	Value  float64 `json:"value"`
}

