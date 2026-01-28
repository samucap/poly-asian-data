package services

import (
	"context"
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

// orderFilledEvents.maker and taker are addresses of users who made(added liquidity) and took (removed liquidity) from the market
type PlyMktMarket struct {
	ID                           string          `json:"id"`
	Question                     string          `json:"question"`
	ConditionId                  string          `json:"conditionId"`
	Slug                         string          `json:"slug"`
	TwitterCardImage             string          `json:"twitterCardImage"`
	ResolutionSource             string          `json:"resolutionSource"`
	EndDate                      time.Time       `json:"endDate"`
	Category                     string          `json:"category"`
	AmmType                      string          `json:"ammType"`
	Liquidity                    string          `json:"liquidity"`
	SponsorName                  string          `json:"sponsorName"`
	SponsorImage                 string          `json:"sponsorImage"`
	StartDate                    time.Time       `json:"startDate"`
	XAxisValue                   string          `json:"xAxisValue"`
	YAxisValue                   string          `json:"yAxisValue"`
	DenominationToken            string          `json:"denominationToken"`
	Fee                          string          `json:"fee"`
	Image                        string          `json:"image"`
	Icon                         string          `json:"icon"`
	LowerBound                   string          `json:"lowerBound"`
	UpperBound                   string          `json:"upperBound"`
	Description                  string          `json:"description"`
	Outcomes                     string          `json:"outcomes"`
	OutcomePrices                string          `json:"outcomePrices"`
	Volume                       string          `json:"volume"`
	Active                       bool            `json:"active"`
	MarketType                   string          `json:"marketType"`
	FormatType                   string          `json:"formatType"`
	LowerBoundDate               string          `json:"lowerBoundDate"`
	UpperBoundDate               string          `json:"upperBoundDate"`
	Closed                       bool            `json:"closed"`
	MarketMakerAddress           string          `json:"marketMakerAddress"`
	CreatedBy                    int             `json:"createdBy"`
	UpdatedBy                    int             `json:"updatedBy"`
	CreatedAt                    time.Time       `json:"createdAt"`
	UpdatedAt                    time.Time       `json:"updatedAt"`
	ClosedTime                   string          `json:"closedTime"`
	WideFormat                   bool            `json:"wideFormat"`
	New                          bool            `json:"new"`
	MailchimpTag                 string          `json:"mailchimpTag"`
	Featured                     bool            `json:"featured"`
	Archived                     bool            `json:"archived"`
	ResolvedBy                   string          `json:"resolvedBy"`
	Restricted                   bool            `json:"restricted"`
	MarketGroup                  int             `json:"marketGroup"`
	GroupItemTitle               string          `json:"groupItemTitle"`
	GroupItemThreshold           string          `json:"groupItemThreshold"`
	QuestionID                   string          `json:"questionID"`
	UmaEndDate                   string          `json:"umaEndDate"`
	EnableOrderBook              bool            `json:"enableOrderBook"`
	OrderPriceMinTickSize        float64         `json:"orderPriceMinTickSize"`
	OrderMinSize                 float64         `json:"orderMinSize"`
	UmaResolutionStatus          string          `json:"umaResolutionStatus"`
	CurationOrder                int             `json:"curationOrder"`
	VolumeNum                    float64         `json:"volumeNum"`
	LiquidityNum                 float64         `json:"liquidityNum"`
	EndDateIso                   string          `json:"endDateIso"`
	StartDateIso                 string          `json:"startDateIso"`
	UmaEndDateIso                string          `json:"umaEndDateIso"`
	HasReviewedDates             bool            `json:"hasReviewedDates"`
	ReadyForCron                 bool            `json:"readyForCron"`
	CommentsEnabled              bool            `json:"commentsEnabled"`
	Volume24hr                   float64         `json:"volume24hr"`
	Volume1wk                    float64         `json:"volume1wk"`
	Volume1mo                    float64         `json:"volume1mo"`
	Volume1yr                    float64         `json:"volume1yr"`
	GameStartTime                string          `json:"gameStartTime"`
	SecondsDelay                 int             `json:"secondsDelay"`
	ClobTokenIds                 string          `json:"clobTokenIds"`
	DisqusThread                 string          `json:"disqusThread"`
	ShortOutcomes                string          `json:"shortOutcomes"`
	TeamAID                      string          `json:"teamAID"`
	TeamBID                      string          `json:"teamBID"`
	UmaBond                      string          `json:"umaBond"`
	UmaReward                    string          `json:"umaReward"`
	FpmmLive                     bool            `json:"fpmmLive"`
	Volume24hrAmm                float64         `json:"volume24hrAmm"`
	Volume1wkAmm                 float64         `json:"volume1wkAmm"`
	Volume1moAmm                 float64         `json:"volume1moAmm"`
	Volume1yrAmm                 float64         `json:"volume1yrAmm"`
	Volume24hrClob               float64         `json:"volume24hrClob"`
	Volume1wkClob                float64         `json:"volume1wkClob"`
	Volume1moClob                float64         `json:"volume1moClob"`
	Volume1yrClob                float64         `json:"volume1yrClob"`
	VolumeAmm                    float64         `json:"volumeAmm"`
	VolumeClob                   float64         `json:"volumeClob"`
	LiquidityAmm                 float64         `json:"liquidityAmm"`
	LiquidityClob                float64         `json:"liquidityClob"`
	MakerBaseFee                 float64         `json:"makerBaseFee"`
	TakerBaseFee                 float64         `json:"takerBaseFee"`
	CustomLiveness               int             `json:"customLiveness"`
	AcceptingOrders              bool            `json:"acceptingOrders"`
	NotificationsEnabled         bool            `json:"notificationsEnabled"`
	Score                        float64         `json:"score"`
	ImageOptimized               *ImageOptimized `json:"imageOptimized"`
	IconOptimized                *ImageOptimized `json:"iconOptimized"`
	Events                       []PlyMktEvent   `json:"events"`
	Categories                   []Category      `json:"categories"`
	Tags                         []PlyMktTag     `json:"tags"`
	Creator                      string          `json:"creator"`
	Ready                        bool            `json:"ready"`
	Funded                       bool            `json:"funded"`
	PastSlugs                    string          `json:"pastSlugs"`
	ReadyTimestamp               time.Time       `json:"readyTimestamp"`
	FundedTimestamp              time.Time       `json:"fundedTimestamp"`
	AcceptingOrdersTimestamp     time.Time       `json:"acceptingOrdersTimestamp"`
	Competitive                  float64         `json:"competitive"`
	RewardsMinSize               float64         `json:"rewardsMinSize"`
	RewardsMaxSpread             float64         `json:"rewardsMaxSpread"`
	Spread                       float64         `json:"spread"`
	AutomaticallyResolved        bool            `json:"automaticallyResolved"`
	OneDayPriceChange            float64         `json:"oneDayPriceChange"`
	OneHourPriceChange           float64         `json:"oneHourPriceChange"`
	OneWeekPriceChange           float64         `json:"oneWeekPriceChange"`
	OneMonthPriceChange          float64         `json:"oneMonthPriceChange"`
	OneYearPriceChange           float64         `json:"oneYearPriceChange"`
	LastTradePrice               float64         `json:"lastTradePrice"`
	BestBid                      float64         `json:"bestBid"`
	BestAsk                      float64         `json:"bestAsk"`
	AutomaticallyActive          bool            `json:"automaticallyActive"`
	ClearBookOnStart             bool            `json:"clearBookOnStart"`
	ChartColor                   string          `json:"chartColor"`
	SeriesColor                  string          `json:"seriesColor"`
	ShowGmpSeries                bool            `json:"showGmpSeries"`
	ShowGmpOutcome               bool            `json:"showGmpOutcome"`
	ManualActivation             bool            `json:"manualActivation"`
	NegRiskOther                 bool            `json:"negRiskOther"`
	GameId                       string          `json:"gameId"`
	GroupItemRange               string          `json:"groupItemRange"`
	SportsMarketType             string          `json:"sportsMarketType"`
	Line                         float64         `json:"line"`
	UmaResolutionStatuses        string          `json:"umaResolutionStatuses"`
	PendingDeployment            bool            `json:"pendingDeployment"`
	Deploying                    bool            `json:"deploying"`
	DeployingTimestamp           time.Time       `json:"deployingTimestamp"`
	ScheduledDeploymentTimestamp time.Time       `json:"scheduledDeploymentTimestamp"`
	RfqEnabled                   bool            `json:"rfqEnabled"`
	EventStartTime               time.Time       `json:"eventStartTime"`
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

type Series struct {
	ID                string       `json:"id"`
	Ticker            string       `json:"ticker"`
	Slug              string       `json:"slug"`
	Title             string       `json:"title"`
	Subtitle          string       `json:"subtitle"`
	SeriesType        string       `json:"seriesType"`
	Recurrence        string       `json:"recurrence"`
	Description       string       `json:"description"`
	Image             string       `json:"image"`
	Icon              string       `json:"icon"`
	Layout            string       `json:"layout"`
	Active            bool         `json:"active"`
	Closed            bool         `json:"closed"`
	Archived          bool         `json:"archived"`
	New               bool         `json:"new"`
	Featured          bool         `json:"featured"`
	Restricted        bool         `json:"restricted"`
	IsTemplate        bool         `json:"isTemplate"`
	TemplateVariables bool         `json:"templateVariables"`
	PublishedAt       string       `json:"publishedAt"`
	CreatedBy         string       `json:"createdBy"`
	UpdatedBy         string       `json:"updatedBy"`
	CreatedAt         time.Time    `json:"createdAt"`
	UpdatedAt         time.Time    `json:"updatedAt"`
	CommentsEnabled   bool         `json:"commentsEnabled"`
	Competitive       string       `json:"competitive"`
	Volume24hr        float64      `json:"volume24hr"`
	Volume            float64      `json:"volume"`
	Liquidity         float64      `json:"liquidity"`
	StartDate         time.Time    `json:"startDate"`
	PythTokenID       string       `json:"pythTokenID"`
	CgAssetName       string       `json:"cgAssetName"`
	Score             float64      `json:"score"`
	Events            []PlyMktEvent `json:"events"`
	Collections       []Collection `json:"collections"`
	Categories        []Category   `json:"categories"`
	Tags              []PlyMktTag `json:"tags"`
	CommentCount      int          `json:"commentCount"`
	Chats             []Chat       `json:"chats"`
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
	Series                       []*Series            `json:"series"`
	Categories                   []*Category          `json:"categories"`
	Collections                  []*Collection        `json:"collections"`
	Tags                         []*PlyMktTag         `json:"tags"`
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
	EventCreators                []*EventCreator      `json:"eventCreators"`
	TweetCount                   int                 `json:"tweetCount"`
	Chats                        []*Chat              `json:"chats"`
	FeaturedOrder                int                 `json:"featuredOrder"`
	EstimateValue                bool                `json:"estimateValue"`
	CantEstimate                 bool                `json:"cantEstimate"`
	EstimatedValue               string              `json:"estimatedValue"`
	Templates                    []*Template          `json:"templates"`
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

type Collection struct {
	ID                     string          `json:"id"`
	Ticker                 string          `json:"ticker"`
	Slug                   string          `json:"slug"`
	Title                  string          `json:"title"`
	Subtitle               string          `json:"subtitle"`
	CollectionType         string          `json:"collectionType"`
	Description            string          `json:"description"`
	Tags                   string          `json:"tags"`
	Image                  string          `json:"image"`
	Icon                   string          `json:"icon"`
	HeaderImage            string          `json:"headerImage"`
	Layout                 string          `json:"layout"`
	Active                 bool            `json:"active"`
	Closed                 bool            `json:"closed"`
	Archived               bool            `json:"archived"`
	New                    bool            `json:"new"`
	Featured               bool            `json:"featured"`
	Restricted             bool            `json:"restricted"`
	IsTemplate             bool            `json:"isTemplate"`
	TemplateVariables      string          `json:"templateVariables"`
	PublishedAt            string          `json:"publishedAt"`
	CreatedBy              string          `json:"createdBy"`
	UpdatedBy              string          `json:"updatedBy"`
	CreatedAt              time.Time       `json:"createdAt"`
	UpdatedAt              time.Time       `json:"updatedAt"`
	CommentsEnabled        bool            `json:"commentsEnabled"`
	ImageOptimized         *ImageOptimized `json:"imageOptimized"`
	IconOptimized          *ImageOptimized `json:"iconOptimized"`
	HeaderImageOptimized   *ImageOptimized `json:"headerImageOptimized"`
}

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

type sportsTarget struct {
	path   string
	params map[string]string
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
	targets := map[string]sportsTarget{
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

//{
//  ordersMatchedEvents(first: 10, skip: 10, orderBy: id, orderDirection: asc) {
//    id
//    makerAmountFilled
//    makerAssetID
//    takerAmountFilled
//    takerAssetID
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