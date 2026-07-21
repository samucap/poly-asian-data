package config

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

// DefaultEndpoints holds default API URLs.
var DefaultEndpoints = map[string]any{
	"gamma":    "https://gamma-api.polymarket.com",
	"clob":     "https://clob.polymarket.com",
	"data_api": "https://data-api.polymarket.com",
	"subgraph": "https://api.thegraph.com/subgraphs", // Default placeholder
}

// Config holds the application configuration.
type Config struct {
	ENV            string
	LogLevel       string
	PostgresURL    string
	SubgraphAPIKey string
	Services       ServicesConfig

	// Worker Pool Configs
	FetcherCfg   WorkerPoolConfig
	ProcessorCfg WorkerPoolConfig
	SaverCfg     WorkerPoolConfig

	// TopMarketsCfg drives cmd/top-markets refresh filters and interval.
	TopMarkets TopMarketsConfig
	// Catalog drives cmd/catalog-markets (full open-universe sync, no enrichment).
	Catalog CatalogConfig
	// EdgeScan drives cmd/edge-scan (filtered candidates → board).
	EdgeScan EdgeScanConfig
}

type WorkerPoolConfig struct {
	NumWorkers int
	Qsize      int
}

// CatalogConfig holds refresh cadence and pagination for cmd/catalog-markets.
// Full open-universe scan: no rank filters and no OI/trades/prices enrichment.
type CatalogConfig struct {
	// RefreshInterval is time between catalog cycle starts (default 10m).
	// Cycle duration is typically ~1–2m; interval is not cycle length.
	RefreshInterval time.Duration
	// PaginateDelay is the wait between paginated HTTP pages (keyset / offset).
	PaginateDelay time.Duration
}

// EdgeScanConfig holds cadence, keyset filters, Stage-1 thresholds, and board size for cmd/edge-scan.
type EdgeScanConfig struct {
	RefreshInterval time.Duration
	PaginateDelay   time.Duration
	// Strategy name for edge_board PK partition (default "default").
	Strategy string
	// Keyset server-side filters (Gamma event-level).
	KeysetVolumeMin    float64
	KeysetLiquidityMin float64
	EndDateMinOffset   time.Duration // end_date_min = now - offset; 0 omits
	EndDateMaxOffset   time.Duration // end_date_max = now + offset; 0 omits
	// KeysetEventCap stops pagination after N events (0 = unlimited). Default 3000.
	KeysetEventCap int
	// Stage-1 in-process filters (market-level).
	MinVolume24hr float64
	MinLiquidity  float64
	MaxSpread     float64
	MinVolatility float64 // 0 = do not require |Δp|
	// Stage1MaxN is max markets after Stage-1 score (enrichment budget). Default 200.
	Stage1MaxN int
	// BoardMaxN is final edge board size. Default 50 (plan: 30–80).
	BoardMaxN int
	// Sticky keeps prior board members that still pass Stage-1 when possible.
	Sticky bool
}

// TopMarketsConfig holds market-ranking filter thresholds, keyset scan filters,
// enrichment tuning, and refresh cadence for cmd/top-markets.
type TopMarketsConfig struct {
	RefreshInterval time.Duration
	// Rank filters (applied in-process after the scan).
	MinVolume24hr float64
	MinLiquidity  float64
	MaxSpread     float64
	MinVolatility float64
	MaxN          int
	// Keyset server-side filters (Gamma /events/keyset query params).
	// volume_min is Gamma event volume (not necessarily 24h).
	KeysetVolumeMin    float64
	KeysetLiquidityMin float64
	// EndDateMinOffset: if > 0, set end_date_min = now - offset. Zero omits the param.
	EndDateMinOffset time.Duration
	// EndDateMaxOffset: if > 0, set end_date_max = now + offset. Zero omits the param.
	EndDateMaxOffset time.Duration
	// Enrichment (ranked markets only).
	PriceLookback   time.Duration // first-cycle price history window (default 30d for backtests)
	PriceFidelity   int           // minutes; higher = fewer points
	PriceBatchSize  int
	TradesBatchSize int
	// PaginateDelay is the wait between paginated HTTP pages (keyset / offset). Default 0.
	PaginateDelay time.Duration
}

type ServicesConfig struct {
	PlyMkt PlyMktConfig
}

type PlyMktConfig struct {
	Endpoints map[string]any
}

// Load reads configuration from environment variables.
func Load() (*Config, error) {
	// Load .env file if it exists, but don't fail if it doesn't
	_ = godotenv.Load()

	// Dynamic defaults
	defaultWorkers := runtime.NumCPU() * 2 // Aggressive default
	if defaultWorkers < 4 {
		defaultWorkers = 4
	}

	cfg := &Config{
		ENV:            getEnv("ENV", "dev"),
		LogLevel:       getEnv("LOG_LEVEL", "info"),
		PostgresURL:    os.Getenv("POSTGRES_URL"),
		SubgraphAPIKey: os.Getenv("SUBGRAPH_API_KEY"),
		FetcherCfg: WorkerPoolConfig{
			NumWorkers: getEnvInt("FETCHER_WORKERS", defaultWorkers),
			Qsize:      getEnvInt("FETCHER_QUEUE_SIZE", 100),
		},
		ProcessorCfg: WorkerPoolConfig{
			NumWorkers: getEnvInt("PROCESSOR_WORKERS", defaultWorkers),
			Qsize:      getEnvInt("PROCESSOR_QUEUE_SIZE", 100),
		},
		SaverCfg: WorkerPoolConfig{
			NumWorkers: getEnvInt("SAVER_WORKERS", defaultWorkers/2+1), // Safer writes
			Qsize:      getEnvInt("SAVER_QUEUE_SIZE", 200),
		},
		TopMarkets: loadTopMarketsConfig(),
		Catalog:    loadCatalogConfig(),
		EdgeScan:   loadEdgeScanConfig(),
	}

	// Compose PostgresURL if not set
	if cfg.PostgresURL == "" {
		user := getEnv("POSTGRES_USER", "postgres")
		pass := getEnv("POSTGRES_PASSWORD", "postgres")
		host := getEnv("POSTGRES_HOST", "localhost")
		port := getEnv("POSTGRES_PORT", "5432")
		db := getEnv("POSTGRES_DB", "postgres") // Default db name often matches user or 'postgres'

		cfg.PostgresURL = fmt.Sprintf("postgres://%s:%s@%s:%s/%s", user, pass, host, port, db)
	}

	// Validate required fields
	if cfg.PostgresURL == "" {
		return nil, &ValidationError{Field: "PostgresURL", Message: "POSTGRES_URL is required or component vars"}
	}

	// Validate ENV
	if cfg.ENV != "dev" && cfg.ENV != "prod" && cfg.ENV != "test" {
		return nil, &ValidationError{Field: "ENV", Message: "ENV must be dev, prod, or test"}
	}

	// Initialize Services Config with defaults
	cfg.Services.PlyMkt.Endpoints = make(map[string]any)
	for k, v := range DefaultEndpoints {
		cfg.Services.PlyMkt.Endpoints[k] = v
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	if c.PostgresURL == "" {
		return &ValidationError{Field: "PostgresURL", Message: "POSTGRES_URL is required"}
	}
	return nil
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val, ok := os.LookupEnv(key); ok {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

func getEnvFloat(key string, defaultVal float64) float64 {
	if val, ok := os.LookupEnv(key); ok {
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return defaultVal
}

func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	if val, ok := os.LookupEnv(key); ok {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return defaultVal
}

func loadCatalogConfig() CatalogConfig {
	return CatalogConfig{
		RefreshInterval: getEnvDuration("CATALOG_REFRESH_INTERVAL", 10*time.Minute),
		// Fall back to PAGINATE_DELAY when CATALOG_PAGINATE_DELAY unset (shared throttle).
		PaginateDelay: getEnvDuration("CATALOG_PAGINATE_DELAY", getEnvDuration("PAGINATE_DELAY", 0)),
	}
}

func loadEdgeScanConfig() EdgeScanConfig {
	minVol := getEnvFloat("EDGE_MIN_VOLUME", getEnvFloat("MIN_VOLUME", 30000.0))
	minLiq := getEnvFloat("EDGE_MIN_LIQUIDITY", getEnvFloat("MIN_LIQUIDITY", 15000.0))
	return EdgeScanConfig{
		RefreshInterval:    getEnvDuration("EDGE_REFRESH_INTERVAL", 2*time.Minute),
		PaginateDelay:      getEnvDuration("EDGE_PAGINATE_DELAY", getEnvDuration("PAGINATE_DELAY", 0)),
		Strategy:           getEnv("EDGE_STRATEGY", "default"),
		KeysetVolumeMin:    getEnvFloat("EDGE_KEYSET_VOLUME_MIN", minVol),
		KeysetLiquidityMin: getEnvFloat("EDGE_KEYSET_LIQUIDITY_MIN", minLiq),
		EndDateMinOffset:   getEnvDuration("EDGE_END_DATE_MIN_OFFSET", 0),
		EndDateMaxOffset:   getEnvDuration("EDGE_END_DATE_MAX_OFFSET", 0),
		KeysetEventCap:     getEnvInt("EDGE_KEYSET_EVENT_CAP", 3000),
		MinVolume24hr:      minVol,
		MinLiquidity:       minLiq,
		MaxSpread:          getEnvFloat("EDGE_MAX_SPREAD", getEnvFloat("MAX_SPREAD", 0.05)),
		// Default 0: do not require prior 1d move for board membership (M2).
		MinVolatility: getEnvFloat("EDGE_MIN_VOLATILITY", 0),
		Stage1MaxN:    getEnvInt("EDGE_STAGE1_MAX_N", 200),
		BoardMaxN:     getEnvInt("EDGE_BOARD_MAX_N", 50),
		Sticky:        getEnv("EDGE_STICKY", "true") != "false",
	}
}

func loadTopMarketsConfig() TopMarketsConfig {
	minVol := getEnvFloat("MIN_VOLUME", 30000.0)
	minLiq := getEnvFloat("MIN_LIQUIDITY", 15000.0)
	return TopMarketsConfig{
		RefreshInterval:    getEnvDuration("REFRESH_INTERVAL", 10*time.Minute),
		MinVolume24hr:      minVol,
		MinLiquidity:       minLiq,
		MaxSpread:          getEnvFloat("MAX_SPREAD", 0.05),
		MinVolatility:      getEnvFloat("MIN_VOLATILITY", 0.01),
		MaxN:               getEnvInt("MAX_N", 500),
		KeysetVolumeMin:    getEnvFloat("KEYSET_VOLUME_MIN", minVol),
		KeysetLiquidityMin: getEnvFloat("KEYSET_LIQUIDITY_MIN", minLiq),
		EndDateMinOffset:   getEnvDuration("END_DATE_MIN_OFFSET", 24*time.Hour),
		EndDateMaxOffset:   getEnvDuration("END_DATE_MAX_OFFSET", 0),
		PriceLookback:      getEnvDuration("PRICE_LOOKBACK", 30*24*time.Hour),
		PriceFidelity:      getEnvInt("PRICE_FIDELITY", 60),
		PriceBatchSize:     getEnvInt("PRICE_BATCH_SIZE", 20),
		TradesBatchSize:    getEnvInt("TRADES_BATCH_SIZE", 40),
		PaginateDelay:      getEnvDuration("PAGINATE_DELAY", 0),
	}
}
