package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Strategy version status values (M5).
const (
	StrategyStatusDraft     = "draft"
	StrategyStatusCandidate = "candidate"
	StrategyStatusActive    = "active"
	StrategyStatusRetired   = "retired"
)

// StrategyVersion is one immutable board-policy weight freeze.
type StrategyVersion struct {
	ID          int64
	Strategy    string
	Status      string
	Params      json.RawMessage // edge.Weights as JSON
	WeightsHash string
	SourcePath  string
	GitSHA      string
	Note        string
	CreatedAt   time.Time
	CreatedBy   string
}

// StrategyActive is the live pointer for a strategy family name.
type StrategyActive struct {
	Strategy      string
	VersionID     int64
	PromotedAt    time.Time
	PromotedBy    string
	EvalRunID     string
	PrevVersionID *int64
}

// StrategyPromotion is one audit row.
type StrategyPromotion struct {
	ID              int64
	Strategy        string
	Action          string
	FromVersionID   *int64
	ToVersionID     *int64
	EvalRunID       string
	WeightsHash     string
	PromoteEligible *bool
	Reason          string
	Actor           string
	CreatedAt       time.Time
}

// ErrStrategyNotFound is returned when a version or active pointer is missing.
var ErrStrategyNotFound = errors.New("db: strategy version not found")

// ErrNoActiveStrategy is returned when no active pointer exists for a name.
var ErrNoActiveStrategy = errors.New("db: no active strategy")

// ErrNoRollbackTarget is returned when prev_version_id is null (already rolled back once).
var ErrNoRollbackTarget = errors.New("db: no prev_version_id to rollback")

// EnsureStrategyTables creates strategy_versions / strategy_active / strategy_promotions.
// Call once at process start — not from hot-path getters.
func EnsureStrategyTables(ctx context.Context, conn DBInterface) error {
	if conn == nil {
		return ErrNilDB
	}
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS strategy_versions (
			id              BIGSERIAL PRIMARY KEY,
			strategy        TEXT NOT NULL,
			status          TEXT NOT NULL DEFAULT 'draft'
				CHECK (status IN ('draft','candidate','active','retired')),
			params          JSONB NOT NULL,
			weights_hash    TEXT NOT NULL,
			source_path     TEXT,
			git_sha         TEXT,
			note            TEXT,
			created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			created_by      TEXT NOT NULL DEFAULT 'operator'
		)`,
		`CREATE INDEX IF NOT EXISTS idx_strategy_versions_strategy_status
			ON strategy_versions (strategy, status)`,
		`CREATE INDEX IF NOT EXISTS idx_strategy_versions_hash
			ON strategy_versions (weights_hash)`,
		// At most one status=active row per strategy family.
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_versions_one_active
			ON strategy_versions (strategy) WHERE status = 'active'`,
		`CREATE TABLE IF NOT EXISTS strategy_active (
			strategy        TEXT PRIMARY KEY,
			version_id      BIGINT NOT NULL REFERENCES strategy_versions(id),
			promoted_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			promoted_by     TEXT NOT NULL DEFAULT 'operator',
			eval_run_id     TEXT,
			prev_version_id BIGINT REFERENCES strategy_versions(id)
		)`,
		`CREATE TABLE IF NOT EXISTS strategy_promotions (
			id              BIGSERIAL PRIMARY KEY,
			strategy        TEXT NOT NULL,
			action          TEXT NOT NULL,
			from_version_id BIGINT,
			to_version_id   BIGINT,
			eval_run_id     TEXT,
			weights_hash    TEXT,
			promote_eligible BOOLEAN,
			reason          TEXT,
			actor           TEXT NOT NULL DEFAULT 'operator',
			created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_strategy_promotions_strategy_time
			ON strategy_promotions (strategy, created_at DESC)`,
	}
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("db: ensure strategy tables: %w", err)
		}
	}
	return nil
}

// InsertStrategyVersion inserts a draft (or given status) version and returns it with ID.
// Caller must EnsureStrategyTables first.
func InsertStrategyVersion(ctx context.Context, conn DBInterface, v StrategyVersion) (StrategyVersion, error) {
	if conn == nil {
		return StrategyVersion{}, ErrNilDB
	}
	if v.Strategy == "" {
		return StrategyVersion{}, fmt.Errorf("db: strategy name required")
	}
	if v.WeightsHash == "" {
		return StrategyVersion{}, fmt.Errorf("db: weights_hash required")
	}
	if len(v.Params) == 0 {
		return StrategyVersion{}, fmt.Errorf("db: params required")
	}
	status := v.Status
	if status == "" {
		status = StrategyStatusDraft
	}
	actor := v.CreatedBy
	if actor == "" {
		actor = "operator"
	}
	err := conn.QueryRow(ctx, `
		INSERT INTO strategy_versions (strategy, status, params, weights_hash, source_path, git_sha, note, created_by)
		VALUES ($1, $2, $3, $4, NULLIF($5,''), NULLIF($6,''), NULLIF($7,''), $8)
		RETURNING id, created_at
	`, v.Strategy, status, v.Params, v.WeightsHash, v.SourcePath, v.GitSHA, v.Note, actor,
	).Scan(&v.ID, &v.CreatedAt)
	if err != nil {
		return StrategyVersion{}, fmt.Errorf("db: insert strategy_version: %w", err)
	}
	v.Status = status
	v.CreatedBy = actor
	return v, nil
}

// GetStrategyVersion loads one version by id. Does not Ensure tables (hot path).
func GetStrategyVersion(ctx context.Context, conn DBInterface, id int64) (StrategyVersion, error) {
	if conn == nil {
		return StrategyVersion{}, ErrNilDB
	}
	var v StrategyVersion
	var src, git, note *string
	err := conn.QueryRow(ctx, `
		SELECT id, strategy, status, params, weights_hash,
		       source_path, git_sha, note, created_at, created_by
		FROM strategy_versions WHERE id = $1
	`, id).Scan(
		&v.ID, &v.Strategy, &v.Status, &v.Params, &v.WeightsHash,
		&src, &git, &note, &v.CreatedAt, &v.CreatedBy,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return StrategyVersion{}, fmt.Errorf("%w: id=%d", ErrStrategyNotFound, id)
		}
		return StrategyVersion{}, fmt.Errorf("db: get strategy_version: %w", err)
	}
	if src != nil {
		v.SourcePath = *src
	}
	if git != nil {
		v.GitSHA = *git
	}
	if note != nil {
		v.Note = *note
	}
	return v, nil
}

// ListStrategyVersions lists versions for a strategy name (newest first). Empty strategy = all.
func ListStrategyVersions(ctx context.Context, conn DBInterface, strategy string, limit int) ([]StrategyVersion, error) {
	if conn == nil {
		return nil, ErrNilDB
	}
	if limit <= 0 {
		limit = 50
	}
	var rows pgx.Rows
	var err error
	if strategy == "" {
		rows, err = conn.Query(ctx, `
			SELECT id, strategy, status, params, weights_hash,
			       source_path, git_sha, note, created_at, created_by
			FROM strategy_versions
			ORDER BY id DESC
			LIMIT $1
		`, limit)
	} else {
		rows, err = conn.Query(ctx, `
			SELECT id, strategy, status, params, weights_hash,
			       source_path, git_sha, note, created_at, created_by
			FROM strategy_versions
			WHERE strategy = $1
			ORDER BY id DESC
			LIMIT $2
		`, strategy, limit)
	}
	if err != nil {
		return nil, fmt.Errorf("db: list strategy_versions: %w", err)
	}
	defer rows.Close()
	return scanStrategyVersions(rows)
}

func scanStrategyVersions(rows pgx.Rows) ([]StrategyVersion, error) {
	var out []StrategyVersion
	for rows.Next() {
		var v StrategyVersion
		var src, git, note *string
		if err := rows.Scan(
			&v.ID, &v.Strategy, &v.Status, &v.Params, &v.WeightsHash,
			&src, &git, &note, &v.CreatedAt, &v.CreatedBy,
		); err != nil {
			return nil, err
		}
		if src != nil {
			v.SourcePath = *src
		}
		if git != nil {
			v.GitSHA = *git
		}
		if note != nil {
			v.Note = *note
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// GetStrategyActive returns the live pointer for a strategy family. No Ensure.
func GetStrategyActive(ctx context.Context, conn DBInterface, strategy string) (StrategyActive, error) {
	if conn == nil {
		return StrategyActive{}, ErrNilDB
	}
	if strategy == "" {
		strategy = "default"
	}
	var a StrategyActive
	var prev *int64
	var evalRun *string
	err := conn.QueryRow(ctx, `
		SELECT strategy, version_id, promoted_at, promoted_by, eval_run_id, prev_version_id
		FROM strategy_active WHERE strategy = $1
	`, strategy).Scan(&a.Strategy, &a.VersionID, &a.PromotedAt, &a.PromotedBy, &evalRun, &prev)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return StrategyActive{}, fmt.Errorf("%w: %s", ErrNoActiveStrategy, strategy)
		}
		return StrategyActive{}, fmt.Errorf("db: get strategy_active: %w", err)
	}
	if evalRun != nil {
		a.EvalRunID = *evalRun
	}
	a.PrevVersionID = prev
	return a, nil
}

// LoadActiveStrategyVersion joins strategy_active → strategy_versions (two queries).
func LoadActiveStrategyVersion(ctx context.Context, conn DBInterface, strategy string) (StrategyVersion, StrategyActive, error) {
	if strategy == "" {
		strategy = "default"
	}
	a, err := GetStrategyActive(ctx, conn, strategy)
	if err != nil {
		return StrategyVersion{}, StrategyActive{}, err
	}
	v, err := GetStrategyVersion(ctx, conn, a.VersionID)
	if err != nil {
		return StrategyVersion{}, a, err
	}
	return v, a, nil
}

// SetStrategyStatus updates status only (immutable params).
func SetStrategyStatus(ctx context.Context, conn DBInterface, id int64, status string) error {
	if conn == nil {
		return ErrNilDB
	}
	tag, err := conn.Exec(ctx, `UPDATE strategy_versions SET status = $2 WHERE id = $1`, id, status)
	if err != nil {
		return fmt.Errorf("db: set strategy status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: id=%d", ErrStrategyNotFound, id)
	}
	return nil
}

// InsertStrategyPromotion appends an audit row. Caller must EnsureStrategyTables first.
func InsertStrategyPromotion(ctx context.Context, conn DBInterface, p StrategyPromotion) (StrategyPromotion, error) {
	if conn == nil {
		return StrategyPromotion{}, ErrNilDB
	}
	actor := p.Actor
	if actor == "" {
		actor = "operator"
	}
	err := conn.QueryRow(ctx, `
		INSERT INTO strategy_promotions (
			strategy, action, from_version_id, to_version_id,
			eval_run_id, weights_hash, promote_eligible, reason, actor
		) VALUES ($1,$2,$3,$4,NULLIF($5,''),NULLIF($6,''),$7,NULLIF($8,''),$9)
		RETURNING id, created_at
	`, p.Strategy, p.Action, p.FromVersionID, p.ToVersionID,
		p.EvalRunID, p.WeightsHash, p.PromoteEligible, p.Reason, actor,
	).Scan(&p.ID, &p.CreatedAt)
	if err != nil {
		return StrategyPromotion{}, fmt.Errorf("db: insert strategy_promotion: %w", err)
	}
	p.Actor = actor
	return p, nil
}

// ActivateOpts controls a promote/activate transaction.
type ActivateOpts struct {
	VersionID       int64
	Actor           string
	EvalRunID       string
	WeightsHash     string
	PromoteEligible bool
	Reason          string
	// Action is "promote" or "rollback" (audit label).
	Action string
	// ClearPrev when true (rollback): strategy_active.prev_version_id becomes NULL
	// so a second rollback hard-fails instead of toggling.
	ClearPrev bool
}

// ActivateStrategyVersion promotes versionID to active in one transaction.
// Retires every status=active row for that strategy family, then sets the new one active.
func ActivateStrategyVersion(ctx context.Context, conn DBInterface, opts ActivateOpts) (StrategyActive, error) {
	if conn == nil {
		return StrategyActive{}, ErrNilDB
	}
	if opts.VersionID <= 0 {
		return StrategyActive{}, fmt.Errorf("db: version_id required")
	}
	actor := opts.Actor
	if actor == "" {
		actor = "operator"
	}
	action := opts.Action
	if action == "" {
		action = "promote"
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return StrategyActive{}, fmt.Errorf("db: activate begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var strategy, status string
	var hash string
	err = tx.QueryRow(ctx, `
		SELECT strategy, status, weights_hash FROM strategy_versions WHERE id = $1 FOR UPDATE
	`, opts.VersionID).Scan(&strategy, &status, &hash)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return StrategyActive{}, fmt.Errorf("%w: id=%d", ErrStrategyNotFound, opts.VersionID)
		}
		return StrategyActive{}, fmt.Errorf("db: lock strategy_version: %w", err)
	}

	// Lock / read current active pointer (may not exist yet).
	var fromID *int64
	var curVersionID int64
	err = tx.QueryRow(ctx, `
		SELECT version_id FROM strategy_active WHERE strategy = $1 FOR UPDATE
	`, strategy).Scan(&curVersionID)
	hasActive := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return StrategyActive{}, fmt.Errorf("db: lock strategy_active: %w", err)
	}
	if hasActive {
		if curVersionID == opts.VersionID {
			// Already active — fix status if needed; no audit spam.
			if status != StrategyStatusActive {
				if _, err := tx.Exec(ctx, `
					UPDATE strategy_versions SET status = $2
					WHERE strategy = $1 AND status = $3 AND id <> $4
				`, strategy, StrategyStatusRetired, StrategyStatusActive, opts.VersionID); err != nil {
					return StrategyActive{}, err
				}
				if _, err := tx.Exec(ctx, `UPDATE strategy_versions SET status = $2 WHERE id = $1`, opts.VersionID, StrategyStatusActive); err != nil {
					return StrategyActive{}, err
				}
			}
			if err := tx.Commit(ctx); err != nil {
				return StrategyActive{}, err
			}
			return GetStrategyActive(ctx, conn, strategy)
		}
		prev := curVersionID
		fromID = &prev
	}

	// Retire all active rows for this family (integrity; unique index needs zero actives first).
	if _, err := tx.Exec(ctx, `
		UPDATE strategy_versions SET status = $2
		WHERE strategy = $1 AND status = $3 AND id <> $4
	`, strategy, StrategyStatusRetired, StrategyStatusActive, opts.VersionID); err != nil {
		return StrategyActive{}, fmt.Errorf("db: mass-retire active: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		UPDATE strategy_versions SET status = $2 WHERE id = $1
	`, opts.VersionID, StrategyStatusActive); err != nil {
		return StrategyActive{}, fmt.Errorf("db: set active status: %w", err)
	}

	// Promote records prev; rollback clears prev so undo is one-shot (not a toggle).
	var storePrev *int64
	if !opts.ClearPrev && fromID != nil {
		storePrev = fromID
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO strategy_active (strategy, version_id, promoted_at, promoted_by, eval_run_id, prev_version_id)
		VALUES ($1, $2, NOW(), $3, NULLIF($4,''), $5)
		ON CONFLICT (strategy) DO UPDATE SET
			version_id = EXCLUDED.version_id,
			promoted_at = EXCLUDED.promoted_at,
			promoted_by = EXCLUDED.promoted_by,
			eval_run_id = EXCLUDED.eval_run_id,
			prev_version_id = EXCLUDED.prev_version_id
	`, strategy, opts.VersionID, actor, opts.EvalRunID, storePrev)
	if err != nil {
		return StrategyActive{}, fmt.Errorf("db: upsert strategy_active: %w", err)
	}

	pe := opts.PromoteEligible
	wh := opts.WeightsHash
	if wh == "" {
		wh = hash
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO strategy_promotions (
			strategy, action, from_version_id, to_version_id,
			eval_run_id, weights_hash, promote_eligible, reason, actor
		) VALUES ($1,$2,$3,$4,NULLIF($5,''),NULLIF($6,''),$7,NULLIF($8,''),$9)
	`, strategy, action, fromID, opts.VersionID, opts.EvalRunID, wh, pe, opts.Reason, actor)
	if err != nil {
		return StrategyActive{}, fmt.Errorf("db: audit promote: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return StrategyActive{}, fmt.Errorf("db: activate commit: %w", err)
	}
	return GetStrategyActive(ctx, conn, strategy)
}

// RollbackStrategyActive restores prev_version_id once, then clears prev (no toggle).
func RollbackStrategyActive(ctx context.Context, conn DBInterface, strategy, actor, reason string) (StrategyActive, error) {
	if strategy == "" {
		strategy = "default"
	}
	if actor == "" {
		actor = "operator"
	}
	cur, err := GetStrategyActive(ctx, conn, strategy)
	if err != nil {
		return StrategyActive{}, err
	}
	if cur.PrevVersionID == nil || *cur.PrevVersionID <= 0 {
		return StrategyActive{}, fmt.Errorf("%w for strategy %s", ErrNoRollbackTarget, strategy)
	}
	return ActivateStrategyVersion(ctx, conn, ActivateOpts{
		VersionID: *cur.PrevVersionID,
		Actor:     actor,
		Reason:    reason,
		Action:    "rollback",
		ClearPrev: true,
	})
}
