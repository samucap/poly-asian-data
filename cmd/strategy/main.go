// Command strategy manages M5 strategy_versions: register, promote, rollback, list.
//
//	go run ./cmd/strategy register --weights configs/strategies/default.yaml
//	go run ./cmd/strategy promote --id 1 --surface artifacts/eval_surface/latest.json
//	go run ./cmd/strategy rollback --strategy default
//	go run ./cmd/strategy list
//	go run ./cmd/strategy show --id 1
//	go run ./cmd/strategy active
//	go run ./cmd/strategy candidate --id 1
//
// Promote is fail-closed: requires eval_surface.promote_eligible && matching weights_hash.
// Exit 2 = gate rejected; exit 1 = other errors.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/pipeline"
	"github.com/samucap/poly-asian-data/internal/strategyreg"
)

const exitGate = 2

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	cmd := os.Args[1]
	args := os.Args[2:]

	logging.Init(os.Getenv("ENV"))
	cfg, err := config.Load()
	if err != nil {
		logging.Error("config", "error", err)
		os.Exit(1)
	}
	logging.Init(cfg.ENV)
	logger := logging.Logger

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-quit
		cancel()
	}()

	factory, err := pipeline.NewFactory(ctx, cfg, logger)
	if err != nil {
		logger.Error("pipeline factory", "error", err)
		os.Exit(1)
	}
	defer factory.Close()
	conn := factory.DB()
	if err := db.EnsureStrategyTables(ctx, conn); err != nil {
		logger.Error("ensure strategy tables", "error", err)
		os.Exit(1)
	}

	var runErr error
	switch cmd {
	case "register":
		runErr = cmdRegister(ctx, conn, args)
	case "promote":
		runErr = cmdPromote(ctx, conn, args)
	case "rollback":
		runErr = cmdRollback(ctx, conn, args)
	case "list":
		runErr = cmdList(ctx, conn, args)
	case "show":
		runErr = cmdShow(ctx, conn, args)
	case "active":
		runErr = cmdActive(ctx, conn, args)
	case "candidate":
		runErr = cmdCandidate(ctx, conn, args)
	case "help", "-h", "--help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", cmd)
		usage()
		os.Exit(1)
	}
	if runErr != nil {
		if errors.Is(runErr, strategyreg.ErrGateRejected) {
			fmt.Fprintf(os.Stderr, "gate rejected: %v\n", runErr)
			os.Exit(exitGate)
		}
		fmt.Fprintf(os.Stderr, "error: %v\n", runErr)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `usage: strategy <command> [flags]

commands:
  register   --weights PATH [--strategy NAME] [--note TEXT] [--git-sha SHA] [--actor A]
  promote    --id N --surface PATH [--reason TEXT] [--actor A]
  rollback   [--strategy NAME] [--reason TEXT] [--actor A]
  candidate  --id N [--reason TEXT] [--actor A]
  list       [--strategy NAME] [--limit N]
  show       --id N
  active     [--strategy NAME]

Promote requires eval_surface.promote_eligible=true and matching weights_hash.
Exit 2 = gate rejected.
Rollback is one-shot (clears prev); a second rollback fails until a new promote.
`)
}

func cmdRegister(ctx context.Context, conn db.DBInterface, args []string) error {
	fs := flag.NewFlagSet("register", flag.ContinueOnError)
	weights := fs.String("weights", "", "path to strategy weights YAML")
	strategy := fs.String("strategy", "", "strategy family name (default: YAML name)")
	note := fs.String("note", "", "optional note")
	gitSHA := fs.String("git-sha", "", "optional git commit")
	actor := fs.String("actor", "operator", "actor label")
	if err := fs.Parse(args); err != nil {
		return err
	}
	v, err := strategyreg.Register(ctx, conn, strategyreg.RegisterOpts{
		WeightsPath: *weights,
		Strategy:    *strategy,
		Note:        *note,
		GitSHA:      *gitSHA,
		Actor:       *actor,
	})
	if err != nil {
		return err
	}
	printJSON(map[string]any{
		"action":       "register",
		"id":           v.ID,
		"strategy":     v.Strategy,
		"status":       v.Status,
		"weights_hash": v.WeightsHash,
		"source_path":  v.SourcePath,
		"created_at":   v.CreatedAt.Format(time.RFC3339),
	})
	return nil
}

func cmdPromote(ctx context.Context, conn db.DBInterface, args []string) error {
	fs := flag.NewFlagSet("promote", flag.ContinueOnError)
	id := fs.Int64("id", 0, "strategy_versions.id")
	surface := fs.String("surface", "", "path to eval_surface JSON")
	reason := fs.String("reason", "", "optional reason")
	actor := fs.String("actor", "operator", "actor label")
	if err := fs.Parse(args); err != nil {
		return err
	}
	a, err := strategyreg.Promote(ctx, conn, strategyreg.PromoteOpts{
		VersionID:   *id,
		SurfacePath: *surface,
		Actor:       *actor,
		Reason:      *reason,
	})
	if err != nil {
		return err
	}
	printJSON(map[string]any{
		"action":      "promote",
		"strategy":    a.Strategy,
		"version_id":  a.VersionID,
		"promoted_at": a.PromotedAt.Format(time.RFC3339),
		"promoted_by": a.PromotedBy,
		"eval_run_id": a.EvalRunID,
		"prev":        a.PrevVersionID,
	})
	return nil
}

func cmdRollback(ctx context.Context, conn db.DBInterface, args []string) error {
	fs := flag.NewFlagSet("rollback", flag.ContinueOnError)
	strategy := fs.String("strategy", "default", "strategy family")
	reason := fs.String("reason", "", "optional reason")
	actor := fs.String("actor", "operator", "actor label")
	if err := fs.Parse(args); err != nil {
		return err
	}
	a, err := strategyreg.Rollback(ctx, conn, *strategy, *actor, *reason)
	if err != nil {
		return err
	}
	printJSON(map[string]any{
		"action":      "rollback",
		"strategy":    a.Strategy,
		"version_id":  a.VersionID,
		"promoted_at": a.PromotedAt.Format(time.RFC3339),
		"prev":        a.PrevVersionID,
	})
	return nil
}

func cmdCandidate(ctx context.Context, conn db.DBInterface, args []string) error {
	fs := flag.NewFlagSet("candidate", flag.ContinueOnError)
	id := fs.Int64("id", 0, "strategy_versions.id")
	reason := fs.String("reason", "", "optional reason")
	actor := fs.String("actor", "operator", "actor label")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id <= 0 {
		return fmt.Errorf("--id required")
	}
	if err := strategyreg.MarkCandidate(ctx, conn, *id, *actor, *reason); err != nil {
		return err
	}
	printJSON(map[string]any{"action": "candidate", "id": *id})
	return nil
}

func cmdList(ctx context.Context, conn db.DBInterface, args []string) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	strategy := fs.String("strategy", "", "filter by strategy name")
	limit := fs.Int("limit", 50, "max rows")
	if err := fs.Parse(args); err != nil {
		return err
	}
	vs, err := strategyreg.List(ctx, conn, *strategy, *limit)
	if err != nil {
		return err
	}
	rows := make([]map[string]any, 0, len(vs))
	for _, v := range vs {
		rows = append(rows, map[string]any{
			"id":           v.ID,
			"strategy":     v.Strategy,
			"status":       v.Status,
			"weights_hash": truncate(v.WeightsHash, 12),
			"source_path":  v.SourcePath,
			"note":         v.Note,
			"created_at":   v.CreatedAt.Format(time.RFC3339),
		})
	}
	printJSON(rows)
	return nil
}

func cmdShow(ctx context.Context, conn db.DBInterface, args []string) error {
	fs := flag.NewFlagSet("show", flag.ContinueOnError)
	id := fs.Int64("id", 0, "strategy_versions.id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id <= 0 {
		return fmt.Errorf("--id required")
	}
	v, err := strategyreg.Get(ctx, conn, *id)
	if err != nil {
		return err
	}
	var params any
	_ = json.Unmarshal(v.Params, &params)
	printJSON(map[string]any{
		"id":           v.ID,
		"strategy":     v.Strategy,
		"status":       v.Status,
		"weights_hash": v.WeightsHash,
		"source_path":  v.SourcePath,
		"git_sha":      v.GitSHA,
		"note":         v.Note,
		"created_at":   v.CreatedAt.Format(time.RFC3339),
		"created_by":   v.CreatedBy,
		"params":       params,
	})
	return nil
}

func cmdActive(ctx context.Context, conn db.DBInterface, args []string) error {
	fs := flag.NewFlagSet("active", flag.ContinueOnError)
	strategy := fs.String("strategy", "default", "strategy family")
	if err := fs.Parse(args); err != nil {
		return err
	}
	la, ok, err := strategyreg.TryLoadActive(ctx, conn, *strategy)
	if err != nil {
		return err
	}
	if !ok {
		printJSON(map[string]any{"strategy": *strategy, "active": false})
		return nil
	}
	printJSON(map[string]any{
		"strategy":     la.Active.Strategy,
		"active":       true,
		"version_id":   la.Version.ID,
		"status":       la.Version.Status,
		"weights_hash": la.Version.WeightsHash,
		"weights_name": la.Weights.Name,
		"promoted_at":  la.Active.PromotedAt.Format(time.RFC3339),
		"promoted_by":  la.Active.PromotedBy,
		"prev":         la.Active.PrevVersionID,
	})
	return nil
}

func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
