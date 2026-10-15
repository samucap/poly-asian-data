package strategyreg

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveLive_ExplicitFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "w.yaml")
	require.NoError(t, os.WriteFile(path, []byte("name: exp\nw_vol: 0.99\n"), 0o644))

	r := ResolveLive(context.Background(), nil, ResolveOpts{
		Strategy:     "default",
		ExplicitPath: path,
	})
	require.Equal(t, SourceFile, r.Source)
	require.Nil(t, r.VersionID)
	require.Equal(t, "exp", r.Weights.Name)
	require.InDelta(t, 0.99, r.Weights.WVol, 1e-9)
	require.NotEmpty(t, r.Hash)
}

func TestResolveLive_MissingFallbackUsesDefaults(t *testing.T) {
	r := ResolveLive(context.Background(), nil, ResolveOpts{
		Strategy:     "default",
		FallbackPath: filepath.Join(t.TempDir(), "nope.yaml"),
	})
	require.Equal(t, SourceDefault, r.Source)
	require.Nil(t, r.VersionID)
	require.NotEmpty(t, r.LoadNote)
	require.Equal(t, "default", r.Weights.Name)
}
