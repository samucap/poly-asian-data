package db

import (
	"context"
	"testing"

	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/stretchr/testify/require"
)

func TestSanitizeTagParents_NullsMissingParents(t *testing.T) {
	// No DB — parents not in batch are cleared.
	in := []*services.PlyMktTag{
		{ID: "child", ParentTagID: "missing_parent", Label: "Child"},
		{ID: "ok", ParentTagID: "child", Label: "OK"}, // parent in batch kept
	}
	out, cleared := sanitizeTagParents(context.Background(), nil, in)
	require.NotEmpty(t, cleared)
	byID := map[string]*services.PlyMktTag{}
	for _, t := range out {
		byID[t.ID] = t
	}
	require.Equal(t, "", byID["child"].ParentTagID)
	require.Equal(t, "child", byID["ok"].ParentTagID)
}

func TestSanitizeTagParents_NullsSelfParent(t *testing.T) {
	in := []*services.PlyMktTag{
		{ID: "x", ParentTagID: "x"},
	}
	out, _ := sanitizeTagParents(context.Background(), nil, in)
	require.Len(t, out, 1)
	require.Equal(t, "", out[0].ParentTagID)
}

func TestSanitizeTagParents_KeepsInBatchParents(t *testing.T) {
	in := []*services.PlyMktTag{
		{ID: "family", ParentTagID: ""},
		{ID: "league", ParentTagID: "family"},
	}
	out, cleared := sanitizeTagParents(context.Background(), nil, in)
	require.Empty(t, cleared)
	byID := map[string]*services.PlyMktTag{}
	for _, t := range out {
		byID[t.ID] = t
	}
	require.Equal(t, "family", byID["league"].ParentTagID)
}
