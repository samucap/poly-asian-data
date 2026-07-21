package db_test

import (
	"context"
	"testing"

	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResetTagCatalog_NilConn(t *testing.T) {
	_, err := db.ResetTagCatalog(context.Background(), nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, db.ErrNilDB)
}
