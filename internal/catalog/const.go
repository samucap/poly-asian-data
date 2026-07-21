// Package catalog provides shared Gamma tag-catalog and open-events keyset helpers
// for catalog-markets (and top-markets reference path).
package catalog

// CategoriesRootTagID is Polymarket's categories root; top tags have parent_tag_id = this.
const CategoriesRootTagID = "102982"

// GammaKeysetLimit is Gamma /events/keyset max page size.
const GammaKeysetLimit = 500

// SchemaVersion is the catalog artifact schema_version.
const SchemaVersion = "1.0"

// ArtifactPipeline is the artifacts/ subdirectory name for catalog runs.
const ArtifactPipeline = "catalog"
