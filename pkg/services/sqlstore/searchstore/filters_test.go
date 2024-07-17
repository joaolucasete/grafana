package searchstore_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/grafana/grafana/pkg/services/sqlstore/searchstore"
)

func TestFolderUIDFilter(t *testing.T) {
	testCases := []struct {
		description    string
		uids           []string
		expectedSql    string
		expectedParams []any
	}{
		{
			description:    "searching general folder",
			uids:           []string{"general"},
			expectedSql:    "dashboard.folder_id = ? ",
			expectedParams: []any{0},
		},
		{
			description:    "searching a specific folder",
			uids:           []string{"abc-123"},
			expectedSql:    "dashboard.folder_id IN (SELECT id FROM dashboard WHERE org_id = ? AND uid = ?)",
			expectedParams: []any{int64(1), "abc-123"},
		},
		{
			description:    "searching a specific folders",
			uids:           []string{"abc-123", "def-456"},
			expectedSql:    "dashboard.folder_id IN (SELECT id FROM dashboard WHERE org_id = ? AND uid IN (?,?))",
			expectedParams: []any{int64(1), "abc-123", "def-456"},
		},
		{
			description:    "searching a specific folders or general",
			uids:           []string{"general", "abc-123", "def-456"},
			expectedSql:    "(dashboard.folder_id IN (SELECT id FROM dashboard WHERE org_id = ? AND uid IN (?,?)) OR dashboard.folder_id = ?)",
			expectedParams: []any{int64(1), "abc-123", "def-456", 0},
		},
	}

	store := setupTestEnvironment(t)

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			f := searchstore.FolderUIDFilter{
				Dialect: store.GetDialect(),
				OrgID:   1,
				UIDs:    tc.uids,
			}

			sql, params := f.Where()

			assert.Equal(t, tc.expectedSql, sql)
			assert.Equal(t, tc.expectedParams, params)
		})
	}
}
