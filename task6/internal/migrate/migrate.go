package migrate

import (
	_ "embed"
	"fmt"
	"strings"

	"github.com/gocql/gocql"
)

var (
	//go:embed schema.cql
	schema string
)

func Apply(sess *gocql.Session) error {
	parts := strings.Split(schema, ";")
	for _, p := range parts {
		stmt := strings.TrimSpace(p)
		if stmt == "" {
			continue
		}
		if err := sess.Query(stmt).Exec(); err != nil {
			return fmt.Errorf("%w: %s", err, stmt)
		}
	}
	return nil
}
