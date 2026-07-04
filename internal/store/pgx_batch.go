package store

import "github.com/jackc/pgx/v5"

type pgxBatch = *pgx.Batch

func newPgxBatch() pgxBatch {
	return &pgx.Batch{}
}
