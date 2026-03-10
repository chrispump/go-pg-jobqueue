package scenario

import (
	"context"

	"github.com/chrispump/go-pg-jobqueue/internal/db"
)

type Scenario interface {
	Name() string
	Describe() string
	Run(parentCtx context.Context, dbconn *db.DB) (Result, error)
}

type Result struct {
	RunID           string
	FilePath        string
	MetricsFilePath string
}
