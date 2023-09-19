package pgdb

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	pgxuuid "github.com/jackc/pgx-gofrs-uuid"
	pgxdecimal "github.com/jackc/pgx-shopspring-decimal"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/tracelog"
)

type PgDb struct {
	pool *pgxpool.Pool
}

func NewPgDbWithLog(dbUrl, logLevel string) (*PgDb, error) {
	config, err := pgxpool.ParseConfig(dbUrl)
	if err != nil {
		return nil, err
	}
	pgLogger := NewPgLogger(logLevel)
	config.ConnConfig.Tracer = &tracelog.TraceLog{
		Logger:   pgLogger,
		LogLevel: 0,
	}

	config.AfterConnect = typeRegister

	conn, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, err
	}

	return &PgDb{pool: conn}, nil
}

func NewPgLogger(logLevel string) *PgLogger {
	pgLevel, err := tracelog.LogLevelFromString(logLevel)
	if err != nil {
		slog.Log(context.Background(),
			slog.LevelError,
			fmt.Sprintf("NewPgDbWithLog: failed to parse the logLevel %s with error: %s. defaulting to %s",
				logLevel,
				err.Error(),
				slog.LevelError.String()))
	}

	var level slog.Level
	switch pgLevel {
	case tracelog.LogLevelTrace, tracelog.LogLevelDebug:
		level = slog.LevelDebug
	case tracelog.LogLevelInfo:
		level = slog.LevelInfo
	case tracelog.LogLevelWarn:
		level = slog.LevelWarn
	case tracelog.LogLevelError:
		level = slog.LevelError
	default:
		level = slog.LevelError
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		AddSource: true,
		Level:     level,
	}))
	return &PgLogger{
		logger: logger,
		level:  level,
	}
}

func typeRegister(_ context.Context, conn *pgx.Conn) (err error) {
	pgxuuid.Register(conn.TypeMap())
	pgxdecimal.Register(conn.TypeMap())
	return
}

type PgLogger struct {
	logger *slog.Logger
	level  slog.Level
}

func (p PgLogger) Log(ctx context.Context, level tracelog.LogLevel, msg string, data map[string]any) {
	if level == tracelog.LogLevelNone {
		return
	}
	attrs := make([]slog.Attr, 0)
	for k, v := range data {
		attrs = append(attrs, slog.Any(k, v))
	}
	p.logger.LogAttrs(ctx, p.level, msg, attrs...)
}
