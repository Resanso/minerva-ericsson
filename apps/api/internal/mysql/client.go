package mysql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	sqldriver "github.com/go-sql-driver/mysql"
)

// Config maps connection settings for the MySQL instance.
type Config struct {
	DSN             string
	Host            string
	Port            string
	User            string
	Password        string
	Database        string
	Params          string
	TLSCAPath       string
	TLSConfigName   string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	PingTimeout     time.Duration
}

// FromEnv constructs Config from environment variables.
func FromEnv() (Config, error) {
	cfg := Config{
		DSN:             strings.TrimSpace(os.Getenv("MYSQL_DSN")),
		Host:            os.Getenv("MYSQL_HOST"),
		Port:            defaultString(os.Getenv("MYSQL_PORT"), "3306"),
		User:            os.Getenv("MYSQL_USER"),
		Password:        os.Getenv("MYSQL_PASSWORD"),
		Database:        os.Getenv("MYSQL_DATABASE"),
		Params:          os.Getenv("MYSQL_PARAMS"),
		TLSCAPath:       os.Getenv("MYSQL_TLS_CA"),
		TLSConfigName:   defaultString(os.Getenv("MYSQL_TLS_CONFIG"), "aiven"),
		MaxOpenConns:    parseInt(os.Getenv("MYSQL_MAX_OPEN_CONNS"), 10),
		MaxIdleConns:    parseInt(os.Getenv("MYSQL_MAX_IDLE_CONNS"), 5),
		ConnMaxLifetime: parseDuration(os.Getenv("MYSQL_CONN_MAX_LIFETIME"), 30*time.Minute),
		PingTimeout:     parseDuration(os.Getenv("MYSQL_PING_TIMEOUT"), 5*time.Second),
	}

	if strings.HasPrefix(strings.ToLower(cfg.DSN), "mysql://") {
		if err := cfg.applyURLDSN(cfg.DSN); err != nil {
			return Config{}, fmt.Errorf("parse MYSQL_DSN: %w", err)
		}
		cfg.DSN = ""
	}

	if cfg.DSN == "" {
		if cfg.Host == "" || cfg.User == "" || cfg.Password == "" || cfg.Database == "" {
			return Config{}, errors.New("incomplete MySQL configuration: provide MYSQL_DSN or MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE")
		}
	}

	return cfg, nil
}

// New opens a pooled MySQL connection and validates connectivity.
func New(ctx context.Context, cfg Config) (*sql.DB, error) {
	dsn, err := cfg.effectiveDSN()
	if err != nil {
		return nil, err
	}

	if cfg.TLSCAPath != "" {
		if err := registerTLSConfig(cfg.TLSConfigName, cfg.TLSCAPath); err != nil {
			return nil, fmt.Errorf("register TLS config: %w", err)
		}
		if !strings.Contains(dsn, "tls=") {
			if strings.Contains(dsn, "?") {
				dsn += "&tls=" + cfg.TLSConfigName
			} else {
				dsn += "?tls=" + cfg.TLSConfigName
			}
		}
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql connection: %w", err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	pingCtx, cancel := context.WithTimeout(ctx, cfg.PingTimeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}

	return db, nil
}

func (cfg Config) effectiveDSN() (string, error) {
	if cfg.DSN != "" {
		return cfg.DSN, nil
	}

	params := cfg.Params
	if params == "" {
		params = "parseTime=true&loc=UTC"
	} else if !strings.Contains(params, "parseTime=") {
		params = "parseTime=true&loc=UTC&" + strings.TrimPrefix(params, "?")
	}

	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?%s",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.Database,
		strings.TrimPrefix(params, "?"),
	), nil
}

func registerTLSConfig(name, caPath string) error {
	pem, err := os.ReadFile(caPath)
	if err != nil {
		return fmt.Errorf("read CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if ok := pool.AppendCertsFromPEM(pem); !ok {
		return errors.New("failed to append CA certificate")
	}
	return sqldriver.RegisterTLSConfig(name, &tls.Config{RootCAs: pool})
}

func (cfg *Config) applyURLDSN(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if parsed.User != nil {
		cfg.User = parsed.User.Username()
		if password, ok := parsed.User.Password(); ok {
			cfg.Password = password
		}
	}
	if host := parsed.Hostname(); host != "" {
		cfg.Host = host
	}
	if port := parsed.Port(); port != "" {
		cfg.Port = port
	}
	if db := strings.TrimPrefix(parsed.Path, "/"); db != "" {
		cfg.Database = db
	}

	query := parsed.Query()
	query.Del("ssl-mode")
	urlParams := query.Encode()

	existing := strings.TrimPrefix(cfg.Params, "?")
	switch {
	case existing != "" && urlParams != "":
		cfg.Params = existing + "&" + urlParams
	case urlParams != "":
		cfg.Params = urlParams
	default:
		cfg.Params = existing
	}

	return nil
}

func defaultString(val, fallback string) string {
	if val == "" {
		return fallback
	}
	return val
}

func parseInt(raw string, fallback int) int {
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		log.Printf("invalid int value %q: %v, using %d", raw, err, fallback)
		return fallback
	}
	return v
}

func parseDuration(raw string, fallback time.Duration) time.Duration {
	if raw == "" {
		return fallback
	}
	dur, err := time.ParseDuration(raw)
	if err != nil {
		log.Printf("invalid duration value %q: %v, using %s", raw, err, fallback)
		return fallback
	}
	return dur
}

// FetchRows executes the provided query and returns the result set as a slice of column maps.
func FetchRows(ctx context.Context, db *sql.DB, query string, args ...any) ([]map[string]any, error) {
	if db == nil {
		return nil, errors.New("mysql fetch: nil db handle")
	}
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	results := make([]map[string]any, 0)
	values := make([]any, len(columns))
	valuePtrs := make([]any, len(columns))

	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}
		row := make(map[string]any, len(columns))
		for i, col := range columns {
			switch v := values[i].(type) {
			case []byte:
				row[col] = string(v)
			default:
				row[col] = v
			}
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}
