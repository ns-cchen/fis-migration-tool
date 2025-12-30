// Copyright (c) 2022 Netskope, Inc. All rights reserved.

package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	dbDriver      = "mysql"
	DefaultDBName = "fis"
	dbPoolSize    = 10
	dbConnLife    = 30 * time.Minute
	dbTimeout     = 5
)

var ErrBadHostname = fmt.Errorf("hostname is required")

type SQLClient struct {
	db      *sql.DB
	timeout time.Duration
	name    string
}

func (sc *SQLClient) Name() string {
	if sc == nil {
		return ""
	}
	return sc.name
}

func (sc *SQLClient) context() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), sc.timeout)
}

func (sc *SQLClient) Close() error {
	if sc.db != nil {
		err := sc.db.Close()
		sc.db = nil
		return err
	}
	return nil
}

func (sc *SQLClient) GetDB() *sql.DB {
	return sc.db
}

func (sc *SQLClient) Ping() error {
	ctx, cancel := sc.context()
	defer cancel()
	return sc.db.PingContext(ctx)
}

func NewSQLClient(hostname, user, pwd string, timeout int, dbType, dbName string) (*SQLClient, error) {
	if hostname == "" {
		return nil, ErrBadHostname
	}

	if dbType == "" {
		dbType = "mp-mariadb"
	}

	if dbName == "" {
		dbName = DefaultDBName
	}

	var dsn string
	switch dbType {
	case "aws-aurora":
		if user == "" {
			user = "root"
		}
		if pwd != "" {
			dsn = fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true", user, pwd, hostname, dbName)
		} else {
			dsn = fmt.Sprintf("%s@tcp(%s)/%s?parseTime=true", user, hostname, dbName)
		}
	case "mp-mariadb":
		dsn = fmt.Sprintf("tcp(%s)/%s?parseTime=true", hostname, dbName)
		if user != "" {
			if pwd != "" {
				user += ":" + pwd
			}
			dsn = user + "@" + dsn
		}
	default:
		return nil, fmt.Errorf("unsupported database type: %s (must be mp-mariadb or aws-aurora)", dbType)
	}

	db, err := sql.Open(dbDriver, dsn)
	if err != nil {
		return nil, err
	}

	db.SetConnMaxLifetime(dbConnLife)
	db.SetMaxOpenConns(dbPoolSize)
	db.SetMaxIdleConns(dbPoolSize)

	if timeout < 1 {
		timeout = dbTimeout
	}

	sc := &SQLClient{
		db:      db,
		timeout: time.Duration(timeout) * time.Second,
		name:    dbType,
	}

	if err = sc.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return sc, nil
}

