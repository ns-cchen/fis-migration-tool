// Copyright (c) 2022 Netskope, Inc. All rights reserved.

package log

import (
	"os"
	"path/filepath"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NewLogger returns a logger using the Zap structured logger.
// If stdout is false, a file-based logger is used. Otherwise a console logger is used.
func NewLogger(logDir, logName string, debug, stdout bool) (*zap.Logger, error) {
	cfg := zap.NewProductionEncoderConfig()
	cfg.EncodeTime = zapcore.EpochTimeEncoder
	cfg.LevelKey = "lv"
	cfg.EncodeLevel = func(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString(l.CapitalString()[:2])
	}

	level := zap.InfoLevel
	if debug {
		level = zap.DebugLevel
		cfg.EncodeCaller = zapcore.ShortCallerEncoder
		cfg.CallerKey = "call"
	}

	var core zapcore.Core
	if stdout {
		core = zapcore.NewCore(zapcore.NewJSONEncoder(cfg),
			zapcore.AddSync(os.Stdout), level)
	} else {
		if logDir == "" {
			logDir = "/tmp"
		}
		if logName == "" {
			logName = filepath.Base(os.Args[0])
		}

		logFile := filepath.Join(logDir, logName+".log")
		file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, err
		}

		core = zapcore.NewCore(zapcore.NewJSONEncoder(cfg),
			zapcore.AddSync(file), level)
	}

	var logger *zap.Logger
	if debug {
		logger = zap.New(core, zap.AddCaller())
	} else {
		logger = zap.New(core)
	}

	return logger, nil
}

