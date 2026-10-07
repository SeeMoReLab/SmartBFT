// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	smart "github.com/hyperledger-labs/SmartBFT/pkg/api"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type smallBankLogMode int

const (
	smallBankLogModeQuiet smallBankLogMode = iota
	smallBankLogModeDebug
)

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func newSmallBankLogger(logMode smallBankLogMode) smart.Logger {
	if logMode == smallBankLogModeDebug {
		zapLogger, _ := zap.NewDevelopment()
		return zapLogger.Sugar()
	}

	level := zap.NewAtomicLevelAt(zapcore.PanicLevel)
	writer := zapcore.AddSync(discardWriter{})
	zapLogger := zap.New(zapcore.NewCore(
		zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()),
		zapcore.AddSync(zapcore.Lock(writer)),
		level,
	))
	return zapLogger.Sugar()
}
