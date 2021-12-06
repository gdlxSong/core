package raft

import (
	"io"
	"io/ioutil"
	golog "log"

	"github.com/hashicorp/go-hclog"
	"github.com/tkeel-io/core/pkg/logger"
)

var log = logger.NewLogger("core.placement.raft")

func newLoggerAdapter() hclog.Logger {
	return &loggerAdapter{}
}

type loggerAdapter struct{}

func (l *loggerAdapter) Log(level hclog.Level, msg string, args ...interface{}) {
	switch level {
	case hclog.Debug:
		log.Debugf(msg, args)
	case hclog.Warn:
		log.Debugf(msg, args)
	case hclog.Error:
		log.Debugf(msg, args)
	default:
		log.Debugf(msg, args)
	}
}

func (l *loggerAdapter) Trace(msg string, args ...interface{}) {
	log.Debugf(msg, args)
}

func (l *loggerAdapter) Debug(msg string, args ...interface{}) {
	log.Debugf(msg, args)
}

func (l *loggerAdapter) Info(msg string, args ...interface{}) {
	log.Debugf(msg, args)
}

func (l *loggerAdapter) Warn(msg string, args ...interface{}) {
	log.Debugf(msg, args)
}

func (l *loggerAdapter) Error(msg string, args ...interface{}) {
	log.Debugf(msg, args)
}

func (l *loggerAdapter) IsTrace() bool { return false }

func (l *loggerAdapter) IsDebug() bool { return true }

func (l *loggerAdapter) IsInfo() bool { return false }

func (l *loggerAdapter) IsWarn() bool { return false }

func (l *loggerAdapter) IsError() bool { return false }

func (l *loggerAdapter) ImpliedArgs() []interface{} { return []interface{}{} }

func (l *loggerAdapter) With(args ...interface{}) hclog.Logger { return l }

func (l *loggerAdapter) Name() string { return "core" }

func (l *loggerAdapter) Named(name string) hclog.Logger { return l }

func (l *loggerAdapter) ResetNamed(name string) hclog.Logger { return l }

func (l *loggerAdapter) SetLevel(level hclog.Level) {}

func (l *loggerAdapter) StandardLogger(opts *hclog.StandardLoggerOptions) *golog.Logger {
	return golog.New(l.StandardWriter(opts), "placement-raft", golog.LstdFlags)
}

func (l *loggerAdapter) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	return ioutil.Discard
}
