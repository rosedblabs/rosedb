package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
)

type (
	LogLevel int
	LogType  int
)

const (
	LogFatal   = LogType(0x1)
	LogError   = LogType(0x2)
	LogWarning = LogType(0x4)
	LogInfo    = LogType(0x8)
	LogDebug   = LogType(0x10)
)

const (
	LogLevelNone  = LogLevel(0x0)
	LogLevelFatal = LogLevelNone | LogLevel(LogFatal)
	LogLevelError = LogLevelFatal | LogLevel(LogError)
	LogLevelWarn  = LogLevelError | LogLevel(LogWarning)
	LogLevelInfo  = LogLevelWarn | LogLevel(LogInfo)
	LogLevelDebug = LogLevelInfo | LogLevel(LogDebug)
	LogLevelAll   = LogLevelDebug
)

var _log = New()

func init() {
	SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	SetHighlighting(runtime.GOOS != "windows")
}

func New() *Logger {
	return NewLogger(os.Stderr, "")
}

func NewLogger(w io.Writer, prefix string) *Logger {
	var level LogLevel
	if l := os.Getenv("LOG_LEVEL"); len(l) != 0 {
		level = StringToLogLevel(os.Getenv("LOG_LEVEL"))
	} else {
		level = LogLevelInfo
	}
	return &Logger{_log: log.New(w, prefix, log.LstdFlags), level: level, highlighting: true}
}

func GlobalLogger() *log.Logger {
	return _log._log
}

func SetLevel(level LogLevel) {
	_log.SetLevel(level)
}
func GetLogLevel() LogLevel {
	return _log.level
}

func SetFlags(flags int) {
	_log._log.SetFlags(flags)
}

func Info(v ...interface{}) {
	_log.Info(v...)
}

func Infof(format string, v ...interface{}) {
	_log.Infof(format, v...)
}

func Panic(v ...interface{}) {
	_log.Panic(v...)
}

func Panicf(format string, v ...interface{}) {
	_log.Panicf(format, v...)
}

func Debug(v ...interface{}) {
	_log.Debug(v...)
}

func Debugf(format string, v ...interface{}) {
	_log.Debugf(format, v...)
}

func Warn(v ...interface{}) {
	_log.Warn(v...)
}

func Warnf(format string, v ...interface{}) {
	_log.Warnf(format, v...)
}

func Error(v ...interface{}) {
	_log.Error(v...)
}

func Errorf(format string, v ...interface{}) {
	_log.Errorf(format, v...)
}

func Fatal(v ...interface{}) {
	_log.Fatal(v...)
}

func Fatalf(format string, v ...interface{}) {
	_log.Fatalf(format, v...)
}

func SetLevelByString(level string) {
	_log.SetLevelByString(level)
}

func SetHighlighting(highlighting bool) {
	_log.SetHighlighting(highlighting)
}

type Logger struct {
	_log         *log.Logger
	level        LogLevel
	highlighting bool
}

func (l *Logger) SetHighlighting(highlighting bool) {
	l.highlighting = highlighting
}

func (l *Logger) SetFlags(flags int) {
	l._log.SetFlags(flags)
}

func (l *Logger) Flags() int {
	return l._log.Flags()
}

func (l *Logger) SetLevel(level LogLevel) {
	l.level = level
}

func (l *Logger) SetLevelByString(level string) {
	l.level = StringToLogLevel(level)
}

func (l *Logger) log(t LogType, v ...interface{}) {
	l.logf(t, "%v\n", v)
}

func (l *Logger) logf(t LogType, format string, v ...interface{}) {
	if l.level|LogLevel(t) != l.level {
		return
	}

	logStr, logColor := LogTypeToString(t)
	var s string
	if l.highlighting {
		s = "\033" + logColor + "m[" + logStr + "] " + fmt.Sprintf(format, v...) + "\033[0m"
	} else {
		s = "[" + logStr + "] " + fmt.Sprintf(format, v...)
	}
	_ = l._log.Output(4, s)
}

func (l *Logger) Fatal(v ...interface{}) {
	l.log(LogFatal, v...)
	os.Exit(-1)
}

func (l *Logger) Fatalf(format string, v ...interface{}) {
	l.logf(LogFatal, format, v...)
	os.Exit(-1)
}

func (l *Logger) Panic(v ...interface{}) {
	l._log.Panic(v...)
}

func (l *Logger) Panicf(format string, v ...interface{}) {
	l._log.Panicf(format, v...)
}

func (l *Logger) Error(v ...interface{}) {
	l.log(LogError, v...)
}

func (l *Logger) Errorf(format string, v ...interface{}) {
	l.logf(LogError, format, v...)
}

func (l *Logger) Warn(v ...interface{}) {
	l.log(LogWarning, v...)
}

func (l *Logger) Warnf(format string, v ...interface{}) {
	l.logf(LogWarning, format, v...)
}

func (l *Logger) Debug(v ...interface{}) {
	l.log(LogDebug, v...)
}

func (l *Logger) Debugf(format string, v ...interface{}) {
	l.logf(LogDebug, format, v...)
}

func (l *Logger) Info(v ...interface{}) {
	l.log(LogInfo, v...)
}

func (l *Logger) Infof(format string, v ...interface{}) {
	l.logf(LogInfo, format, v...)
}

func StringToLogLevel(level string) LogLevel {
	switch level {
	case "fatal":
		return LogLevelFatal
	case "error":
		return LogLevelError
	case "warn":
		return LogLevelWarn
	case "warning":
		return LogLevelWarn
	case "debug":
		return LogLevelDebug
	case "info":
		return LogLevelInfo
	}
	return LogLevelAll
}

func LogTypeToString(t LogType) (string, string) {
	switch t {
	case LogFatal:
		return "fatal", "[0;31"
	case LogError:
		return "error", "[0;31"
	case LogWarning:
		return "warning", "[0;33"
	case LogDebug:
		return "debug", "[0;36"
	case LogInfo:
		return "info", "[0;37"
	}
	return "unknown", "[0;37"
}
