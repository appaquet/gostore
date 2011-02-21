package log

import (
	golog "log"
	"os"
	"fmt"
	"time"
)

const (
	L_Fatal   = 0
	L_Error   = 1
	L_Warning = 2
	L_Info    = 3
	L_Debug   = 4
)

var (
	fatal   = golog.New(os.Stderr, "", golog.Ldate|golog.Lmicroseconds) // |L_Date|L_Time
	error   = golog.New(os.Stderr, "", golog.Ldate|golog.Lmicroseconds)
	warning = golog.New(os.Stderr, "", golog.Ldate|golog.Lmicroseconds)
	info    = golog.New(os.Stdout, "", golog.Ldate|golog.Lmicroseconds)
	debug   = golog.New(os.Stdout, "", golog.Ldate|golog.Lmicroseconds)

	Differential = true
	StartTime    int64 = -1

	MaxLevel = 2
)

func Log(level int, message string, v ...interface{}) {
	if level > MaxLevel {
		return
	}

	var curtime int64
	if Differential {
		if StartTime == -1 {
			StartTime = time.Nanoseconds()
		}
		curtime = time.Nanoseconds() - StartTime
	} else {
		curtime = time.Nanoseconds()
	}

	// Miliseconds
	message = fmt.Sprintf("%dms - %s", curtime/1000000, message)

	switch level {
	case L_Fatal:
		s := fmt.Sprintf(message, v...)
		fatal.Output(2, s)
		panic(s)
	case L_Error:
		error.Output(2, fmt.Sprintf(message, v...))
	case L_Warning:
		warning.Output(2, fmt.Sprintf(message, v...))
	case L_Info:
		info.Output(2, fmt.Sprintf(message, v...))
	case L_Debug:
		debug.Output(2, fmt.Sprintf(message, v...))
	}
}

func Fatal(message string, v ...interface{}) {
	Log(L_Fatal, message, v...)
}

func Error(message string, v ...interface{}) { Log(L_Error, message, v...) }

func Warning(message string, v ...interface{}) {
	Log(L_Warning, message, v...)
}

func Info(message string, v ...interface{}) { Log(L_Info, message, v...) }

func Debug(message string, v ...interface{}) { Log(L_Debug, message, v...) }
