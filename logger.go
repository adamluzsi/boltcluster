package boltcluster

import (
	"log"
	"os"
)

// Logger makes able to set Verbosity using the standard logger
type Logger struct {
	logger    *log.Logger
	Verbosity bool
}

func newLogger() *Logger {
	return &Logger{logger: log.New(os.Stderr, "Cluster - ", log.LstdFlags), Verbosity: false}
}

// Printf calls l.Output to print to the logger.
// Arguments are handled in the manner of fmt.Printf.
func (l *Logger) Printf(format string, v ...interface{}) {
	if l.Verbosity {
		l.logger.Printf(format, v...)
	}
}

// Print calls l.Output to print to the logger.
// Arguments are handled in the manner of fmt.Print.
func (l *Logger) Print(v ...interface{}) {
	if l.Verbosity {
		l.logger.Print(v...)
	}
}

// Println calls l.Output to print to the logger.
// Arguments are handled in the manner of fmt.Println.
func (l *Logger) Println(v ...interface{}) {
	if l.Verbosity {
		l.logger.Println(v...)
	}
}

// Fatal is equivalent to l.Print() followed by a call to os.Exit(1).
func (l *Logger) Fatal(v ...interface{}) {
	if l.Verbosity {
		l.logger.Fatal(v...)
	}
}

// Fatalf is equivalent to l.Printf() followed by a call to os.Exit(1).
func (l *Logger) Fatalf(format string, v ...interface{}) {
	if l.Verbosity {
		l.logger.Fatalf(format, v...)
	}
}

// Fatalln is equivalent to l.Println() followed by a call to os.Exit(1).
func (l *Logger) Fatalln(v ...interface{}) {
	if l.Verbosity {
		l.logger.Fatalln(v...)
	}
}
