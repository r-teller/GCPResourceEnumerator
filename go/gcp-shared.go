package main

type DebugLevel int

const (
	OFF   DebugLevel = 0 // EnumIndex = 0
	FATAL            = 1 // EnumIndex = 1 // Not Implemented Yet
	ERROR            = 2 // EnumIndex = 2 // Not Implemented Yet
	WARN             = 3 // EnumIndex = 3
	INFO             = 4 // EnumIndex = 4 // Not Implemented Yet
	DEBUG            = 5 // EnumIndex = 5 // Not Implemented Yet
	TRACE            = 6 // EnumIndex = 6
)

func (d DebugLevel) String() string {
	return [...]string{"OFF", "FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE"}[d]
}

func (d DebugLevel) EnumIndex() int {
	return int(d)
}

var DebugLevelLevel = DebugLevel(ERROR)
