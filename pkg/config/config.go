package config

import (
	"fmt"
	"os"
)

type Config struct {
	UseREPL  bool
	WALPath  string
	SockName string
}

func NewDefaultConfig() *Config {
	return &Config{
		UseREPL:  false,
		WALPath:  "wal.log",
		SockName: "/tmp/dkvg.sock",
	}
}

func (cfg *Config) ParseArgs(args []string) {
	l := len(args)
	for i := 0; i < l; i++ {
		arg := args[i]
		switch arg {
		case "--help", "-h", "help":
			Usage(0)
		case "--repl":
			cfg.UseREPL = true
		case "--sock", "-s":
			cfg.SockName = args[i+1]
			fmt.Printf("Setting socket name: %s\n", cfg.SockName)
			i++
		default:
			panic("unrecognized arg: " + arg)
		}
	}
}

func Usage(ec int) {
	fmt.Printf("Usage: $0 [ --sock /path/to/sock.sock ] [ --repl ] [ --help ]\n")
	os.Exit(ec)
}
