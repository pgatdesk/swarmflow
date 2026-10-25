// Command swarmflow is a SwarmFlow peer and its helper tools.
//
//	swarmflow gendata       -out FILE -size 64MiB [-seed N]
//	swarmflow make-manifest -in FILE [-out FILE.swarm] [-piece-size 256KiB]
//	swarmflow seed          -manifest FILE.swarm -file FILE [peer flags]
//	swarmflow get           -manifest FILE.swarm [-out PATH] [peer flags]
package main

import (
	"os"

	"github.com/pgatdesk/swarmflow/internal/cli"
)

func main() { os.Exit(cli.Main(os.Args[1:], os.Stdout, os.Stderr)) }
