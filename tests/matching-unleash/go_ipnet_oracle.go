package main

import (
	"fmt"
	"net"
	"os"
)

func check(address, cidr string, want bool) {
	ip := net.ParseIP(address)
	_, network, err := net.ParseCIDR(cidr)
	if ip == nil || err != nil || network.Contains(ip) != want {
		fmt.Fprintf(os.Stderr, "net.ParseCIDR/IPNet.Contains(%q, %q) = %v, want %v (err=%v)\n", address, cidr, network != nil && network.Contains(ip), want, err)
		os.Exit(1)
	}
}

func main() {
	// ParseCIDR canonicalizes a mapped IPv4 network and its prefix. /120 is
	// IPv4 /24 (so .10 is contained); /121 is IPv4 /25 (so .10 is not).
	check("192.0.2.10", "::ffff:192.0.2.128/120", true)
	check("192.0.2.10", "::ffff:192.0.2.128/121", false)
	check("::ffff:192.0.2.10", "192.0.2.0/24", true)
}
