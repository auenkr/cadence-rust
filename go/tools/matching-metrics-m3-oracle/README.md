# Matching M3 UDP oracle

Run `../../../../scripts/emit-matching-m3-go-oracle.sh` to use pinned Tally
v3.5.8's actual M3 reporter and UDP Thrift transports. The tool records a
counter, gauge, timer and duration histogram, issues a Tally flush boundary,
closes the reporter, captures packets from two configured UDP hosts, decodes
the v2 oneway Thrift envelopes and prints semantic JSON batches. Each host
receives the same emitted batches; packet order between listeners is not a
comparison requirement.

This is the Go side of encoder, tag, value, timestamp, histogram bucket,
multi-host fan-out and shutdown-flush parity checks. The oracle has not been
run in the source-only lane.
