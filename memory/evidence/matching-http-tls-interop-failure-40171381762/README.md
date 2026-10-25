# HTTP TLS interop failure — run 40171381762

Run: https://github.com/auenkr/cadence-rust/actions/runs/40171381762

Branch: `t3/http-tls-go-oracle-correction-20261025`

Exact source: `5cafc2dc941fc676cad66d6756fb812c502aa5bf`

Conclusion: failure before TLS assertions.

Parent-reported stage: embedded TChannel passed; the configured decision-task
delivery probe for TChannel/HTTP/trailing behavior then emitted a “not enough
hosts” membership warning. TLS was not reached. Sonnet owns diagnosis; this
record does not infer a cause or suggest a fix.

Original Actions artifact ZIP is retained outside the repository at
`/mnt/storage/service/ci-evidence/run-40171381762/matching-http-tls-interop.zip`
with SHA256
`b416ad16410b27a6fd1ee18c58c6e0de400404451a2c65cd8e7ac570bf2d266f`.
No rerun or other workflow dispatch was made as part of integration.
