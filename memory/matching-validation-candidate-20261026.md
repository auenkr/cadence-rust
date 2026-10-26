# Matching validation candidate — 2026-10-26

Candidate assembly starts from `origin/master` at `63946adbddc00e3a03e6688de7881a6c65688a11` (fatal integration `40f98ee1`; later TLS documentation remains outside this candidate). It merges the exact adaptive-scaler change `db5c471c7e1613390c4b21a030bcfc79a6c1a122` and writer shutdown regression `c027a0ae66b80586e139a234d1d2f314dfeb5e0f`. The merges were clean. No master merge or mixed-cluster gate is part of this validation step.

The adaptive production gate `40367985106` is running on the exact adaptive commit. Writer focused CI `40366940209` succeeded on the exact writer commit; its uploaded formatter evidence reports `query-format.exit=1`, and the diff includes `matching_writer.rs`. The earlier Actions formatter change `c0e7830d5` is an ancestor, but the later writer edit still leaves formatting debt. Obtain and apply the mechanical patch through the existing `format_query_files` workflow input; do not change regression semantics.

Independent source review found SDK coverage using the same scaled queue and completion of all 24 load workflows. Root/child routing is not directly asserted, so claims remain scoped to the checked scenarios; no concrete blocker was found. This source review does not establish full Matching service parity.

Expected final check: dispatch `rust-ci.yml` on the stable candidate with `clean_target=true`, no package input, so the fresh runner target runs whole-workspace Clippy followed by workspace tests. Record the final source SHA, run URL, and terminal conclusion here. The candidate remains validation work, not a service-completion claim.
