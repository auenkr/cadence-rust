# Native codec wire-edge validation checkpoint

Matching remains incomplete. Production handlers still use the UTF-8 model.

Combined diagnostic40581471310 ataf52d2f3d failed in build-script Clippy:
the new oneof generator checked is_some then unwrapped. Parent now binds
the index using if-let/filter. The generated codecs did not compile in that
attempt and no runtime native transport pass is claimed.
https://github.com/auenkr/cadence-rust/actions/runs/40581471310

The wire-edge worker extended the original Go oracle and added a comparison
test. Parent corrected the purported duplicate-map case to actually repeat
the raw key, added actual Int64Value empty/zero/repeated-wrapper cases, and
limited decoded-value comparisons to accepted messages. Scratch state after
a rejected decoder call is not observable by a handler. Existing row shape
and outputs are retained; extra edge rows live on the first fixture row.

The new test and generator correction require Actions validation. Two workers
still own native decision response graphs and the remaining Thrift requests;
those partial files must not be included in a parent commit until complete.
No local Cargo, Go, rustfmt or mixed gate ran.
