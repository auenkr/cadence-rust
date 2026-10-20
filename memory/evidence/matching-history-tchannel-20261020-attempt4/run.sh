#!/usr/bin/env bash
set +e
cd /home/ubuntu/.t3/worktrees/cadence-rust/t3-83afa06b/rust
TARGET=/home/ubuntu/.t3/worktrees/cadence-rust/t3-6f14baa6/rust/target
run() {
  name="$1"
  filter="$2"
  CARGO_TARGET_DIR="$TARGET" CARGO_BUILD_JOBS=1 cargo test --locked -p cadence-server --lib "$filter" > "/home/ubuntu/.t3/worktrees/cadence-rust/t3-83afa06b/memory/evidence/matching-history-tchannel-20261020-attempt4/$name.raw.log" 2>&1
  printf '%s\n' "$?" > "/home/ubuntu/.t3/worktrees/cadence-rust/t3-83afa06b/memory/evidence/matching-history-tchannel-20261020-attempt4/$name.exit"
}
run native-peer matching_history_tchannel::typed_codec_tests::native_tchannel_peer_exercises_all_five_history_methods_and_wrappers
run tchannel-redirect matching_history::tests::tchannel_redirect_uses_owner_endpoint_and_preserves_operation_deadline
run matching-history-module matching_history::tests::
run tchannel-client-module tchannel_json_client::adversarial_tests::
exit 0
