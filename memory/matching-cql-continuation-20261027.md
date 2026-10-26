# Matching CQL integration checkpoint

Matching remains incomplete. Current work is Cassandra configured protocol v2/v3 parity; persistence still rejects non-v4 until the transport, body codecs, session selection and legacy metadata paths are integrated and validated. No History migration is authorized by this checkpoint.

The earlier combined source `91c481a612eac48f786f208f4c2f4193864ed966` passed fresh workspace CI 40409014196. This does not validate subsequent CQL changes or replace final exact-source mixed gates. Panic quoting, Cassandra cache UUID normalization, deterministic History wire oracle and adaptive timeout corrections have separate evidence in memory.

CQL header run https://github.com/auenkr/cadence-rust/actions/runs/40412418162 tested `88cec365221c584e8570387d23276db2d7d99d64` and failed compilation because `BufMut` was absent from frame/mod.rs imports. Its artifact is retained at `/mnt/storage/service/ci-evidence/40412418162/cql-header-foundation`. Applied its runner-generated formatting patch and restored the required trait import. These corrections require new CI; no local Cargo, Go or rustfmt ran.

Three delegated increments are in progress: body codec dispatch, explicit session protocol selection, and complete legacy metadata readers. The partial keyspace fallback is on `t3/legacy-keyspace-fallback` (`bd07d8125`, evidence `af9598109`), not yet integrated here. Protocol acceptance and final mixed evidence remain pending.

Correction validation dispatched at `e7657429b`: header run https://github.com/auenkr/cadence-rust/actions/runs/40412871046 and fresh persistence run https://github.com/auenkr/cadence-rust/actions/runs/40412875471. Both were newly dispatched; no result is claimed.

Merged session-builder selection from `6b5279b81` after the above dispatches. Its focused run https://github.com/auenkr/cadence-rust/actions/runs/40412821321 was pending on handback. Selection defaults to V4 and persistence rejection remains. The two earlier runs do not validate this merge. No replacement worker was started because the host load check was 6.15 (above the user's threshold of 6); MemAvailable was 3.8 GiB.
