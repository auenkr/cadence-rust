# Matching deflake: local_parent_poll_applies_its_own_expiration_and_tailroom

Cause: timing tightness, not a production race. Run 40192622680 hit Elapsed on a 500ms bound; rerun 40193747635 passed.
- Poll 1: request timeout 2s minus two 1s tailroom => returns immediately. Bound widened 500ms -> 900ms; it must stay below ~1s so a single-tailroom regression (1s wait) still fails.
- Poll 2: root expiration is 1ms versus the default long poll (tens of seconds). Bound widened 500ms -> 10s; still discriminates.
Other short timeouts in the file: the 30/50ms ones assert a future is still pending (they pass when slow); line 4649 is a request timeout; 9430 is an intentional 1ms expiry. None changed.
No production code changed.
Validation: CI run recorded in the final report / appended below.
