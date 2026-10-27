# Matching domain cache source error classification (2026-10-27)

## Change

`MatchingDomainCache::get_domain` and `get_domain_by_id` previously reduced every `DomainMetadataSourceError` to `DomainCacheError::Source(message)`, dropping the adapter's already-known `DomainErrorClass`. Cache misses now use `SourceClassified { message, class }` for recognized non-`Internal` classes and retain the existing `Source(String)` representation for `Internal` errors. Display text is unchanged. This carries typed classifications across the cache boundary without message parsing or changing cache key/callback behavior.

Production source adapters classify `MatchingDomainMetadataError::NotFound` as `DomainErrorClass::NotFound`; validation errors map to `BadRequest`. SQL/driver errors currently unsupported by a more specific adapter class remain `Internal`. Other recognized class values pass through unchanged.

A unit regression, `name_cache_miss_preserves_typed_missing_domain_and_internal_errors`, verifies a name lookup retains `NotFound`, while an ordinary source failure stays `Source(String)`. It is source-only evidence until CI completes.

## Validation evidence

- `git diff --check`: passed locally (does not invoke Cargo).
- No local Cargo, Go, build, test, formatting, or runtime command was run.
- Focused public GitHub Actions workflow dispatched: https://github.com/auenkr/cadence-rust/actions/runs/40481661096
- Head SHA: `7a159cea82fdbdfe36e3a3d1c607acba5aff3bef`
- Last observed workflow status: `in_progress`; conclusion pending. Do not treat this as passing validation.

## Integration note

RPC status mapping should handle `DomainCacheError::SourceClassified` by its `class`; the cache source change intentionally leaves `grpc_matching.rs` untouched.
