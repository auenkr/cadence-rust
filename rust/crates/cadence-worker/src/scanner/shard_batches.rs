//! Shard batching used by Go `scanner/shardscanner` scanner and fixer workflows.

/// Select this worker's round-robin indices and group them into batches.
///
/// Go's `getBatchIndices` first assigns source positions using
/// `index % concurrency == worker_index`, then cuts that ordered stream at
/// `batch_size`. Both scanner shard IDs and corrupted-key entries use this
/// exact partitioning rule.
pub fn batch_indices(
    batch_size: usize,
    concurrency: usize,
    slice_length: usize,
    worker_index: usize,
) -> Result<Vec<Vec<usize>>, BatchArgumentsError> {
    if batch_size == 0 {
        return Err(BatchArgumentsError::ZeroBatchSize);
    }
    if concurrency == 0 {
        return Err(BatchArgumentsError::ZeroConcurrency);
    }
    if worker_index >= concurrency {
        return Err(BatchArgumentsError::WorkerIndexOutOfRange {
            worker_index,
            concurrency,
        });
    }

    let mut batches = Vec::new();
    let mut current = Vec::with_capacity(batch_size);
    for index in (worker_index..slice_length).step_by(concurrency) {
        current.push(index);
        if current.len() == batch_size {
            batches.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        batches.push(current);
    }
    Ok(batches)
}

/// Apply the Go shard scanner's round-robin batching to values while
/// preserving their original order within each worker's batches.
pub fn batch_values<T: Clone>(
    values: &[T],
    batch_size: usize,
    concurrency: usize,
    worker_index: usize,
) -> Result<Vec<Vec<T>>, BatchArgumentsError> {
    batch_indices(batch_size, concurrency, values.len(), worker_index).map(|batches| {
        batches
            .into_iter()
            .map(|batch| batch.into_iter().map(|index| values[index].clone()).collect())
            .collect()
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchArgumentsError {
    ZeroBatchSize,
    ZeroConcurrency,
    WorkerIndexOutOfRange {
        worker_index: usize,
        concurrency: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::{batch_indices, batch_values, BatchArgumentsError};

    #[test]
    fn go_get_batch_indices_fixtures_match_all_worker_partitions() {
        let expected = [
            vec![vec![0, 3, 6, 9, 12], vec![15, 18]],
            vec![vec![1, 4, 7, 10, 13], vec![16, 19]],
            vec![vec![2, 5, 8, 11, 14], vec![17]],
        ];
        for (worker, batches) in expected.iter().enumerate() {
            assert_eq!(batch_indices(5, 3, 20, worker), Ok(batches.clone()));
        }
    }

    #[test]
    fn go_get_shard_batches_preserves_round_robin_order_and_short_tail() {
        let shards: Vec<_> = (5..50).step_by(2).collect();
        assert_eq!(
            batch_values(&shards, 5, 3, 1),
            Ok(vec![vec![7, 13, 19, 25, 31], vec![37, 43, 49]])
        );
    }

    #[test]
    fn invalid_batch_parameters_are_reported_before_partitioning() {
        assert_eq!(
            batch_indices(0, 1, 1, 0),
            Err(BatchArgumentsError::ZeroBatchSize)
        );
        assert_eq!(
            batch_indices(1, 0, 1, 0),
            Err(BatchArgumentsError::ZeroConcurrency)
        );
        assert_eq!(
            batch_indices(1, 2, 1, 2),
            Err(BatchArgumentsError::WorkerIndexOutOfRange {
                worker_index: 2,
                concurrency: 2,
            })
        );
    }
}
