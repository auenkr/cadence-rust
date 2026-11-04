//! Port of engineimpl/{reset_queues,describe_queues}.go.
use super::{deps::*, runtime::*};
use cadence_api::admin::DescribeQueueResponse;
impl HistoryEngine {
    pub async fn reset_transfer_queue(&self, cluster: &str) -> HResult<()> {
        let processor = self
            .queue_processors
            .get(&HistoryTaskCategory::Transfer)
            .ok_or_else(|| HistoryError::Other("transfer processor not found".into()))?;
        processor.handle_action(cluster, QueueAction::Reset).await?;
        Ok(())
    }
    pub async fn reset_timer_queue(&self, cluster: &str) -> HResult<()> {
        let processor = self
            .queue_processors
            .get(&HistoryTaskCategory::Timer)
            .ok_or_else(|| HistoryError::Other("timer processor not found".into()))?;
        processor.handle_action(cluster, QueueAction::Reset).await?;
        Ok(())
    }
    pub async fn describe_transfer_queue(&self, cluster: &str) -> HResult<DescribeQueueResponse> {
        self.describe_queue(HistoryTaskCategory::Transfer, cluster)
            .await
    }
    pub async fn describe_timer_queue(&self, cluster: &str) -> HResult<DescribeQueueResponse> {
        self.describe_queue(HistoryTaskCategory::Timer, cluster)
            .await
    }
    async fn describe_queue(
        &self,
        category: HistoryTaskCategory,
        cluster: &str,
    ) -> HResult<DescribeQueueResponse> {
        let processor = self.queue_processors.get(&category).ok_or_else(|| {
            // Go formats the category struct as {categoryType categoryID categoryName}.
            let name = match category {
                HistoryTaskCategory::Transfer => "{1 1 transfer}",
                HistoryTaskCategory::Timer => "{2 2 timer}",
                HistoryTaskCategory::Replication => "{1 3 replication}",
                HistoryTaskCategory::CrossCluster => "{0 0 cross-cluster}",
            };
            HistoryError::Other(format!("queue processor not found for category {name}"))
        })?;
        // TODO(link): QueueProcessor adapter serializes each ProcessingQueueState using Go's String representation.
        let processing_queue_states = processor
            .handle_action(cluster, QueueAction::GetState)
            .await?;
        Ok(DescribeQueueResponse {
            processing_queue_states,
        })
    }
}
