/*
    Cache-Aware Load Balancing Router

    This router combines two strategies to optimize both cache utilization and request distribution:

    1. Cache-Aware Routing (Approximate Tree)
    2. Load Balancing (Shortest Queue with Balance Thresholds)

    The router dynamically switches between these strategies based on load conditions:
    - Preserves the best cache prefix across the fleet unless its holder is hot
    - Uses active request count among equal cache holders and on cache misses

    A cache holder is considered hot when its load exceeds the healthy-worker
    mean by both the configured absolute and relative margins.

    Strategy Details:

    1. Cache-Aware Routing (Approximate Tree)
    -------------------------------------------
    This strategy maintains an approximate radix tree for each worker based on request history,
    eliminating the need for direct cache state queries. The tree stores raw text characters
    instead of token IDs to avoid tokenization overhead.

    Process:
    a. For each request, find the worker with the highest prefix match
    b. If match rate > cache_threshold:
    Route to the strongest healthy cache holder, breaking ties by load
    c. If match rate ≤ cache_threshold:
    Route to the least-loaded healthy worker and remember the placement
    d. Background maintenance:
    Periodically evict least recently used leaf nodes to prevent memory overflow

    2. Load Balancing (Shortest Queue)
    -------------------------------------------
    Active request counts are a lightweight expected-wait proxy for misses,
    equal cache matches, and hot-holder spills.

    Configuration Parameters:
    ------------------------
    1. cache_threshold: (float, 0.0 to 1.0)
    Minimum prefix match ratio at which remembered cache affinity is trusted.

    2. balance_abs_threshold: (integer)
    Absolute active-request margin used by the cache-holder spill gate.

    3. balance_rel_threshold: (float)
    Relative load margin used with balance_abs_threshold by the spill gate.

    4. eviction_interval_secs: (integer)
    Interval between LRU eviction cycles for the approximate trees.

    5. max_tree_size: (integer)
    Maximum remembered characters per worker in a model tree. When exceeded, LRU leaf nodes are evicted
    during the next eviction cycle.
*/

use super::{get_healthy_worker_indices, CacheAwareConfig, LoadBalancingPolicy, RequestHeaders};
use crate::core::Worker;
use crate::metrics::RouterMetrics;
use crate::policies::normalize_model_key;
use crate::tree::Tree;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tracing::{debug, info};

/// Cache-aware routing policy
///
/// Preserves the strongest cache prefix unless its holder is overloaded, and
/// uses active request count for misses and affinity ties.
/// Maintains separate trees per model for multi-model support.
#[derive(Debug)]
pub struct CacheAwarePolicy {
    config: CacheAwareConfig,
    trees: Arc<DashMap<String, Arc<Tree>>>, // model_id -> Arc<Tree>
    eviction_handle: Option<thread::JoinHandle<()>>,
}

impl CacheAwarePolicy {
    pub fn new() -> Self {
        Self::with_config(CacheAwareConfig::default())
    }

    pub fn with_config(config: CacheAwareConfig) -> Self {
        let trees = Arc::new(DashMap::<String, Arc<Tree>>::new());

        // Start background eviction thread if configured
        let eviction_handle = if config.eviction_interval_secs > 0 {
            let trees_clone = Arc::clone(&trees);
            let max_tree_size = config.max_tree_size;
            let interval = config.eviction_interval_secs;

            Some(thread::spawn(move || loop {
                thread::sleep(Duration::from_secs(interval));

                // Evict for all model trees
                for entry in trees_clone.iter() {
                    let model_id = entry.key();
                    let tree = entry.value();
                    tree.evict_tenant_by_size(max_tree_size);
                    debug!(
                        "Cache eviction completed for model {}, max_size: {}",
                        model_id, max_tree_size
                    );
                }
            }))
        } else {
            None
        };

        Self {
            config,
            trees,
            eviction_handle,
        }
    }

    /// Add a single worker to the tree (incremental update)
    pub fn add_worker(&self, worker: &dyn Worker) {
        let tree_key = normalize_model_key(worker.model_id());
        let tree = self
            .trees
            .entry(tree_key.to_string())
            .or_insert_with(|| Arc::new(Tree::new()));
        tree.insert("", worker.url());
    }

    /// Add a worker by URL and model (for backward compatibility)
    pub fn add_worker_by_url(&self, url: &str, model_id: &str) {
        let tree = self
            .trees
            .entry(model_id.to_string())
            .or_insert_with(|| Arc::new(Tree::new()));
        tree.insert("", url);
    }

    /// Remove a worker from the tree
    pub fn remove_worker(&self, worker: &dyn Worker) {
        let tree_key = normalize_model_key(worker.model_id());
        if let Some(tree) = self.trees.get(tree_key) {
            tree.remove_tenant(worker.url());
        }
    }

    /// Remove a worker by URL (removes from all model trees for backward compatibility)
    pub fn remove_worker_by_url(&self, url: &str) {
        // Remove from all trees since we don't know which model it belongs to
        for tree_ref in self.trees.iter() {
            tree_ref.value().remove_tenant(url);
        }
    }

    /// Run cache eviction to prevent unbounded growth
    pub fn evict_cache(&self, max_size: usize) {
        for tree_ref in self.trees.iter() {
            let model_id = tree_ref.key();
            let tree = tree_ref.value();
            tree.evict_tenant_by_size(max_size);
            debug!(
                "Cache eviction for model {}, max_size: {}",
                model_id, max_size
            );
        }
    }
}

impl LoadBalancingPolicy for CacheAwarePolicy {
    fn select_worker_with_headers(
        &self,
        workers: &[Arc<dyn Worker>],
        request_text: Option<&str>,
        _headers: Option<&RequestHeaders>,
    ) -> Option<usize> {
        let healthy_indices = get_healthy_worker_indices(workers);

        if healthy_indices.is_empty() {
            return None;
        }

        // Determine the model for this set of workers (router pre-filters by model)
        // All workers should be from the same model
        let model_id = normalize_model_key(workers[healthy_indices[0]].model_id());

        // Metrics describe the same healthy pool considered for selection.
        let (min_load, max_load) =
            healthy_indices
                .iter()
                .fold((usize::MAX, 0usize), |(min, max), &idx| {
                    let load = workers[idx].load();
                    (min.min(load), max.max(load))
                });

        // Check cache affinity in both balanced and imbalanced states. Fleet
        // imbalance alone is not a reason to discard a useful prefix hit;
        // the selected holder's own load is checked by the spill gate below.
        let text = request_text.unwrap_or("");

        // Get the tree reference without locking the entire HashMap
        // DashMap only locks the specific shard containing this key
        let tree = self
            .trees
            .entry(model_id.to_string())
            .or_insert_with(|| Arc::new(Tree::new()))
            .value()
            .clone();

        debug!("Using cache-aware routing for model '{}'", model_id);
        // Compare each healthy worker against its own recorded path. The tree
        // query is tenant-aware, so a shared ancestor only counts for a worker
        // if that worker actually owns the matched path.
        let input_chars = text.chars().count();
        let matches = healthy_indices
            .iter()
            .map(|&idx| {
                (
                    idx,
                    tree.prefix_match_tenant_char_count(text, workers[idx].url()),
                )
            })
            .collect::<Vec<_>>();
        let best_match_chars = matches.iter().map(|(_, count)| *count).max().unwrap_or(0);
        let match_rate = if input_chars == 0 {
            0.0
        } else {
            best_match_chars as f32 / input_chars as f32
        };
        let matching_holders = if !text.is_empty() && match_rate > self.config.cache_threshold {
            matches
                .iter()
                .filter_map(|(idx, count)| (*count == best_match_chars).then_some(*idx))
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        debug!(
            "Cache match for model '{}': matched_chars={}, input_chars={}, match_rate={:.2}, holders={}",
            model_id, best_match_chars, input_chars, match_rate, matching_holders.len()
        );

        // Prefer the least-loaded worker among equal best cache holders.
        // If all best holders are materially hotter than the healthy-worker
        // mean, deliberately spill to the least-loaded healthy worker.
        let mut selected_idx = if matching_holders.is_empty() {
            None
        } else {
            matching_holders
                .iter()
                .min_by_key(|&&idx| workers[idx].load())
                .copied()
        };
        if let Some(hit_idx) = selected_idx {
            let total_load: usize = healthy_indices.iter().map(|&i| workers[i].load()).sum();
            let mean_load = total_load as f32 / healthy_indices.len() as f32;
            let hit_load = workers[hit_idx].load() as f32;
            let overloaded = healthy_indices.len() > 1
                && hit_load > mean_load + self.config.balance_abs_threshold as f32
                && hit_load > mean_load * self.config.balance_rel_threshold;
            if overloaded {
                selected_idx = healthy_indices
                    .iter()
                    .min_by_key(|&&idx| workers[idx].load())
                    .copied();
                RouterMetrics::record_load_balancing_event();
                RouterMetrics::set_load_range(max_load, min_load);
            }
        } else {
            // A miss (or too little usable overlap) falls back to the least
            // expected-wait proxy available in this HTTP-only router: active
            // request count. We still remember the final placement below.
            selected_idx = healthy_indices
                .iter()
                .min_by_key(|&&idx| workers[idx].load())
                .copied();
            RouterMetrics::record_load_balancing_event();
            RouterMetrics::set_load_range(max_load, min_load);
        }

        let idx = selected_idx?;
        // Learn the final placement, including spill decisions, for future
        // turns. Multiple workers may retain the same prefix in the tree.
        if !text.is_empty() {
            tree.insert(text, workers[idx].url());
        }
        workers[idx].increment_processed();
        RouterMetrics::record_processed_request(workers[idx].url());
        RouterMetrics::record_policy_decision(self.name(), workers[idx].url());
        Some(idx)
    }

    fn name(&self) -> &'static str {
        "cache_aware"
    }

    fn needs_request_text(&self) -> bool {
        true // Cache-aware policy needs request text for cache affinity
    }

    fn on_request_complete(&self, worker_url: &str, success: bool) {
        // Could track success rates per worker for more intelligent routing
        if !success {
            // Optionally reduce affinity for failed requests
            tracing::debug!(
                "Request to {} completed with success={}",
                worker_url,
                success
            );
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn select_worker_pair_with_headers(
        &self,
        prefill_workers: &[Arc<dyn Worker>],
        decode_workers: &[Arc<dyn Worker>],
        request_text: Option<&str>,
        headers: Option<&RequestHeaders>,
    ) -> Option<(usize, usize)> {
        // DEPRECATED: This method is no longer used when separate policies are configured.
        // The PD router now uses separate policies for prefill and decode selection.
        // This implementation remains for backward compatibility when a single policy is used.

        // In PD mode with single policy:
        // - Prefill: Use cache-aware routing for better cache utilization
        // - Decode: Use least-load routing for better load distribution

        // Select prefill worker using cache-aware logic
        let prefill_idx =
            self.select_worker_with_headers(prefill_workers, request_text, headers)?;

        // Select decode worker using least-load logic
        let healthy_decode = get_healthy_worker_indices(decode_workers);
        if healthy_decode.is_empty() {
            return None;
        }

        let decode_idx = healthy_decode
            .iter()
            .min_by_key(|&&idx| decode_workers[idx].load())
            .copied()?;

        Some((prefill_idx, decode_idx))
    }

    fn requires_initialization(&self) -> bool {
        true // Cache-aware policy requires init_workers() to set up trees
    }

    fn init_workers(&self, workers: &[Arc<dyn Worker>]) {
        // Group workers by model
        info!(
            "Initializing workers for cache-aware policy: {}",
            workers
                .iter()
                .map(|w| w.url())
                .collect::<Vec<_>>()
                .join(", ")
        );
        let mut model_workers: HashMap<String, Vec<&Arc<dyn Worker>>> = HashMap::new();
        for worker in workers {
            let tree_key = normalize_model_key(worker.model_id());
            model_workers
                .entry(tree_key.to_string())
                .or_default()
                .push(worker);
        }

        // Initialize tree for each model
        for (tree_key, model_workers) in model_workers {
            info!(
                "Creating tree for model key: '{}' with {} workers",
                tree_key,
                model_workers.len()
            );
            let tree = self
                .trees
                .entry(tree_key)
                .or_insert_with(|| Arc::new(Tree::new()))
                .clone();
            for worker in model_workers {
                tree.insert("", worker.url());
            }
        }
    }
}

impl Default for CacheAwarePolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for CacheAwarePolicy {
    fn drop(&mut self) {
        // Note: We can't properly stop the eviction thread since it's in an infinite loop
        // In a production system, we'd use a channel or atomic flag to signal shutdown
        if let Some(handle) = self.eviction_handle.take() {
            // The thread will continue running until the program exits
            // This is acceptable for now since the router typically runs for the lifetime of the program
            drop(handle);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{BasicWorker, WorkerType};

    #[test]
    fn test_cache_aware_with_balanced_load() {
        // Create policy without eviction thread for testing
        let config = CacheAwareConfig {
            eviction_interval_secs: 0, // Disable eviction thread
            ..Default::default()
        };
        let policy = CacheAwarePolicy::with_config(config);
        let workers: Vec<Arc<dyn Worker>> = vec![
            Arc::new(BasicWorker::new(
                "http://w1:8000".to_string(),
                WorkerType::Regular,
            )),
            Arc::new(BasicWorker::new(
                "http://w2:8000".to_string(),
                WorkerType::Regular,
            )),
        ];

        // Initialize the policy with workers
        policy.init_workers(&workers);

        // First request should be distributed
        let idx1 = policy.select_worker(&workers, Some("hello world")).unwrap();

        // Same request should go to same worker (cache hit)
        let idx2 = policy.select_worker(&workers, Some("hello world")).unwrap();
        assert_eq!(idx1, idx2);

        // Similar request should also go to same worker
        let idx3 = policy.select_worker(&workers, Some("hello")).unwrap();
        assert_eq!(idx1, idx3);
    }

    #[test]
    fn test_cache_aware_with_imbalanced_load() {
        let policy = CacheAwarePolicy::with_config(CacheAwareConfig {
            cache_threshold: 0.5,
            balance_abs_threshold: 5,
            balance_rel_threshold: 2.0,
            eviction_interval_secs: 0, // Disable eviction thread
            max_tree_size: 10000,
        });

        let worker1 = BasicWorker::new("http://w1:8000".to_string(), WorkerType::Regular);
        let worker2 = BasicWorker::new("http://w2:8000".to_string(), WorkerType::Regular);

        // Create significant load imbalance
        for _ in 0..20 {
            worker1.increment_load();
        }
        // worker2 has load 0

        let workers: Vec<Arc<dyn Worker>> = vec![Arc::new(worker1), Arc::new(worker2)];
        policy.init_workers(&workers);

        // A cache miss should select worker2 (lower load).
        for _ in 0..5 {
            let idx = policy.select_worker(&workers, Some("test")).unwrap();
            assert_eq!(idx, 1); // Should always pick worker2
        }
    }

    #[test]
    fn cache_hit_survives_fleet_imbalance_unless_holder_is_hot() {
        let policy = CacheAwarePolicy::with_config(CacheAwareConfig {
            cache_threshold: 0.5,
            balance_abs_threshold: 5,
            balance_rel_threshold: 1.1,
            eviction_interval_secs: 0,
            max_tree_size: 10000,
        });
        let first = BasicWorker::new("http://affinity:8000".to_string(), WorkerType::Regular);
        let second = BasicWorker::new("http://cold:8000".to_string(), WorkerType::Regular);
        let workers: Vec<Arc<dyn Worker>> = vec![Arc::new(first), Arc::new(second)];
        policy.init_workers(&workers);

        // A miss lands on the least-loaded worker and records its placement.
        assert_eq!(
            policy.select_worker(&workers, Some("stable prompt prefix")),
            Some(0)
        );
        for _ in 0..20 {
            workers[1].increment_load();
        }
        // Global imbalance alone must not discard the useful cache affinity.
        assert_eq!(
            policy.select_worker(&workers, Some("stable prompt prefix")),
            Some(0)
        );

        for _ in 0..100 {
            workers[0].increment_load();
        }
        // Once the holder exceeds both spill margins, route to the cool worker.
        assert_eq!(
            policy.select_worker(&workers, Some("stable prompt prefix")),
            Some(1)
        );
    }

    #[test]
    fn shared_prefix_tie_prefers_less_loaded_holder() {
        let policy = CacheAwarePolicy::with_config(CacheAwareConfig {
            cache_threshold: 0.5,
            balance_abs_threshold: 1000,
            balance_rel_threshold: 10.0,
            eviction_interval_secs: 0,
            max_tree_size: 10000,
        });
        let workers: Vec<Arc<dyn Worker>> = vec![
            Arc::new(BasicWorker::new(
                "http://w1:8000".to_string(),
                WorkerType::Regular,
            )),
            Arc::new(BasicWorker::new(
                "http://w2:8000".to_string(),
                WorkerType::Regular,
            )),
        ];
        policy.init_workers(&workers);
        let tree = policy.trees.get("default").unwrap().value().clone();
        tree.insert("common prefix alpha", workers[0].url());
        tree.insert("common prefix beta", workers[1].url());
        for _ in 0..3 {
            workers[0].increment_load();
        }

        // Both requests share a substantial prefix; among equally strong
        // holders use active load as the tie breaker.
        assert_eq!(
            policy.select_worker(&workers, Some("common prefix gamma")),
            Some(1)
        );
    }

    #[test]
    fn test_cache_aware_worker_removal() {
        let config = CacheAwareConfig {
            eviction_interval_secs: 0, // Disable eviction thread
            ..Default::default()
        };
        let policy = CacheAwarePolicy::with_config(config);
        let workers: Vec<Arc<dyn Worker>> = vec![
            Arc::new(BasicWorker::new(
                "http://w1:8000".to_string(),
                WorkerType::Regular,
            )),
            Arc::new(BasicWorker::new(
                "http://w2:8000".to_string(),
                WorkerType::Regular,
            )),
        ];

        policy.init_workers(&workers);

        // Route some requests
        policy.select_worker(&workers, Some("test1"));
        policy.select_worker(&workers, Some("test2"));

        // Remove a worker
        policy.remove_worker_by_url("http://w1:8000");
        workers[0].set_healthy(false);

        // All requests should now go to worker2
        let idx = policy.select_worker(&workers, Some("test1")).unwrap();
        assert_eq!(idx, 1);
    }
}
