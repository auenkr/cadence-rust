use cadence_common::ringpop_bootstrap_selection::{BootstrapSelection, Options};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Fixture {
    cases: Vec<Case>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Case {
    name: String,
    local: String,
    seeds: Vec<String>,
    requested_timeout_nanos: i64,
    requested_max_join_duration_nanos: i64,
    requested_size: i64,
    requested_parallelism: i64,
    bootstrap_hosts: Vec<String>,
    potential: Option<Vec<String>>,
    preferred: Option<Vec<String>>,
    non_preferred: Option<Vec<String>>,
    joined: Option<Vec<String>>,
    group: Option<Vec<String>>,
    timeout_nanos: i64,
    max_join_duration_nanos: i64,
    size: i64,
    parallelism: i64,
}

fn sorted(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values
}

#[derive(Default)]
struct First;
impl cadence_common::ringpop_bootstrap_selection::IndexChooser for First {
    fn choose(&mut self, _: usize) -> usize {
        0
    }
}

#[test]
fn rust_selector_consumes_pinned_original_go_selection_cases() {
    let fixture: Fixture = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/ringpop_bootstrap_selection.json"
    ))
    .unwrap();
    for case in fixture.cases {
        let local = case.local.as_str();
        let seeds = case.seeds.clone();
        assert_eq!(
            case.bootstrap_hosts.len(),
            seeds.len() + usize::from(!seeds.iter().any(|s| s == local)),
            "{}",
            case.name
        );
        let options = Options::from_signed(
            case.requested_timeout_nanos,
            case.requested_max_join_duration_nanos,
            case.requested_size,
            case.requested_parallelism,
        );
        let mut selector = BootstrapSelection::new(local, &seeds, options);
        assert_eq!(
            sorted(selector.bootstrap_hosts.clone()),
            sorted(case.bootstrap_hosts),
            "{} bootstrap",
            case.name
        );
        selector.init(case.joined.as_deref().unwrap_or_default());
        assert_eq!(
            sorted(selector.potential.clone()),
            sorted(case.potential.unwrap_or_default()),
            "{} potential",
            case.name
        );
        assert_eq!(
            sorted(selector.preferred.clone()),
            sorted(case.preferred.unwrap_or_default()),
            "{} preferred",
            case.name
        );
        assert_eq!(
            sorted(selector.non_preferred.clone()),
            sorted(case.non_preferred.unwrap_or_default()),
            "{} nonpreferred",
            case.name
        );
        assert_eq!(selector.options.timeout_nanos, case.timeout_nanos);
        assert_eq!(
            selector.options.max_join_duration_nanos,
            case.max_join_duration_nanos
        );
        assert_eq!(selector.options.size, case.size);
        assert_eq!(selector.options.parallelism, case.parallelism);
        assert_eq!(
            selector.effective_size,
            case.size
                .min(i64::try_from(selector.potential.len()).unwrap_or(i64::MAX))
        );
        let joined = case.joined.unwrap_or_default();
        let mut chooser = First;
        let group = selector.select_group_with(&joined, &mut chooser);
        let pool = selector
            .preferred
            .iter()
            .chain(selector.non_preferred.iter())
            .cloned()
            .collect::<Vec<_>>();
        let mut available = pool.clone();
        for target in &group {
            let Some(index) = available.iter().position(|item| item == target) else {
                panic!("{} selected an unavailable target {target}", case.name)
            };
            available.remove(index);
        }
        let preferred_picks = group
            .iter()
            .take_while(|target| selector.preferred.contains(target))
            .count();
        assert_eq!(
            preferred_picks,
            group.len().min(selector.preferred.len()),
            "{} preferred targets must be selected before fallback targets",
            case.name
        );
        let cap = (selector.effective_size - joined.len() as i64)
            .wrapping_mul(selector.options.parallelism);
        let expected_len = if cap < 0 {
            0
        } else {
            (cap as usize).min(pool.len())
        };
        assert_eq!(group.len(), expected_len, "{} group length", case.name);
        if cap as usize >= pool.len() && cap >= 0 {
            assert_eq!(
                sorted(group.clone()),
                sorted(pool),
                "{} full group",
                case.name
            );
        }
        // Go maps define only preference classes and eligibility, not exact
        // subsets or ordering. The fixture's group is therefore not compared.
        let _oracle_group = case.group;
    }
}

#[test]
fn requested_size_is_capped_and_exhausted_round_restarts() {
    let seeds = vec!["n2:1".to_owned(), "n3:1".to_owned()];
    let mut selector = BootstrapSelection::new("n1:1", &seeds, Options::from_signed(0, 0, 9, 1));
    assert_eq!(selector.effective_size, 2);
    let mut chooser = First;
    let first = selector.select_group_with(&[], &mut chooser);
    let second = selector.select_group_with(&[], &mut chooser);
    assert_eq!(first.len(), 2);
    assert_eq!(sorted(first), sorted(second));
}

#[test]
fn capture_host_matches_go_split_host_port_matrix() {
    use cadence_common::ringpop_bootstrap_selection::capture_host;
    let cases = [
        ("host:", "host"),
        (":80", ""),
        ("[::1]:", "::1"),
        ("[fe80::1%eth0]:80", "fe80::1%eth0"),
        ("[abc]:80", "abc"),
        ("[[::1]:80", ""),
        ("host:abc", "host"),
        ("host:1:2", ""),
        ("host:[]:80", ""),
        ("[::1]x:80", ""),
        ("[::1]:80:90", ""),
        ("host:80]", ""),
        ("[]:80", ""),
    ];
    for (input, expected) in cases {
        assert_eq!(capture_host(input), expected, "{input}");
    }
}

#[test]
fn go_int64_group_cap_wraps_and_partial_round_exhausts_exactly() {
    let seeds = vec!["n2:1".to_owned(), "n3:1".to_owned(), "n4:1".to_owned()];
    let mut selector =
        BootstrapSelection::new("n1:1", &seeds, Options::from_signed(0, 0, 2, i64::MAX));
    let mut first = First;
    assert!(selector.select_group_with(&[], &mut first).is_empty()); // Go int64 overflow produces a negative loop bound.

    let mut selector = BootstrapSelection::new("n1:1", &seeds, Options::from_signed(0, 0, 2, 1));
    let round1 = selector.select_group_with(&[], &mut First);
    assert_eq!(round1.len(), 2);
    let round2 = selector.select_group_with(&[], &mut First);
    assert_eq!(round2.len(), 1);
    let round3 = selector.select_group_with(&[], &mut First);
    assert_eq!(round3.len(), 2);
    assert_eq!(sorted(round1), sorted(round3));
}
