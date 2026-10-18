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
    bootstrap_hosts: Vec<String>,
    potential: Option<Vec<String>>,
    preferred: Option<Vec<String>>,
    non_preferred: Option<Vec<String>>,
    joined: Option<Vec<String>>,
    group: Option<Vec<String>>,
    timeout_nanos: i64,
    max_join_duration_nanos: i64,
    size: i32,
    parallelism: i32,
}

fn sorted(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values
}

fn inputs(name: &str) -> (&'static str, Vec<String>) {
    let (local, seeds): (&str, &[&str]) = match name {
        "empty-seeds-self-only" => ("127.0.0.1:3000", &[]),
        "normalization-self-and-exact-duplicate" => (
            "127.0.0.1:3000",
            &[
                "127.0.0.1:3000",
                "127.0.0.1:3001",
                "127.0.0.1:3001",
                "127.0.0.2:4000",
            ],
        ),
        "remote-host-preference" => (
            "192.0.2.1:1",
            &["192.0.2.1:2", "192.0.2.1:3", "192.0.2.2:1", "192.0.2.3:1"],
        ),
        "joined-exclusion" => (
            "192.0.2.1:1",
            &["192.0.2.1:2", "192.0.2.2:1", "192.0.2.3:1"],
        ),
        "requested-size-capped-to-potential" => ("192.0.2.1:1", &["192.0.2.2:1", "192.0.2.3:1"]),
        "zero-options-default" => (
            "192.0.2.1:1",
            &["192.0.2.2:1", "192.0.2.3:1", "192.0.2.4:1", "192.0.2.5:1"],
        ),
        "negative-options-retained" => ("192.0.2.1:1", &["192.0.2.2:1", "192.0.2.3:1"]),
        "explicit-nonzero-options" => (
            "192.0.2.1:1",
            &[
                "192.0.2.2:1",
                "192.0.2.3:1",
                "192.0.2.4:1",
                "192.0.2.5:1",
                "192.0.2.6:1",
                "192.0.2.7:1",
            ],
        ),
        "exhausted-round-reset" => ("192.0.2.1:1", &["192.0.2.2:1"]),
        "ipv6-capture" => (
            "[2001:db8::1]:10",
            &["[2001:db8::1]:11", "[2001:db8::2]:12"],
        ),
        "hostname-capture" => (
            "node-a.example:10",
            &["node-a.example:11", "node-b.example:12"],
        ),
        _ => panic!("unrecognized Go oracle case: {name}"),
    };
    (local, seeds.iter().map(|s| (*s).to_owned()).collect())
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
        let (local, seeds) = inputs(&case.name);
        assert_eq!(
            case.bootstrap_hosts.len(),
            seeds.len() + usize::from(!seeds.iter().any(|s| s == local)),
            "{}",
            case.name
        );
        let options = Options::from_signed(
            case.timeout_nanos,
            case.max_join_duration_nanos,
            case.size,
            case.parallelism,
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
        assert_eq!(selector.effective_size, case.size);
        let joined = case.joined.unwrap_or_default();
        let mut chooser = First;
        let group = selector.select_group_with(&joined, &mut chooser);
        let pool = selector
            .preferred
            .iter()
            .chain(selector.non_preferred.iter())
            .cloned()
            .collect::<Vec<_>>();
        let mut available = pool;
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
        let cap = i64::from(selector.effective_size - joined.len() as i32)
            * i64::from(selector.options.parallelism);
        if cap >= 0 {
            assert!(group.len() <= cap as usize, "{} group cap", case.name);
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
