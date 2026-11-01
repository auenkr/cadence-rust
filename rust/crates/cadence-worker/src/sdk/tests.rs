//! Replay tests for the SDK runtime.

#[test]
fn converter_roundtrip() {
    let v = super::converter::encode(&"x").unwrap();
    assert_eq!(v, b"\"x\"\n");
    let s: String = super::converter::decode(&v).unwrap();
    assert_eq!(s, "x");
}
