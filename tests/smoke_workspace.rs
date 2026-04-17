use std::process::Command;

#[test]
fn cli_help_includes_operator_workflows() {
    let output = Command::new(env!("CARGO_BIN_EXE_logpose-cli"))
        .arg("--help")
        .output()
        .expect("cli help should execute");

    assert!(output.status.success());

    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    assert!(stdout.contains("status"));
    assert!(stdout.contains("collection"));
    assert!(stdout.contains("record"));
    assert!(stdout.contains("query"));
    assert!(stdout.contains("inspect"));
    assert!(stdout.contains("interactive"));
    assert!(!stdout.contains("--interactive <MODE>"));
}
