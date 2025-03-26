fn main() {
    println!("cargo:warning=checking for cargo-build-sbf");

    let mut cargo_build_sbf = std::process::Command::new("cargo-build-sbf");
    if cargo_build_sbf.arg("--help").output().is_ok() {
        println!("cargo:warning=cargo-build-sbf found");
    } else {
        println!("cargo:warning=cargo-build-sbf not found");

        println!("cargo:warning=installing cargo-build-sbf");
        let install_command_output = std::process::Command::new("sh")
            .arg("-c")
            .arg("curl -sSfL https://release.anza.xyz/edge/install | sh -s -- --data-dir . --no-modify-path v2.1.16")
            .output()
            .expect("failed to execute install command");

        println!(
            "cargo:warning=cargo-build-sbf {}",
            String::from_utf8_lossy(&install_command_output.stderr)
        );

        cargo_build_sbf = std::process::Command::new("./active_release/bin/cargo-build-sbf");
    }

    let output = cargo_build_sbf
        .arg("--version")
        .output()
        .expect("failed to run cargo-build-sbf");

    if output.status.success() {
        let version = String::from_utf8_lossy(&output.stdout);
        println!("cargo:warning=cargo-build-sbf version: {}", version);
    } else {
        eprintln!("cargo:warning=failed to get cargo-build-sbf version");
    }
}
