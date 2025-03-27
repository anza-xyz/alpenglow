use std::path::Path;

fn main() {
    if std::env::var("CLIPPY_ARGS").is_ok() {
        println!("cargo:warning=Build script detected Clippy run.");
        return;
    }

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
        println!("cargo:warning=failed to get cargo-build-sbf version");
    }

    let noop_so_path = Path::new("./noop.so");
    if noop_so_path.exists() {
        std::fs::remove_file(noop_so_path).expect("failed to remove ./noop.so");
        println!("cargo:warning=removed existing noop.so");
    }

    let mut program_path =
        Path::new("../platform-tools-sdk/cargo-build-sbf/tests/crates/noop/Cargo.toml");
    if !program_path.exists() {
        program_path =
            Path::new("/solana/platform-tools-sdk/cargo-build-sbf/tests/crates/noop/Cargo.toml");
    }
    println!(
        "cargo:warning=using program path: {}",
        program_path.display()
    );

    let output = std::process::Command::new(cargo_build_sbf.get_program())
        .arg("--manifest-path")
        .arg(program_path)
        .arg("--sbf-out-dir")
        .arg(".")
        .output()
        .expect("failed to run cargo-build-sbf");
    if !output.status.success() {
        println!(
            "cargo:warning=stderr: {}\nstdout: {}",
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout)
        );
        panic!("failed to run cargo-build-sbf");
    }

    if noop_so_path.exists() {
        println!("cargo:warning=./noop.so exists");
        let output = std::process::Command::new("ls")
            .arg("-l")
            .arg(noop_so_path)
            .output()
            .expect("failed to run ls");
        println!(
            "cargo:warning=ls -l ./noop.so: {}",
            String::from_utf8_lossy(&output.stdout)
        );
    } else {
        println!("cargo:warning=./noop.so does not exist");
    }
}
