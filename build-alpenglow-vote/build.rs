use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::process::Stdio;

fn cargo_build_sbf_exists() -> bool {
    let exists = Command::new("cargo")
        .arg("build-sbf")
        .arg("--help")
        .status()
        .expect("Error 1")
        .success();

    build_print::custom_println!(
        "[build-alpenglow-vote]",
        green,
        "cargo build-sbf works: {}",
        exists
    );

    exists
}

fn uninstall_solana_cli() {
    build_print::custom_println!(
        "[build-alpenglow-vote]",
        yellow,
        "Uninstalling (potentially) existing Solana CLI components, since cargo build-sbf doesn't work.",
    );

    // rm -rf potentially existing Solana CLI
    let home_dir = env::var("HOME").unwrap();

    for glob in [
        ".cache/solana",
        ".local/share/solana",
        ".cargo/bin/solana*",
        ".cargo/bin/cargo-build-sbf",
        ".cargo/bin/solana-install",
    ] {
        for path in glob::glob(&format!("{home_dir}/{glob}")).unwrap().flatten() {
            let _ = fs::remove_file(path);
        }
    }
}

fn install_solana_cli() {
    build_print::custom_println!(
        "[build-alpenglow-vote]",
        green,
        "Installing Solana CLI components.",
    );

    // curl --proto '=https' --tlsv1.2 -sSfL https://solana-install.solana.workers.dev | bash
    let mut install_cli_script = Command::new("curl")
        .arg("--proto")
        .arg("=https")
        .arg("--tlsv1.2")
        .arg("-sSfL")
        .arg("https://release.anza.xyz/stable/install")
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    install_cli_script
        .wait()
        .expect("Couldn't fetch Solana CLI install script.");

    let mut bash = Command::new("bash")
        .stdin(Stdio::from(install_cli_script.stdout.unwrap()))
        .spawn()
        .unwrap();

    if !bash.wait().expect("Couldn't install Solana CLI").success() {
        panic!("Solana CLI install not successful.");
    }
}

fn maybe_install_cargo_sbf() {
    if cargo_build_sbf_exists() {
        return;
    }

    uninstall_solana_cli();
    install_solana_cli();

    if !cargo_build_sbf_exists() {
        panic!("Couldn't get cargo build-sbf to work after installing Solana CLI!");
    }
}

fn get_cargo_path() -> PathBuf {
    PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())
        .parent()
        .unwrap()
        .to_owned()
        .join("cargo")
}

fn build_and_fetch_shared_object_path(manifest_path: &PathBuf) -> (PathBuf, PathBuf) {
    // Run cargo build-sbf
    maybe_install_cargo_sbf();

    let new_path = format!(
        "/var/lib/buildkite-agent/.local/share/solana/install/active_release/bin:{}",
        env::var("PATH").unwrap()
    );

    if !Command::new(get_cargo_path())
        .arg("build-sbf")
        .arg("--manifest-path")
        .arg(
            manifest_path.to_str().unwrap_or_else(|| {
                panic!("Couldn't fetch manifest path as str: {:?}", &manifest_path)
            }),
        )
        .env("PATH", new_path)
        .status()
        .unwrap_or_else(|err| {
            panic!(
                "Couldn't build alpenglow-vote with manifest path: {:?}. Error:\n{}",
                &manifest_path, err
            )
        })
        .success()
    {
        panic!(
            "cargo build-sbf failed for manifest path: {:?}",
            &manifest_path
        );
    }

    // Return the path to the shared object
    let src_dir = manifest_path.parent().unwrap().to_owned();
    let so_path = src_dir
        .join("target")
        .join("deploy")
        .join("alpenglow_vote.so");

    (src_dir, so_path)
}

fn generate_github_rev(rev: &str) -> (PathBuf, PathBuf) {
    // Form the glob that searches for the git repo's manifest path under ~/.cargo/git/checkouts
    let git_checkouts_path = PathBuf::from(env::var("CARGO_HOME").unwrap())
        .join("git")
        .join("checkouts");

    let glob_str = format!(
        "{}/alpenglow-vote-*/{}/Cargo.toml",
        git_checkouts_path.to_str().unwrap(),
        rev
    );

    // Find the manifest path
    let manifest_path = glob::glob(&glob_str)
        .unwrap_or_else(|_| panic!("Failed to read glob: {}", &glob_str))
        .filter_map(Result::ok)
        .next()
        .unwrap_or_else(|| {
            panic!(
                "Couldn't find path to git repo with glob {} and revision {}",
                &glob_str, rev
            )
        });

    build_and_fetch_shared_object_path(&manifest_path)
}

fn generate_local_checkout(path: &str) -> (PathBuf, PathBuf) {
    let err = || {
        format!("Local checkout path must be of the form: /x/y/z/alpenglow-vote-project-path/program. In particular, alpenglow-vote-project-path is the local checkout, which might typically just be called alpenglow-vote. Current checkout path: {}", path)
    };
    let path = PathBuf::from(path);

    // Ensure that path ends with "program"
    if path
        .file_name()
        .and_then(|p| p.to_str())
        .unwrap_or_else(|| panic!("{}", err()))
        != "program"
    {
        panic!("{}", err());
    }

    // If this is a relative path, then make it absolute by determining the relative path with
    // respect to the project directory, and not the current CARGO_MANIFEST_DIR.
    let path = if path.is_relative() {
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())
            .parent()
            .unwrap()
            .to_owned()
            .join(path)
    } else {
        path
    };

    // Turn the path into an absolute path
    let path = std::path::absolute(path).unwrap();

    let manifest_path = path.parent().unwrap().to_owned().join("Cargo.toml");

    build_and_fetch_shared_object_path(&manifest_path)
}

fn main() {
    // Get the project's Cargo.toml
    let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let project_cargo_toml_path = PathBuf::from(&cargo_manifest_dir)
        .join("..")
        .join("Cargo.toml");

    // Parse the Cargo file.
    let project_cargo_toml_contents =
        fs::read_to_string(&project_cargo_toml_path).expect("Couldn't read root Cargo.toml.");

    let project_cargo_toml = project_cargo_toml_contents
        .parse::<toml::Value>()
        .expect("Couldn't parse root Cargo.toml into a valid toml::Value.");

    // Find alpenglow-vote
    let workspace_dependencies = &project_cargo_toml["workspace"]["dependencies"];

    let err = "alpenglow-vote must either be of form: (1) if you're trying to fetch from a git repo: { git = \"...\", rev = \"...\" } or (2) if you're trying to use a local checkout of alpenglow-vote : { path = \"...\" }";

    let alpenglow_vote = workspace_dependencies
        .get("alpenglow-vote")
        .expect("Couldn't find alpenglow-vote under workspace.dependencies in root Cargo.toml.")
        .as_table()
        .expect(err);

    // Are we trying to build alpenglow-vote from Github or a local checkout?
    let (src_path, so_src_path) =
        if alpenglow_vote.contains_key("git") && alpenglow_vote.contains_key("rev") {
            build_print::custom_println!(
                "Compiling",
                green,
                "spl_alpenglow-vote.so: building from github rev: {:?}",
                &alpenglow_vote
            );
            generate_github_rev(alpenglow_vote["rev"].as_str().unwrap())
        } else if alpenglow_vote.contains_key("path") {
            build_print::custom_println!(
                "Compiling",
                green,
                "spl_alpenglow-vote.so: building from local checkout: {:?}",
                &alpenglow_vote
            );
            generate_local_checkout(alpenglow_vote["path"].as_str().unwrap())
        } else {
            panic!("{}", err);
        };

    // Copy the .so to project_dir/target/tmp/
    let so_dest_path = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())
        .parent()
        .unwrap()
        .to_owned()
        .join("target")
        .join("alpenglow-vote-so")
        .join("spl_alpenglow-vote.so");

    fs::create_dir_all(so_dest_path.parent().unwrap())
        .unwrap_or_else(|_| panic!("Couldn't create path: {:?}", &so_dest_path));

    fs::copy(&so_src_path, &so_dest_path).unwrap_or_else(|err| {
        panic!(
            "Couldn't copy alpenglow_vote from {:?} to {:?}:\n{}",
            &so_src_path, &so_dest_path, err
        )
    });

    build_print::custom_println!(
        "[build-alpenglow-vote]",
        green,
        "spl_alpenglow-vote.so: successfully built alpenglow_vote! Copying {} -> {}",
        so_src_path.display(),
        so_dest_path.display(),
    );

    // Save the destination path as an environment variable that can later be invoked in Rust code
    println!(
        "cargo:rustc-env=ALPENGLOW_VOTE_SO_PATH={}",
        so_dest_path.display()
    );

    // Re-build if we detect a change in either (1) the alpenglow-vote src or (2) this build script
    println!("cargo::rerun-if-changed={:?}", src_path);
    println!("cargo::rerun-if-changed=build.rs");
}
