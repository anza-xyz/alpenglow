use std::path::PathBuf;
use std::process::Command;
use std::{env, fs};

fn cargo_build_sbf2(manifest_path: &PathBuf) {
    println!("cargo:warning=checking for cargo-build-sbf");

    let mut cargo_build_sbf = Command::new("cargo-build-sbf");
    if cargo_build_sbf.arg("--help").output().is_ok() {
        println!("cargo:warning=cargo-build-sbf found");
    } else {
        println!("cargo:warning=cargo-build-sbf not found");

        println!("cargo:warning=installing cargo-build-sbf");
        let install_command_output = Command::new("sh")
            .arg("-c")
            .arg("curl -sSfL https://release.anza.xyz/edge/install | sh -s -- --data-dir . --no-modify-path v2.1.16")
            .output()
            .expect("failed to execute install command");

        println!(
            "cargo:warning=cargo-build-sbf {}",
            String::from_utf8_lossy(&install_command_output.stderr)
        );

        cargo_build_sbf = Command::new("./active_release/bin/cargo-build-sbf");
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

    let output =
        cargo_build_sbf
            .arg("--manifest-path")
            .arg(manifest_path.to_str().unwrap_or_else(|| {
                panic!("Couldn't fetch manifest path as str: {:?}", &manifest_path)
            }))
            .output()
            .expect("failed to run cargo-build-sbf");

    if output.status.success() {
        let version = String::from_utf8_lossy(&output.stdout);
        println!("cargo:warning=cargo-build-sbf output: {}", version);
    } else {
        eprintln!("cargo:warning=failed to get cargo-build-sbf output");
    }
}

// fn cargo_build_sbf() -> Command {
//     if Command::new("cargo-build-sbf")
//         .arg("--help")
//         .output()
//         .is_err()
//     {
//         build_print::custom_println!(
//             "[build-alpenglow-vote]",
//             green,
//             "installing cargo-build-sbf"
//         );
//
//         let install_command_output = Command::new("sh")
//             .arg("-c")
//             .arg("curl -sSfL https://release.anza.xyz/edge/install | sh -s -- --data-dir . --no-modify-path v2.1.16")
//             .output()
//             .expect("failed to execute install command");
//
//         if !install_command_output.stderr.is_empty() {
//             build_print::custom_println!(
//                 "[build-alpenglow-vote]",
//                 green,
//                 "{}",
//                 String::from_utf8_lossy(&install_command_output.stderr)
//             );
//         }
//     }
//
//     Command::new("./active_release/bin/cargo-build-sbf")
// }

fn build_and_fetch_shared_object_path(manifest_path: &PathBuf) -> (PathBuf, PathBuf) {
    cargo_build_sbf2(manifest_path);
    // let result =
    //     cargo_build_sbf()
    //         .arg("--manifest-path")
    //         .arg(manifest_path.to_str().unwrap_or_else(|| {
    //             panic!("Couldn't fetch manifest path as str: {:?}", &manifest_path)
    //         }))
    //         .output()
    //         .expect("Couldn't run cargo-build-sbf");

    // if !result.stderr.is_empty() {
    //     build_print::custom_println!(
    //         "[build-alpenglow-vote]",
    //         green,
    //         "{}",
    //         String::from_utf8_lossy(&result.stderr)
    //     );
    // }

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

    build_print::println!("manifest_path: {}", manifest_path.display());
    build_print::println!("manifest_path exists: {}", manifest_path.exists());

    for entry in walkdir::WalkDir::new(manifest_path.parent().unwrap()) {
        build_print::println!("Entry :: {}", entry.unwrap().path().display());
    }

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
