use std::{
    env,
    error::Error,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

fn main() -> Result<(), Box<dyn Error>> {
    #[cfg(target_os = "windows")]
    winfsp_wrs_build::build();

    let crate_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let workspace_root = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .ok_or("failed to resolve workspace root from Cargo manifest directory")?;
    let proto_root = workspace_root.join("proto");
    let proto_file = proto_root.join("filmuvfs/catalog/v1/catalog.proto");

    println!("cargo:rerun-if-changed={}", proto_file.display());
    println!("cargo:rerun-if-changed=build.rs");

    #[cfg(target_os = "windows")]
    ensure_local_winfsp_import_lib(workspace_root)?;

    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    env::set_var("PROTOC", protoc);

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[proto_file], &[proto_root])?;

    Ok(())
}

#[cfg(target_os = "windows")]
fn ensure_local_winfsp_import_lib(workspace_root: &Path) -> Result<(), Box<dyn Error>> {
    let install_dir = resolve_winfsp_install_dir()?;
    let arch = match env::var("CARGO_CFG_TARGET_ARCH")?.as_str() {
        "x86_64" => "x64",
        "x86" => "x86",
        "aarch64" => "a64",
        other => {
            return Err(
                format!("unsupported Windows target architecture for WinFSP: {other}").into(),
            )
        }
    };

    let local_root = workspace_root.join(".winfsp");
    let local_lib_dir = local_root.join("lib");
    let local_lib_path = local_lib_dir.join(format!("winfsp-{arch}.lib"));
    let dll_path = install_dir.join("bin").join(format!("winfsp-{arch}.dll"));

    println!("cargo:rerun-if-env-changed=WINFSP_INSTALL_DIR");
    println!("cargo:rerun-if-changed={}", dll_path.display());

    fs::create_dir_all(&local_lib_dir)?;
    if !local_lib_path.exists() {
        synthesize_winfsp_import_lib(&dll_path, &local_lib_path, &local_root)?;
    }

    println!("cargo:rustc-link-search=native={}", local_lib_dir.display());
    Ok(())
}

#[cfg(target_os = "windows")]
fn resolve_winfsp_install_dir() -> Result<PathBuf, Box<dyn Error>> {
    if let Ok(value) = env::var("WINFSP_INSTALL_DIR") {
        let path = PathBuf::from(value);
        if path.exists() {
            return Ok(path);
        }
    }

    let output = Command::new("reg")
        .args([
            "query",
            r"HKLM\SOFTWARE\WOW6432Node\WinFsp",
            "/v",
            "InstallDir",
        ])
        .output()?;

    if !output.status.success() {
        return Err("failed to query WinFSP install directory from registry".into());
    }

    let stdout = String::from_utf8(output.stdout)?;
    for line in stdout.lines() {
        if line.contains("InstallDir") {
            if let Some((_, raw_path)) = line.split_once("REG_SZ") {
                let candidate = PathBuf::from(raw_path.trim());
                if candidate.exists() {
                    return Ok(candidate);
                }
            }
        }
    }

    Err("WinFSP install directory not found in registry".into())
}

#[cfg(target_os = "windows")]
fn synthesize_winfsp_import_lib(
    dll_path: &Path,
    lib_path: &Path,
    temp_root: &Path,
) -> Result<(), Box<dyn Error>> {
    if !dll_path.exists() {
        return Err(format!("WinFSP DLL not found at {}", dll_path.display()).into());
    }

    let tools_dir = resolve_msvc_tools_dir()?;
    let dumpbin = tools_dir.join("dumpbin.exe");
    let libexe = tools_dir.join("lib.exe");
    let def_path = temp_root.join(
        dll_path
            .file_stem()
            .ok_or("missing WinFSP DLL file stem")?
            .to_string_lossy()
            .to_string()
            + ".def",
    );

    let dump = Command::new(&dumpbin)
        .arg("/exports")
        .arg(dll_path)
        .output()?;
    if !dump.status.success() {
        return Err(format!("dumpbin failed for {}", dll_path.display()).into());
    }

    let stdout = String::from_utf8(dump.stdout)?;
    let mut exports = Vec::new();
    for line in stdout.lines() {
        let trimmed = line.trim();
        let parts = trimmed.split_whitespace().collect::<Vec<_>>();
        if parts.len() >= 4
            && parts[0].chars().all(|c| c.is_ascii_digit())
            && parts[1].chars().all(|c| c.is_ascii_hexdigit())
            && parts[2].chars().all(|c| c.is_ascii_hexdigit())
        {
            exports.push(parts[3].to_owned());
        }
    }

    if exports.is_empty() {
        return Err(format!("no exported symbols were found in {}", dll_path.display()).into());
    }

    let lib_name = dll_path
        .file_name()
        .ok_or("missing WinFSP DLL file name")?
        .to_string_lossy();
    let mut def_content = String::new();
    def_content.push_str("LIBRARY ");
    def_content.push_str(&lib_name);
    def_content.push_str("\nEXPORTS\n");
    for export in exports {
        def_content.push_str(&export);
        def_content.push('\n');
    }
    fs::write(&def_path, def_content)?;

    let status = Command::new(&libexe)
        .arg(format!("/def:{}", def_path.display()))
        .arg(format!("/out:{}", lib_path.display()))
        .arg(match env::var("CARGO_CFG_TARGET_ARCH")?.as_str() {
            "x86_64" => "/machine:x64",
            "x86" => "/machine:x86",
            "aarch64" => "/machine:arm64",
            other => {
                return Err(
                    format!("unsupported Windows target architecture for lib.exe: {other}").into(),
                )
            }
        })
        .status()?;

    if !status.success() {
        return Err(format!("lib.exe failed while generating {}", lib_path.display()).into());
    }

    Ok(())
}

#[cfg(target_os = "windows")]
fn resolve_msvc_tools_dir() -> Result<PathBuf, Box<dyn Error>> {
    if let Ok(value) = env::var("VCToolsInstallDir") {
        let tools_dir = PathBuf::from(value).join("bin").join("Hostx64").join("x64");
        if tools_dir.join("lib.exe").exists() && tools_dir.join("dumpbin.exe").exists() {
            return Ok(tools_dir);
        }
    }

    let program_files =
        env::var("ProgramFiles").unwrap_or_else(|_| String::from(r"C:\Program Files"));
    let editions = ["Enterprise", "Professional", "Community", "BuildTools"];
    let mut candidates = Vec::new();

    for edition in editions {
        let msvc_root = PathBuf::from(&program_files)
            .join("Microsoft Visual Studio")
            .join("2022")
            .join(edition)
            .join("VC")
            .join("Tools")
            .join("MSVC");
        if !msvc_root.exists() {
            continue;
        }

        for entry in fs::read_dir(&msvc_root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let tools_dir = entry.path().join("bin").join("Hostx64").join("x64");
            if tools_dir.join("lib.exe").exists() && tools_dir.join("dumpbin.exe").exists() {
                candidates.push(tools_dir);
            }
        }
    }

    candidates.sort();
    candidates
        .pop()
        .ok_or_else(|| "failed to locate MSVC lib.exe and dumpbin.exe".into())
}
