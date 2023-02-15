use std::collections::HashMap;
use std::io::Read;
use std::io::Write;
use std::path::Path;

use flate2::Compression;
use flate2::GzBuilder;
use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha512};
use tracing::{debug, warn};
use wasmparser::{Parser, Chunk, Payload};

use fluvio_hub_protocol::{HubError, PackageMeta, Result};
use fluvio_hub_protocol::constants::{
    DEF_HUB_INIT_DIR, HUB_PACKAGE_META, HUB_SIGNFILE_BASE, HUB_MANIFEST_BLOB,
    HUB_PACKAGE_META_CLEAN,
};

use crate::PackageMetaExt;
use crate::keymgmt::{Keypair, PublicKey, Signature};
use crate::HubAccess;

/// assemble files into an unsigned fluvio package, a file will be created named
/// packagename-A.B.C.tar after signing it's called an ipkg
///
/// # Arguments
/// * pkgmeta: package-meta.yaml path
/// * outdir: optional output directory
pub fn package_assemble_and_sign<P: AsRef<Path>>(
    pkgmeta: P,
    access: &HubAccess,
    outdir: Option<&str>,
) -> Result<String> {
    let tarname = package_assemble(pkgmeta, outdir)?;
    let ipkgname = tar_to_ipkg(&tarname);
    let keypair = access.keypair()?;
    package_sign(&tarname, &keypair, &ipkgname)?;
    std::fs::remove_file(tarname)?;
    Ok(ipkgname)
}

fn tar_to_ipkg(fname: &str) -> String {
    let path = Path::new(fname);
    path.with_extension("ipkg").display().to_string()
}

/// assemble files into an unsigned fluvio package, a file will be created named
/// packagename-A.B.C.tar
///
/// # Arguments
/// * pkgmeta: package-meta.yaml path
/// * outdir: optional output directory
///
/// # Returns: staging tarfilename
fn package_assemble<P: AsRef<Path>>(pkgmeta: P, outdir: Option<&str>) -> Result<String> {
    debug!(target: "package_assemble", "opening");
    let pm = PackageMeta::read_from_file(pkgmeta)?;
    let mut pm_clean = pm.clone();
    pm_clean.manifest = Vec::new();

    let outdir = outdir.unwrap_or(DEF_HUB_INIT_DIR);
    let pkgtarname = outdir.to_string() + "/" + &pm.packagefile_name_unsigned();

    // crate manifest blob
    //todo: create in tmpdir/tmpfile?
    let manipath = Path::new(outdir).join(HUB_MANIFEST_BLOB);
    debug!(target: "package_assemble", "{pkgtarname}, creating temporary manifest blob");
    let tfio = std::fs::File::create(&manipath)?;
    let mut tfgz = GzBuilder::new()
        .filename(HUB_MANIFEST_BLOB)
        .write(tfio, Compression::default());
    let mut tf = tar::Builder::new(&mut tfgz);

    for fname in &pm.manifest {
        let fname = Path::new(fname);

        // in package, the source path is stripped
        let just_fname = fname
            .file_name()
            .ok_or_else(|| HubError::ManifestInvalidFile(fname.to_string_lossy().to_string()))?;
        tf.append_path_with_name(fname, just_fname)?;
        let just_fname = just_fname.to_string_lossy().to_string();
        pm_clean.manifest.push(just_fname);
    }

    tf.finish()?;
    drop(tf);
    tfgz.finish()?;
    debug!(target: "package_assemble", "{pkgtarname}, temporary manifest blob done");

    // write the clean temporary package file
    let clean_tmp = Path::new(outdir).join(HUB_PACKAGE_META_CLEAN);
    debug!(target: "package_assemble", "writing clean pkg meta");
    pm_clean.write(&clean_tmp)?;

    // add blob and yaml
    let pkgio = std::fs::File::create(&pkgtarname)?;
    let mut pkgtar = tar::Builder::new(pkgio);

    pkgtar.append_path_with_name(&clean_tmp, HUB_PACKAGE_META)?;
    pkgtar.append_path_with_name(&manipath, HUB_MANIFEST_BLOB)?;
    pkgtar.finish()?;

    debug!(target: "package_assemble", "removing temporary manifest blob and cleaned manifest");
    std::fs::remove_file(&manipath)?;
    std::fs::remove_file(&clean_tmp)?;

    Ok(pkgtarname)
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct FileSig {
    pub name: String,
    pub hash: String,
    pub len: u64,
    pub sig: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct PackageSignature {
    pub files: Vec<FileSig>,
    pub pubkey: String,
}

struct PackageSignatureBulder {
    signkey: Keypair,
    pkgsig: PackageSignature,
}

impl PackageSignatureBulder {
    fn new(key: &Keypair) -> Result<Self> {
        let builder = PackageSignatureBulder {
            signkey: key.clone_with_result()?,
            pkgsig: PackageSignature {
                files: Vec::new(),
                pubkey: key.public().to_hex(),
            },
        };
        Ok(builder)
    }

    fn sign(&mut self, fname: &str, buf: &[u8]) -> Result<()> {
        let sha = {
            let mut s = Sha512::new();
            s.update(buf);
            s.finalize()
        };
        let sig = self.signkey.sign(buf)?;

        let fsig = FileSig {
            name: String::from(fname),
            hash: hex::encode(sha),
            len: buf.len() as u64,
            sig: hex::encode(sig.to_bytes()),
        };
        self.pkgsig.files.push(fsig);
        Ok(())
    }
}

/// sign described in package-meta.yaml file / metafile
/// packagename-A.B.C.tar -> packagename-A.B.C.ipkg
/// a signature file is added with hash, len, signature of
/// every upper tier file (ie. the manifest file stays in a single file)
/// the signature is added as signature.0 for the first signature
/// but subsequent signatures are added as .1, .2 etc.
/// the later signatures will also sign the earlier signature files.
pub fn package_sign(in_pkgfile: &str, key: &Keypair, out_pkgfile: &str) -> Result<()> {
    // todo: add public key from credentials

    let file = std::fs::File::open(in_pkgfile)?;
    let mut ar = tar::Archive::new(file);
    let entries = ar.entries()?;

    let mut signedfile = tempfile::NamedTempFile::new()?;
    let mut signedpkg = tar::Builder::new(&signedfile);

    // scan through package, sign each file
    let mut sig = PackageSignatureBulder::new(key)?;
    let mut sig_number = 0u32;
    let tempdir = tempfile::tempdir()?;
    let mut num_files = 0u32;
    for file in entries {
        let mut fh = {
            if file.is_err() {
                continue;
            }
            file?
        };
        let fp = fh.path()?.to_path_buf();
        let fnamestr = fp.to_string_lossy().to_string();
        if let Some(fname) = fp.file_name() {
            if fname == HUB_SIGNFILE_BASE {
                sig_number += 1;
            }
        } else {
            warn!("Unexpected missing file name {:?}", fp);
        }
        if fh.unpack_in(tempdir.path())? {
            let tmpfile = tempdir.path().join(&fnamestr);
            let buf = std::fs::read(&tmpfile)?;
            // signedpkg
            sig.sign(&fnamestr, &buf)?;
            signedpkg.append_path_with_name(&tmpfile, fnamestr)?;
            num_files += 1;
        } else {
            warn!("Could not unpack file to temp {tempdir:?}/{fnamestr}");
        }
    }
    if num_files == 0 {
        return Err(HubError::PackageSigning(format!(
            "{in_pkgfile}: no files in package"
        )));
    }
    let buf = serde_json::to_string(&sig.pkgsig).map_err(|e| {
        warn!("signature serialization: {}", e);
        HubError::PackageSigning(format!("{in_pkgfile}: signature serialization fault"))
    })?;
    let signame = format!("{HUB_SIGNFILE_BASE}.{sig_number}");
    let tmpsigfile = tempdir.path().join(&signame);
    std::fs::write(&tmpsigfile, buf)?;
    signedpkg.append_path_with_name(&tmpsigfile, &signame)?;
    signedpkg.finish()?;
    drop(signedpkg);
    signedfile.flush()?;
    let sf_path = signedfile.path().to_path_buf();
    if let Err(e) = signedfile.persist(out_pkgfile) {
        warn!("{}, falling back to copy", e);
        std::fs::copy(sf_path, out_pkgfile).map_err(|e| {
            warn!("copy failure {}", e);
            HubError::PackageSigning(format!("{in_pkgfile}: fault creating signed package\n{e}"))
        })?;
    }
    Ok(())
}

/// extract sigfiles out of the package
pub fn package_getsigs_with_readio<R: std::io::Read>(
    readio: &mut R,
    pkgfile: &str,
) -> Result<HashMap<String, PackageSignature>> {
    let mut ar = tar::Archive::new(readio);
    let entries = ar.entries()?;

    let mut sigs = HashMap::new();
    let tempdir = tempfile::tempdir()?;
    for file in entries {
        let mut fh = {
            if file.is_err() {
                continue;
            }
            file?
        };
        let fp = fh.path()?.to_path_buf();
        let fnamestr = fp.to_string_lossy().to_string();
        let file_base = fp
            .file_stem()
            .ok_or_else(|| HubError::PackageVerify(format!("{pkgfile} bad filename")))?;
        let file_name = fp
            .file_name()
            .ok_or_else(|| HubError::PackageVerify(format!("{pkgfile} bad filename")))?;
        if fh.unpack_in(tempdir.path())? {
            let tmpfile = tempdir.path().join(file_name);
            let buf = std::fs::read(tmpfile)?;
            if file_base == HUB_SIGNFILE_BASE {
                // unpack the file, check if the signing key matches and validate it
                let ps: PackageSignature = serde_json::from_slice(&buf).map_err(|_| {
                    HubError::PackageVerify(format!("{pkgfile} could not decode sig"))
                })?;
                sigs.insert(fnamestr.to_string(), ps);
            }
        }
    }
    Ok(sigs)
}

// get a top level file from a package file
pub fn package_get_topfile<P: AsRef<Path>, T: AsRef<Path>>(
    pkgfile: P,
    topfile: T,
) -> Result<Vec<u8>> {
    let mut file = std::fs::File::open(pkgfile)?;
    package_get_topfile_with_readio(&mut file, topfile)
}

// get a top level file a generic reader trait obj
pub fn package_get_topfile_with_readio<R: std::io::Read, P: AsRef<Path>>(
    readio: &mut R,
    topfile: P,
) -> Result<Vec<u8>> {
    let topfile_p = topfile.as_ref();
    let mut ar = tar::Archive::new(readio);
    let entries = ar.entries()?;
    for file in entries {
        if file.is_err() {
            continue;
        }
        let mut f = file?;
        if let Ok(fp) = f.path() {
            if fp != topfile_p {
                continue;
            }
            let mut buf: Vec<u8> = Vec::new();
            f.read_to_end(&mut buf).map_err(|_| {
                HubError::PackageMissingFile(topfile_p.to_string_lossy().to_string())
            })?;
            return Ok(buf);
        }
    }
    Err(HubError::PackageMissingFile(
        topfile_p.to_string_lossy().to_string(),
    ))
}

/// extract files out of the package manifest
/// pkgfile: pkg-0.0.1.ipkg
/// filename: file in manifest
pub fn package_get_manifest_file<P: AsRef<Path>, T: AsRef<Path>>(
    pkgfile: P,
    filename: T,
) -> Result<Vec<u8>> {
    let mut file = std::fs::File::open(pkgfile)?;
    package_get_manifest_file_with_readio(&mut file, filename)
}

/// extract files out of the package manifest
/// pkgfile: pkg-0.0.1.ipkg
/// filename: file in manifest
pub fn package_get_manifest_file_with_readio<R: std::io::Read, P: AsRef<Path>>(
    readio: &mut R,
    filename: P,
) -> Result<Vec<u8>> {
    let manifest_buf = package_get_topfile_with_readio(readio, HUB_MANIFEST_BLOB)?;
    let manifest_io = std::io::Cursor::new(&manifest_buf);
    let gzio = GzDecoder::new(manifest_io);
    let mut ar = tar::Archive::new(gzio);
    let entries = ar.entries()?;
    let manifile = filename.as_ref();
    for file in entries {
        if file.is_err() {
            continue;
        }
        let mut f = file?;
        if let Ok(fp) = f.path() {
            if fp != manifile {
                continue;
            }
            let mut buf: Vec<u8> = Vec::new();
            f.read_to_end(&mut buf).map_err(|_| {
                HubError::PackageMissingFile(manifile.to_string_lossy().to_string())
            })?;
            return Ok(buf);
        }
    }
    Err(HubError::PackageMissingFile(
        manifile.to_string_lossy().to_string(),
    ))
}

/// Extracts WASM files from the package manifest ensuring these are valid
/// WASM files by parsing until encountering a discrepancy.
pub fn package_get_wasmfile_with_readio<R: std::io::Read, P: AsRef<Path>>(
    readio: &mut R,
    filename: P,
) -> Result<Vec<u8>> {
    let wasm_bytes = package_get_manifest_file_with_readio(readio, filename)?;

    validate_wasm_file(&wasm_bytes)?;
    Ok(wasm_bytes)
}

/// Validates a SmartModule's WASM payload to represent a valid WASM file
/// in the binary format (*.wasm).
fn validate_wasm_file(mut data: &[u8]) -> Result<()> {
    let mut parser = Parser::default();

    loop {
        match parser
            .parse(data, true)
            .map_err(|err| HubError::InvalidWasmFileEncountered(err.to_string()))?
        {
            // Given that file bytes are present, its not possible to meet
            // this state.
            Chunk::NeedMoreData(_) => unreachable!(),
            Chunk::Parsed { consumed, payload } => {
                if matches!(&payload, Payload::End(_)) {
                    // Reaches the EOF with success. At this point the
                    // whole file has been read and no errors has occured.
                    return Ok(());
                }

                // Keeps track of parsing offset.
                data = &data[consumed..];
            }
        };
    }
}

/// verify package signature. the pkgsig should contain the desired
/// public key to verify sgainst
fn package_verify_sig_from_readio<R: std::io::Read>(
    readio: &mut R,
    pkgfile: &str,
    pkgsig: &PackageSignature,
) -> Result<()> {
    let mut ar = tar::Archive::new(readio);
    let entries = ar.entries()?;

    let pubkey = PublicKey::from_hex(&pkgsig.pubkey)?;

    // track if a signed file was seen in the package and if it was verified
    struct IsVerified {
        fsig: FileSig,
        seen: bool,
        verify_ok: bool,
    }
    let mut vers: HashMap<String, IsVerified> = HashMap::new();
    for fsig in &pkgsig.files {
        let iv = IsVerified {
            fsig: fsig.clone(),
            seen: false,
            verify_ok: false,
        };
        vers.insert(fsig.name.clone(), iv);
    }
    let tempdir = tempfile::tempdir()?;
    for file in entries {
        let mut fh = {
            if file.is_err() {
                continue;
            }
            file?
        };
        let fp = fh.path()?.to_path_buf();
        let fnamestr = fp.to_string_lossy().to_string();
        let file_name = fp
            .file_name()
            .ok_or_else(|| HubError::PackageVerify(format!("{pkgfile} bad filename")))?;
        if vers.contains_key(&fnamestr) && fh.unpack_in(tempdir.path())? {
            let tmpfile = tempdir.path().join(file_name);
            let buf = std::fs::read(tmpfile)?;

            let mut iv = vers
                .get_mut(&fnamestr)
                .ok_or_else(|| HubError::PackageVerify(format!("{pkgfile} verify error")))?;
            iv.seen = true;
            let sigbytes = hex::decode(&iv.fsig.sig)
                .map_err(|_| HubError::PackageVerify(format!("{pkgfile} key decode error")))?;
            let signature = Signature::from_bytes(&sigbytes)
                .map_err(|_| HubError::PackageVerify(format!("{pkgfile} key decode error")))?;
            iv.verify_ok = pubkey.verify(&buf, &signature).is_ok();
        }
    }
    // not using any or all because of a desire to generate a full log of any
    // failed per file validation
    let any_bad = vers.iter().fold(false, |abad, (k, v)| {
        let pass = v.seen && v.verify_ok;
        if !pass {
            warn!("Package {pkgfile} file {k} failed verification");
        }
        abad || !pass
    });
    if any_bad {
        Err(HubError::PackageVerify("failed verify".into()))
    } else {
        Ok(())
    }
}

/// sign described in package-meta.yaml file / metafile
/// and add signature file to the package tar
/// on download the code would be looking for the signature with the hub public key
/// on upload the caller would generally be looking at owner signature
pub fn package_verify(pkgfile: &str, pubkey: &PublicKey) -> Result<()> {
    let mut file = std::fs::File::open(pkgfile)?;
    package_verify_with_readio(&mut file, pkgfile, pubkey)
}

pub fn package_verify_with_readio<R: std::io::Read + std::io::Seek>(
    readio: &mut R,
    pkgfile: &str,
    pubkey: &PublicKey,
) -> Result<()> {
    // locate sig that matches the public key in cred
    let sigs = package_getsigs_with_readio(readio, pkgfile)?;
    let string_pubkey = hex::encode(pubkey.to_bytes());
    let sig = sigs
        .iter()
        .find_map(|rec| {
            let (_fname, pkgsig) = rec;
            (pkgsig.pubkey == string_pubkey).then_some(pkgsig)
        })
        .ok_or_else(|| HubError::PackageVerify(format!("{pkgfile} no signature with given key")))?;

    readio.rewind()?;
    package_verify_sig_from_readio(readio, pkgfile, sig)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs::read;

    use super::*;
    use crate::package_get_meta;

    const PKG_SIGN_PUBKEY: &str = "tests/hubutil_package_sign-pubkey.pem";

    fn rust_log_init() {
        let trs = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO);
        if std::env::var("RUST_LOG").is_ok() {
            trs.with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                .init();
        } else {
            trs.init();
        }
    }

    #[test]
    fn hubutil_serialize_package_meta() {
        let pm = PackageMeta {
            version: "1.0".into(),
            group: "groupname".into(),
            ..PackageMeta::default()
        };

        let serialized = serde_yaml::to_string(&pm).expect("serialize fail");
        println!("{}", &serialized);

        let pm_dser: PackageMeta = serde_yaml::from_str(&serialized).expect("deseralize fail");
        assert_eq!(pm, pm_dser);
    }

    #[test]
    fn hubutil_package_get_meta() {
        let testfile: &str = "tests/apackage/package-meta.yaml";
        let pkgfile =
            package_assemble(testfile, Some("tests/apackage")).expect("package assemble fail");

        let pm_from_inner =
            package_get_meta(pkgfile).expect("couldn't get meta file from package file");

        // we expect pm_from_file manifest paths to be source paths
        // but inside the package they're stripped. So remap to strip
        // the paths.
        let mut pm_from_file =
            PackageMeta::read_from_file(testfile).expect("couldn't load test package-meta file");
        let stripped_manifest = pm_from_file
            .manifest
            .iter()
            .map(|fpath| {
                Path::new(fpath)
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .to_string()
            })
            .collect();
        pm_from_file.manifest = stripped_manifest;

        assert_eq!(pm_from_file, pm_from_inner);
    }

    #[test]
    fn hubutil_package_assemble() {
        rust_log_init();
        let testfile: &str = "tests/apackage/package-meta.yaml";
        let res = package_assemble(testfile, Some("tests"));
        assert!(res.is_ok());
        let outpath = std::path::Path::new("tests/example-0.0.1.tar");
        assert!(outpath.exists());
    }

    #[test]
    fn hubutil_package_sign() -> Result<()> {
        const UNSIGNED_PKG_FILE: &str = "tests/example-0.0.1.tar";
        const SIGNED_PKG_FILE: &str = "tests/example-0.0.1.ipkg";
        let unsigned = std::path::Path::new(UNSIGNED_PKG_FILE);
        if !unsigned.exists() {
            hubutil_package_assemble();
        }
        assert!(unsigned.exists());
        let keypair = Keypair::new().expect("failed to create keypair");
        // save key for post analysis if needed
        keypair.write_keypair("tests/hubutil_package_sign-keypair.pem")?;
        keypair.public().write(PKG_SIGN_PUBKEY)?;
        let _res = package_sign(UNSIGNED_PKG_FILE, &keypair, SIGNED_PKG_FILE);

        let outpath = std::path::Path::new(SIGNED_PKG_FILE);
        assert!(outpath.exists(), "no signed file generated");
        Ok(())
    }

    #[test]
    fn hubutil_package_verify() -> Result<()> {
        const SIGNED_PKG_FILE: &str = "tests/static-example-0.0.1.ipkg";
        const PKG_SIGN_PUBKEY: &str = "tests/static-example-pubkey.pem";

        let signedpkg = std::path::Path::new(SIGNED_PKG_FILE);
        assert!(signedpkg.exists());

        let pubkey = PublicKey::read_from_file(PKG_SIGN_PUBKEY)?;
        package_verify(SIGNED_PKG_FILE, &pubkey)?;
        Ok(())
    }

    #[test]
    fn hubutil_package_get_manifest_file() {
        const SIGNED_PKG_FILE: &str = "tests/static-example-0.0.1.ipkg";
        let res_fbytes = package_get_manifest_file(SIGNED_PKG_FILE, "module.wasm");
        assert!(res_fbytes.is_ok());
        let fbytes = res_fbytes.unwrap();
        assert!(!fbytes.is_empty());

        let res_of_missing_file = package_get_manifest_file(SIGNED_PKG_FILE, "does-not-exist");
        assert!(res_of_missing_file.is_err());
    }

    #[test]
    fn validates_smartmodule_wasm_binary() {
        let wasm_bytes =
            read("tests/sm_ok.wasm").expect("Failed to find 'sm_ok.wasm' file for tests");
        let result = validate_wasm_file(wasm_bytes.as_slice());

        assert!(result.is_ok());
    }

    #[test]
    fn validate_input_to_be_wasm_file() {
        let wasm_bytes = read("tests/hub_package_meta_rw_test.yaml")
            .expect("Failed to find 'hub_package_meta_rw_test.yaml' file for tests");
        let result = validate_wasm_file(wasm_bytes.as_slice());

        assert!(result.is_err());
    }

    #[test]
    fn validates_smartmodule_wasm_binary_corrupt() {
        let wasm_bytes = read("tests/sm_need_more_data.wasm")
            .expect("Failed to find 'sm_need_more_data.wasm' file for tests");
        let result = validate_wasm_file(wasm_bytes.as_slice());

        assert!(result.is_err());
    }
}
