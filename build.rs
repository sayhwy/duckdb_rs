use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("missing manifest dir"));
    let duckdb_dir = manifest_dir
        .parent()
        .expect("duckdb_rs must be next to duckdb")
        .join("duckdb");
    let fsst_dir = duckdb_dir.join("third_party").join("fsst");
    let duckdb_include_dir = duckdb_dir.join("src").join("include");

    println!("cargo:rerun-if-changed={}", fsst_dir.join("libfsst.cpp").display());
    println!("cargo:rerun-if-changed={}", fsst_dir.join("fsst.h").display());
    println!("cargo:rerun-if-changed={}", fsst_dir.join("libfsst.hpp").display());

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("missing OUT_DIR"));
    let wrapper = out_dir.join("fsst_wrapper.cpp");
    fs::write(
        &wrapper,
        r#"
#include "fsst.h"

extern "C" unsigned int duckdb_rs_fsst_import(duckdb_fsst_decoder_t *decoder, unsigned char *buf) {
    return duckdb_fsst_import(decoder, buf);
}

extern "C" size_t duckdb_rs_fsst_decompress(
    duckdb_fsst_decoder_t *decoder,
    size_t len_in,
    const unsigned char *str_in,
    size_t out_size,
    unsigned char *output
) {
    return duckdb_fsst_decompress(decoder, len_in, str_in, out_size, output);
}
"#,
    )
    .expect("failed to write fsst wrapper");

    cc::Build::new()
        .cpp(true)
        .std("c++17")
        .file(fsst_dir.join("libfsst.cpp"))
        .file(wrapper)
        .include(&fsst_dir)
        .include(&duckdb_include_dir)
        .warnings(false)
        .compile("duckdb_rs_fsst");
}
