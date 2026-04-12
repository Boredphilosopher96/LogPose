//! Build script for generating gRPC bindings.

fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc should exist");
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc);

    println!("cargo:rerun-if-changed=../../proto/logpose/v1/logpose.proto");

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos_with_config(
            config,
            &["../../proto/logpose/v1/logpose.proto"],
            &["../../proto"],
        )
        .expect("protobuf compilation should succeed");
}
