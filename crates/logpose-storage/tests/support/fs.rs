/// A fresh directory named `logpose-{prefix}-…` under the system temp directory, removed when
/// the returned guard drops, also when the test panics. Keep the guard alive for as long as
/// anything uses the directory, reopens after a simulated crash included.
pub fn unique_temp_dir(prefix: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(&format!("logpose-{prefix}-"))
        .tempdir()
        .expect("temp dir should be created")
}
