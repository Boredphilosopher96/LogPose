use logpose_client::LogPoseClient;
use logpose_config::LogPoseConfig;
use logpose_core::AppState;
use std::{
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    sync::{Arc, mpsc},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::runtime::Runtime;

pub struct TestServerFixture {
    pub temp_root: PathBuf,
    pub rest_addr: SocketAddr,
    pub grpc_addr: SocketAddr,
    runtime: Runtime,
    server: tokio::task::JoinHandle<anyhow::Result<()>>,
}

const STARTUP_ATTEMPTS: usize = 5;

#[allow(dead_code)]
impl TestServerFixture {
    pub fn spawn(node_name: &str) -> Self {
        Self::spawn_with_role(node_name, logpose_types::NodeRole::Combined)
    }

    pub fn spawn_with_role(node_name: &str, node_role: logpose_types::NodeRole) -> Self {
        Self::spawn_with_listener_hosts(node_name, node_role, "127.0.0.1", "127.0.0.1")
    }

    pub fn spawn_with_listener_hosts(
        node_name: &str,
        node_role: logpose_types::NodeRole,
        rest_host: &str,
        grpc_host: &str,
    ) -> Self {
        let temp_root = unique_temp_dir(node_name);
        let storage_root = temp_root.join("data");
        for attempt in 0..STARTUP_ATTEMPTS {
            let rest_listener = bind_listener(rest_host);
            let grpc_listener = bind_listener(grpc_host);
            let rest_addr = rest_listener
                .local_addr()
                .expect("rest listener should expose addr");
            let grpc_addr = grpc_listener
                .local_addr()
                .expect("grpc listener should expose addr");
            let runtime = Runtime::new().expect("runtime should build");
            let (ready_tx, ready_rx) = mpsc::channel();
            let state = Arc::new(AppState::new(LogPoseConfig {
                node_name: node_name.to_owned(),
                node_role,
                rest_host: rest_host.to_owned(),
                rest_port: rest_addr.port(),
                grpc_host: grpc_host.to_owned(),
                grpc_port: grpc_addr.port(),
                log_filter: "info".to_owned(),
                storage_root: storage_root.clone(),
                auth_token: None,
            }));
            let mut server = runtime.spawn(async move {
                let rest_listener = tokio::net::TcpListener::from_std(rest_listener)
                    .expect("rest listener should become tokio listener");
                let grpc_listener = tokio::net::TcpListener::from_std(grpc_listener)
                    .expect("grpc listener should become tokio listener");
                let rest_state = Arc::clone(&state);
                let grpc_state = Arc::clone(&state);
                let rest_ready_tx = ready_tx.clone();
                let grpc_ready_tx = ready_tx;
                tokio::try_join!(
                    async move {
                        let _ = rest_ready_tx.send("rest");
                        logpose_api_rest::serve_with_listener(rest_state, rest_listener)
                            .await
                            .map_err(anyhow::Error::from)
                    },
                    async move {
                        let _ = grpc_ready_tx.send("grpc");
                        logpose_api_grpc::serve_with_listener(grpc_state, grpc_listener)
                            .await
                            .map_err(|error| anyhow::anyhow!(error.to_string()))
                    }
                )?;
                Ok(())
            });

            match wait_for_server(&runtime, &mut server, &ready_rx, &[rest_addr, grpc_addr]) {
                Ok(()) => {
                    return Self {
                        temp_root,
                        rest_addr,
                        grpc_addr,
                        runtime,
                        server,
                    };
                }
                Err(_error) if attempt + 1 < STARTUP_ATTEMPTS => {
                    if !server.is_finished() {
                        server.abort();
                        let _ = runtime.block_on(async { (&mut server).await });
                    }
                }
                Err(error) => {
                    assert!(
                        attempt + 1 < STARTUP_ATTEMPTS,
                        "failed to start test server after {STARTUP_ATTEMPTS} attempts: {error}"
                    );
                }
            }
        }

        unreachable!("startup attempts should either succeed or panic")
    }

    pub fn rest_endpoint(&self) -> String {
        format!("http://{}", self.rest_addr)
    }

    pub fn grpc_endpoint(&self) -> String {
        format!("http://{}", self.grpc_addr)
    }

    pub fn run_cli<const N: usize>(&self, args: [&str; N]) -> Output {
        let output = self.run_cli_raw(args);
        assert!(
            output.status.success(),
            "command failed with stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }

    pub fn run_cli_args(&self, args: &[&str]) -> Output {
        let output = self.run_cli_raw_args(args);
        assert!(
            output.status.success(),
            "command failed with stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }

    pub fn run_cli_json(&self, args: &[&str]) -> Output {
        let mut prefixed = Vec::with_capacity(args.len() + 1);
        prefixed.push("--json");
        prefixed.extend_from_slice(args);
        self.run_cli_args(&prefixed)
    }

    pub fn run_cli_expect_failure<const N: usize>(&self, args: [&str; N]) -> Output {
        let output = self.run_cli_raw(args);
        assert!(
            !output.status.success(),
            "command unexpectedly succeeded with stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }

    pub fn run_cli_with_stdin<const N: usize>(&self, args: [&str; N], input: &str) -> Output {
        let config = render_config(
            "cli-test",
            logpose_types::NodeRole::Combined,
            &self.temp_root.join("client-data"),
            self.rest_addr,
            self.grpc_addr,
        );
        self.run_cli_with_config_and_stdin(args, config, input)
    }

    pub fn run_cli_raw<const N: usize>(&self, args: [&str; N]) -> Output {
        let config = render_config(
            "cli-test",
            logpose_types::NodeRole::Combined,
            &self.temp_root.join("client-data"),
            self.rest_addr,
            self.grpc_addr,
        );
        self.run_cli_with_config(args, config)
    }

    pub fn run_cli_raw_args(&self, args: &[&str]) -> Output {
        let config = render_config(
            "cli-test",
            logpose_types::NodeRole::Combined,
            &self.temp_root.join("client-data"),
            self.rest_addr,
            self.grpc_addr,
        );
        self.run_cli_with_config_args(args, config)
    }

    pub fn run_cli_with_config<const N: usize>(&self, args: [&str; N], config: String) -> Output {
        Command::new(env!("CARGO_BIN_EXE_logpose-cli"))
            .current_dir(&self.temp_root)
            .env("LOGPOSE_CONFIG", config)
            .args(args)
            .output()
            .expect("cli should run")
    }

    pub fn run_cli_with_config_args(&self, args: &[&str], config: String) -> Output {
        Command::new(env!("CARGO_BIN_EXE_logpose-cli"))
            .current_dir(&self.temp_root)
            .env("LOGPOSE_CONFIG", config)
            .args(args)
            .output()
            .expect("cli should run")
    }

    pub fn run_cli_with_config_and_stdin<const N: usize>(
        &self,
        args: [&str; N],
        config: String,
        input: &str,
    ) -> Output {
        let mut child = Command::new(env!("CARGO_BIN_EXE_logpose-cli"))
            .current_dir(&self.temp_root)
            .env("LOGPOSE_CONFIG", config)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("cli should spawn");
        child
            .stdin
            .as_mut()
            .expect("stdin pipe should exist")
            .write_all(input.as_bytes())
            .expect("stdin should be written");
        child.wait_with_output().expect("cli should finish")
    }
}

impl Drop for TestServerFixture {
    fn drop(&mut self) {
        self.server.abort();
        let _ = self.runtime.block_on(async { (&mut self.server).await });
        let _ = fs::remove_dir_all(&self.temp_root);
    }
}

fn render_config(
    node_name: &str,
    node_role: logpose_types::NodeRole,
    storage_root: &Path,
    rest_addr: SocketAddr,
    grpc_addr: SocketAddr,
) -> String {
    render_config_with_hosts(
        node_name,
        node_role,
        storage_root,
        &rest_addr.ip().to_string(),
        rest_addr.port(),
        &grpc_addr.ip().to_string(),
        grpc_addr.port(),
    )
}

pub fn render_config_with_hosts(
    node_name: &str,
    node_role: logpose_types::NodeRole,
    storage_root: &Path,
    rest_host: &str,
    rest_port: u16,
    grpc_host: &str,
    grpc_port: u16,
) -> String {
    format!(
        r#"node_name = "{node_name}"
node_role = "{node_role}"
rest_host = "{rest_host}"
rest_port = {rest_port}
grpc_host = "{grpc_host}"
grpc_port = {grpc_port}
log_filter = "info"
storage_root = "{storage_root}""#,
        node_role = node_role.as_str(),
        rest_host = rest_host,
        rest_port = rest_port,
        grpc_host = grpc_host,
        grpc_port = grpc_port,
        storage_root = storage_root.display(),
    )
}

fn bind_listener(host: &str) -> TcpListener {
    let listener = TcpListener::bind((host, 0)).expect("listener should bind");
    listener
        .set_nonblocking(true)
        .expect("listener should become nonblocking");
    listener
}

fn wait_for_server(
    runtime: &Runtime,
    server: &mut tokio::task::JoinHandle<anyhow::Result<()>>,
    ready_rx: &mpsc::Receiver<&'static str>,
    addresses: &[SocketAddr],
) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let rest_address = addresses[0];
    let grpc_address = addresses[1];
    let mut rest_ready = false;
    let mut grpc_ready = false;
    while Instant::now() < deadline {
        while let Ok(kind) = ready_rx.try_recv() {
            match kind {
                "rest" => rest_ready = true,
                "grpc" => grpc_ready = true,
                _ => {}
            }
        }
        if rest_ready
            && grpc_ready
            && rest_endpoint_ready(rest_address)
            && grpc_endpoint_ready(runtime, grpc_address)
        {
            return Ok(());
        }
        if server.is_finished() {
            let result = runtime.block_on(async { (&mut *server).await });
            return Err(format!(
                "server exited before listeners were ready: {}",
                describe_server_result(result)
            ));
        }
        match ready_rx.recv_timeout(Duration::from_millis(50)) {
            Ok("rest") => rest_ready = true,
            Ok("grpc") => grpc_ready = true,
            Ok(_) | Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    let status = if server.is_finished() {
        let result = runtime.block_on(async { (&mut *server).await });
        describe_server_result(result)
    } else {
        "server still running".to_owned()
    };
    Err(format!(
        "timed out waiting for listeners at {:?}: {status}",
        addresses
    ))
}

fn describe_server_result(result: Result<anyhow::Result<()>, tokio::task::JoinError>) -> String {
    match result {
        Ok(Ok(())) => "server task exited cleanly before readiness".to_owned(),
        Ok(Err(error)) => error.to_string(),
        Err(error) => error.to_string(),
    }
}

fn rest_endpoint_ready(address: SocketAddr) -> bool {
    let dial_address = dial_address(address);
    let Ok(mut stream) =
        std::net::TcpStream::connect_timeout(&dial_address, Duration::from_millis(100))
    else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));
    let _ = stream.set_write_timeout(Some(Duration::from_millis(100)));
    if stream
        .write_all(b"GET /health HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
        .is_err()
    {
        return false;
    }
    let mut response = String::new();
    if stream.read_to_string(&mut response).is_err() {
        return false;
    }
    response.starts_with("HTTP/1.1 200") || response.starts_with("HTTP/1.0 200")
}

fn grpc_endpoint_ready(runtime: &Runtime, address: SocketAddr) -> bool {
    let dial_address = dial_address(address);
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_millis(200), async {
            let client = LogPoseClient::connect(format!("http://{dial_address}"))
                .await
                .map_err(|error| error.to_string())?;
            client
                .runtime_status()
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        })
        .await
        .is_ok_and(|result| result.is_ok())
    })
}

fn dial_address(address: SocketAddr) -> SocketAddr {
    match address {
        SocketAddr::V4(address) if address.ip().is_unspecified() => {
            SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, address.port()))
        }
        SocketAddr::V6(address) if address.ip().is_unspecified() => {
            SocketAddr::from((std::net::Ipv6Addr::LOCALHOST, address.port()))
        }
        other => other,
    }
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("logpose-{prefix}-{suffix}"));
    fs::create_dir_all(&dir).expect("temp dir should be created");
    dir
}
