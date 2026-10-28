#![cfg(target_os = "linux")]

use std::{
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    os::unix::fs::PermissionsExt,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

fn wait_until(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !ready() {
        assert!(Instant::now() < deadline, "condition timed out");
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn split_request_line_serves_requested_asset() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve port");
    let address = listener.local_addr().expect("address");
    drop(listener);
    let mut server = Command::new(env!("CARGO_BIN_EXE_ofdb-browser-example-tools"))
        .args(["serve", &address.port().to_string()])
        .spawn()
        .expect("start server");
    wait_until(|| TcpStream::connect(address).is_ok());
    let mut stream = TcpStream::connect(address).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .expect("timeout");
    stream.write_all(b"GET /ap").expect("first fragment");
    thread::sleep(Duration::from_millis(100));
    let second_write = stream.write_all(b"p.js HTTP/1.1\r\nHost: localhost\r\n\r\n");
    let mut response = String::new();
    let result = stream.read_to_string(&mut response);
    server.kill().expect("stop server");
    server.wait().expect("reap server");
    second_write.expect("second fragment");
    result.expect("read response");
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
    assert!(response.contains("Content-Type: text/javascript"));
}

#[test]
fn interrupts_reap_driver_and_descendant() {
    for signal in ["-INT", "-TERM"] {
        let dir =
            std::env::temp_dir().join(format!("ofdb-interrupt-{}-{signal}", std::process::id()));
        fs::create_dir_all(&dir).expect("test directory");
        let driver = dir.join("driver");
        let pid_file = dir.join("pids");
        fs::write(
            &driver,
            format!(
                "#!/bin/sh\nsleep 300 &\necho $$ $! $PPID > '{}'\nwait\n",
                pid_file.display()
            ),
        )
        .expect("fake driver");
        fs::set_permissions(&driver, fs::Permissions::from_mode(0o700)).expect("executable");
        let mut helper = Command::new(env!("CARGO_BIN_EXE_ofdb-browser-example-tools"))
            .arg("smoke")
            .env("CHROMEDRIVER", &driver)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("helper");
        wait_until(|| pid_file.exists());
        let pids = fs::read_to_string(&pid_file).expect("pids");
        Command::new("kill")
            .args([signal, &helper.id().to_string()])
            .status()
            .expect("interrupt");
        let deadline = Instant::now() + Duration::from_secs(5);
        let stopped = loop {
            if helper.try_wait().expect("poll helper").is_some() {
                break true;
            }
            if Instant::now() > deadline {
                break false;
            }
            thread::sleep(Duration::from_millis(20));
        };
        if !stopped {
            helper.kill().expect("emergency stop");
            helper.wait().expect("reap");
        }
        let running: Vec<_> = pids
            .split_whitespace()
            .filter(|pid| {
                fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|stat| {
                    !stat
                        .split(") ")
                        .nth(1)
                        .is_some_and(|rest| rest.starts_with('Z'))
                })
            })
            .collect();
        for pid in &running {
            let _ = Command::new("kill").args(["-KILL", pid]).status();
        }
        fs::remove_dir_all(dir).expect("remove fixture");
        assert!(stopped, "helper failed to stop within five seconds");
        assert!(running.is_empty(), "{signal} leaked processes: {running:?}");
    }
}
