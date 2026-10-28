use std::{
    env, fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command},
    thread,
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

use reqwest::blocking::Client;
use serde_json::{Value, json};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        #[cfg(unix)]
        let _ = Command::new("kill")
            .args(["-KILL", "--", &format!("-{}", self.0.id())])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");
    match args.get(1).map(String::as_str) {
        Some("serve") => {
            let port = args.get(2).map_or("8080", String::as_str);
            let listener = TcpListener::bind(format!("127.0.0.1:{port}"))?;
            eprintln!("Serving http://127.0.0.1:{port} (Ctrl-C to stop)");
            serve(listener, &root)
        }
        Some("smoke") => {
            for browser in ["chrome", "firefox"] {
                smoke(&root, browser)?;
            }
            Ok(())
        }
        _ => Err("usage: browser-example-tools serve [port] | smoke".into()),
    }
}

fn serve(listener: TcpListener, root: &Path) -> Result<()> {
    for stream in listener.incoming() {
        let mut stream = stream?;
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
        stream.set_write_timeout(Some(Duration::from_secs(2)))?;
        if let Err(error) = respond(&mut stream, root) {
            eprintln!("HTTP: {error}");
        }
    }
    Ok(())
}

fn respond(stream: &mut TcpStream, root: &Path) -> Result<()> {
    let mut buffer = [0; 8192];
    let count = stream.read(&mut buffer)?;
    let request = String::from_utf8_lossy(&buffer[..count]);
    let path = request.split_whitespace().nth(1).unwrap_or("/");
    let path = path.split('?').next().unwrap_or("/");
    let path = if path == "/" {
        "index.html"
    } else {
        path.strip_prefix('/').unwrap_or("")
    };
    let allowed = matches!(path, "index.html" | "app.js") || path.starts_with("pkg/");
    let file = root.join(path);
    let (status, body) = if allowed && !path.contains("..") {
        match fs::read(&file) {
            Ok(body) => ("200 OK", body),
            Err(_) => ("404 Not Found", b"Not found".to_vec()),
        }
    } else {
        ("404 Not Found", b"Not found".to_vec())
    };
    let mime = match file.extension().and_then(|s| s.to_str()) {
        Some("wasm") => "application/wasm",
        Some("js") => "text/javascript",
        _ => "text/html; charset=utf-8",
    };
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: {mime}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(&body)?;
    Ok(())
}

fn call(client: &Client, base: &str, method: &str, path: &str, body: Value) -> Result<Value> {
    let response: Value = client
        .request(method.parse()?, format!("{base}{path}"))
        .json(&body)
        .send()?
        .json()?;
    if response["value"]["error"].is_string() {
        return Err(format!("WebDriver {path}: {response}").into());
    }
    Ok(response["value"].clone())
}

fn smoke(root: &Path, browser: &str) -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let root = root.to_path_buf();
    thread::spawn(move || {
        let _ = serve(listener, &root);
    });
    let port_listener = TcpListener::bind("127.0.0.1:0")?;
    let port = port_listener.local_addr()?.port();
    drop(port_listener);
    let (variable, executable) = if browser == "chrome" {
        ("CHROMEDRIVER", "chromedriver")
    } else {
        ("GECKODRIVER", "geckodriver")
    };
    let driver_path = env::var(variable).unwrap_or_else(|_| executable.into());
    eprintln!("Smoke {browser}: driver={driver_path}, origin=http://{address}");
    let mut command = Command::new(&driver_path);
    command.arg(format!("--port={port}"));
    #[cfg(unix)]
    command.process_group(0);
    let _driver = Process(command.spawn().map_err(|error| {
        format!("Cannot start {driver_path}: {error}; install driver or set {variable}")
    })?);
    let client = Client::builder().timeout(Duration::from_secs(55)).build()?;
    let base = format!("http://127.0.0.1:{port}");
    let deadline = Instant::now() + Duration::from_secs(10);
    while client.get(format!("{base}/status")).send().is_err() {
        if Instant::now() > deadline {
            return Err("WebDriver startup timed out".into());
        }
        thread::sleep(Duration::from_millis(100));
    }
    let mut options = if browser == "chrome" {
        json!({"args": ["--headless=new", "--disable-dev-shm-usage"]})
    } else {
        json!({"args": ["-headless"]})
    };
    let binary_var = if browser == "chrome" {
        "CHROME_BINARY"
    } else {
        "FIREFOX_BINARY"
    };
    if let Ok(binary) = env::var(binary_var) {
        options["binary"] = json!(binary);
    }
    if browser == "chrome" && env::var_os("CHROME_NO_SANDBOX").is_some() {
        options["args"]
            .as_array_mut()
            .expect("Chrome args are an array")
            .push(json!("--no-sandbox"));
    }
    let option_key = if browser == "chrome" {
        "goog:chromeOptions"
    } else {
        "moz:firefoxOptions"
    };
    let mut capabilities = json!({"browserName": browser, "pageLoadStrategy": "normal"});
    capabilities[option_key] = options;
    let session = call(
        &client,
        &base,
        "POST",
        "/session",
        json!({"capabilities":{"alwaysMatch": capabilities}}),
    )?;
    let id = session["sessionId"].as_str().ok_or("Missing session ID")?;
    eprintln!(
        "{browser}: browser version {}",
        session["capabilities"]["browserVersion"]
    );
    let prefix = format!("/session/{id}");
    let result = (|| -> Result<()> {
        call(
            &client,
            &base,
            "POST",
            &format!("{prefix}/timeouts"),
            json!({"script": 45000, "pageLoad": 30000}),
        )?;
        call(
            &client,
            &base,
            "POST",
            &format!("{prefix}/url"),
            json!({"url":format!("http://{address}/")}),
        )?;
        for phase in [
            "save",
            "committed",
            "reload",
            "sql-save",
            "sql-committed",
            "sql-duplicate",
            "reload-again",
            "reload-again",
            "failure",
        ] {
            if phase == "committed" || phase == "sql-committed" {
                let mut actions = Vec::new();
                for key in if phase == "committed" {
                    vec!["\u{e004}", "\u{e004}", "\u{e007}"]
                } else {
                    vec!["\u{e004}", "\u{e007}"]
                } {
                    actions.push(json!({"type": "keyDown", "value": key}));
                    actions.push(json!({"type": "keyUp", "value": key}));
                }
                call(
                    &client,
                    &base,
                    "POST",
                    &format!("{prefix}/actions"),
                    json!({"actions": [{"type": "key", "id": "keyboard", "actions": actions}]}),
                )?;
            }
            if phase == "reload" || phase == "reload-again" {
                call(
                    &client,
                    &base,
                    "POST",
                    &format!("{prefix}/refresh"),
                    json!({}),
                )?;
            }
            let value = call(
                &client,
                &base,
                "POST",
                &format!("{prefix}/execute/async"),
                json!({"script": include_str!("../smoke.js"), "args": [phase]}),
            )?;
            if value != json!("ok") {
                return Err(format!("{browser} {phase}: {value}").into());
            }
            eprintln!("{browser}: {phase} passed");
        }
        Ok(())
    })();
    let cleanup = call(&client, &base, "DELETE", &prefix, json!({}));
    result?;
    cleanup?;
    Ok(())
}
