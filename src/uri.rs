#[cfg(not(feature = "std"))]
use alloc::string::{String, ToString};
#[cfg(feature = "std")]
use std::string::{String, ToString};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UriScheme {
    InMemory,
    File,
    Grpc,
    Grpcs,
    Unix,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
    Tcp { host: String, port: u16 },
    Unix { path: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Uri {
    pub scheme: UriScheme,
    pub path: Option<String>,
    pub endpoint: Option<Endpoint>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UriError {
    UnsupportedScheme,
    MissingPath,
    InvalidEndpoint,
    TlsUnsupported,
}

pub fn parse_uri(uri: &str) -> Result<Uri, UriError> {
    if uri == ":in_memory:" {
        return Ok(Uri {
            scheme: UriScheme::InMemory,
            path: None,
            endpoint: None,
        });
    }
    if let Some(path) = uri.strip_prefix("ofdb://") {
        if path.is_empty() {
            return Err(UriError::MissingPath);
        }
        return Ok(Uri {
            scheme: UriScheme::File,
            path: Some(path.to_string()),
            endpoint: None,
        });
    }
    if uri.starts_with("ofdb+grpcs://") {
        return Err(UriError::TlsUnsupported);
    }
    if let Some(authority) = uri.strip_prefix("ofdb+grpc://") {
        let (host, port) = parse_authority(authority)?;
        return Ok(Uri {
            scheme: UriScheme::Grpc,
            path: None,
            endpoint: Some(Endpoint::Tcp { host, port }),
        });
    }
    if let Some(path) = uri.strip_prefix("ofdb+unix://") {
        if !path.starts_with('/')
            || path.starts_with("//")
            || path.len() == 1
            || path.contains('?')
            || path.contains('#')
        {
            return Err(UriError::InvalidEndpoint);
        }
        return Ok(Uri {
            scheme: UriScheme::Unix,
            path: None,
            endpoint: Some(Endpoint::Unix {
                path: path.to_string(),
            }),
        });
    }
    Err(UriError::UnsupportedScheme)
}

fn parse_authority(authority: &str) -> Result<(String, u16), UriError> {
    if authority.is_empty() || authority.contains(['/', '?', '#', '@']) {
        return Err(UriError::InvalidEndpoint);
    }
    let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
        let (host, rest) = rest.split_once(']').ok_or(UriError::InvalidEndpoint)?;
        let port = rest.strip_prefix(':').ok_or(UriError::InvalidEndpoint)?;
        if host.is_empty() || !host.contains(':') {
            return Err(UriError::InvalidEndpoint);
        }
        (host, port)
    } else {
        let (host, port) = authority
            .rsplit_once(':')
            .ok_or(UriError::InvalidEndpoint)?;
        if host.is_empty() || host.contains(':') {
            return Err(UriError::InvalidEndpoint);
        }
        (host, port)
    };
    let port = port.parse::<u16>().map_err(|_| UriError::InvalidEndpoint)?;
    if port == 0 || !valid_host(host) {
        return Err(UriError::InvalidEndpoint);
    }
    Ok((host.to_string(), port))
}

fn valid_host(host: &str) -> bool {
    if host.contains(':') {
        return host.parse::<core::net::Ipv6Addr>().is_ok();
    }
    if host
        .bytes()
        .all(|byte| byte.is_ascii_digit() || byte == b'.')
        && host.parse::<core::net::Ipv4Addr>().is_err()
    {
        return false;
    }
    host.len() <= 253
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                && label.as_bytes()[0] != b'-'
                && label.as_bytes()[label.len() - 1] != b'-'
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(feature = "remote"))]
    #[test]
    fn remote_uri_requires_remote_feature() {
        let error = match crate::Database::open_uri("ofdb+grpc://localhost:5000") {
            Ok(_) => panic!("remote URI unexpectedly opened without the feature"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            engine::EngineError::Custom(message) if message.contains("enable feature `remote`")
        ));
    }

    #[test]
    fn local_schemes_keep_existing_behavior() {
        assert_eq!(
            parse_uri(":in_memory:"),
            Ok(Uri {
                scheme: UriScheme::InMemory,
                path: None,
                endpoint: None
            })
        );
        assert_eq!(
            parse_uri("ofdb://./db"),
            Ok(Uri {
                scheme: UriScheme::File,
                path: Some("./db".into()),
                endpoint: None
            })
        );
        assert_eq!(parse_uri("ofdb://"), Err(UriError::MissingPath));
    }

    #[test]
    fn parses_tcp_endpoints() {
        for (uri, host, port) in [
            ("ofdb+grpc://db.local:5000", "db.local", 5000),
            ("ofdb+grpc://127.0.0.1:5000", "127.0.0.1", 5000),
            ("ofdb+grpc://[::1]:5000", "::1", 5000),
        ] {
            assert_eq!(
                parse_uri(uri).expect("valid endpoint").endpoint,
                Some(Endpoint::Tcp {
                    host: host.into(),
                    port
                })
            );
        }
    }

    #[test]
    fn parses_unix_endpoint() {
        assert_eq!(
            parse_uri("ofdb+unix:///tmp/ofdb.sock")
                .expect("valid Unix endpoint")
                .endpoint,
            Some(Endpoint::Unix {
                path: "/tmp/ofdb.sock".into()
            })
        );
    }

    #[test]
    fn rejects_invalid_remote_endpoints() {
        for uri in [
            "ofdb+grpc://",
            "ofdb+grpc://host",
            "ofdb+grpc://host:no",
            "ofdb+grpc://host:0",
            "ofdb+grpc://host:65536",
            "ofdb+grpc://user@host:5",
            "ofdb+grpc://host:5/path",
            "ofdb+grpc://host:5?q",
            "ofdb+grpc://host:5#f",
            "ofdb+grpc://[::1:5",
            "ofdb+grpc://bad_host:5",
            "ofdb+grpc://[::::]:5",
            "ofdb+grpc://999.1.1.1:5",
        ] {
            assert_eq!(parse_uri(uri), Err(UriError::InvalidEndpoint), "{uri}");
        }
        assert_eq!(
            parse_uri("ofdb+grpcs://host:443"),
            Err(UriError::TlsUnsupported)
        );
        assert_eq!(
            parse_uri("ofdb+unix://relative"),
            Err(UriError::InvalidEndpoint)
        );
        assert_eq!(
            parse_uri("ofdb+unix:////tmp/socket"),
            Err(UriError::InvalidEndpoint)
        );
    }
}
