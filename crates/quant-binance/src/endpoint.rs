//! The one rule for pointing a binary at a local stand-in instead of a venue.
//!
//! `live` takes `--rest`, `--ws-api` and `--market-ws`, and `venue-check`
//! takes `--rest`, each to point one endpoint at a server on this host for a
//! rehearsal. Both binaries hold an override to the same rule, so it lives
//! here once rather than in each: two copies of a rule about where money may
//! be sent would be two rules the moment one of them changed.
//!
//! The rule is **loopback only**. An override exists for a local stand-in and
//! for nothing else. Any other host could be the other environment's venue: a
//! testnet run given `--rest https://api.binance.com` would place real orders
//! under a banner that says no money is at risk, and a live check pointed at
//! the testnet would report READY about an account the run never trades. The
//! environment flag is the one way to choose a venue, so the venue a run names
//! is the venue it reaches.

/// Why an override was refused. Neither says what was given, in case it was a
/// key pasted in the wrong place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotLocal {
    /// It does not begin with one of the schemes the endpoint takes.
    Scheme,
    /// It names a host that is not this one's loopback.
    Host,
}

/// `text`, if it begins with one of `schemes` and names a loopback host.
///
/// # Errors
///
/// [`NotLocal`] says which half of the rule it broke.
pub fn local_override(text: &str, schemes: &[&str]) -> Result<String, NotLocal> {
    let Some(rest) = schemes.iter().find_map(|scheme| text.strip_prefix(scheme)) else {
        return Err(NotLocal::Scheme);
    };
    if is_loopback(rest) {
        Ok(text.to_owned())
    } else {
        Err(NotLocal::Host)
    }
}

/// Whether what follows a URL's scheme names a loopback host. Strict: a user
/// part is refused outright (`127.0.0.1@elsewhere` is elsewhere), and a name
/// counts only when it is exactly `localhost`, so `localhost.example` and
/// `127.0.0.1.example` are not loopback.
#[must_use]
pub fn is_loopback(after_scheme: &str) -> bool {
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();
    if authority.contains('@') {
        return false;
    }
    if let Some(bracketed) = authority.strip_prefix('[') {
        let Some((inside, after)) = bracketed.split_once(']') else {
            return false;
        };
        return (after.is_empty() || after.starts_with(':'))
            && inside
                .parse::<std::net::Ipv6Addr>()
                .is_ok_and(|ip| ip.is_loopback());
    }
    let host = authority.split(':').next().unwrap_or_default();
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::Ipv4Addr>()
            .is_ok_and(|ip| ip.is_loopback())
}
