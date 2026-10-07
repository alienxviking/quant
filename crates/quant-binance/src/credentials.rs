//! The API key and secret, and the signature they produce.
//!
//! Everything in this file exists because a secret is a different kind of value
//! from the rest of this codebase, and the differences are all about where it
//! must *not* go.
//!
//! # Why this is a type and not two `String`s
//!
//! A bare `String` secret can be logged, formatted into an error, serialized
//! into a journal line, or printed by a `#[derive(Debug)]` on some struct that
//! happens to hold it three levels up. Every one of those is a plausible
//! accident and none of them fails a test. [`Credentials`] has a hand-written
//! `Debug` that redacts, no `Display`, and no `Serialize` — so the compiler
//! stops the first three and the fourth cannot reach it.
//!
//! The secret also never travels with the public-data client.
//! [`crate::rest::SnapshotClient`] fetches depth and server time and needs no
//! credentials at all; giving it a secret it does not use would mean a secret in
//! the recorder's address space for the whole of a seven-day capture that places
//! no orders. Signing lives behind its own type, held only by the thing that
//! trades.
//!
//! # Why the key is read from the environment and nowhere else
//!
//! Not a file, not a flag. A flag is in `ps` output and in the shell history; a
//! file is a thing to accidentally commit, and this repository is public. The
//! environment is not perfect — it is readable from `/proc` on Linux by the same
//! user — but it is the option whose failure modes are the least silent.
//!
//! # Why signing is `ring` rather than a new dependency
//!
//! `ring` is already compiled into this crate's graph: `rustls` is configured
//! with its `ring` backend precisely to avoid `aws-lc-rs`'s cmake and nasm, and
//! `install_crypto_provider` installs it at startup. Using it for HMAC adds no
//! new build surface at all. `hmac` + `sha2` would be two more crates to do what
//! is already linked.

use ring::hmac;

/// What went wrong before a single request was made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialError {
    /// The variable is absent or empty.
    ///
    /// Carries the variable's *name*, never any part of a value — a diagnostic
    /// that quoted what it found would put a secret in a log the moment someone
    /// set the two variables the wrong way round.
    Missing(&'static str),
    /// The variable is set but is not usable text.
    NotText(&'static str),
}

impl core::fmt::Display for CredentialError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Missing(name) => write!(
                f,
                "{name} is not set. Live trading needs {} and {}; export them in the shell that \
                 starts the run, never in a file this repository can see.",
                Credentials::KEY_VAR,
                Credentials::SECRET_VAR
            ),
            Self::NotText(name) => write!(f, "{name} is set but is not valid text"),
        }
    }
}

impl std::error::Error for CredentialError {}

/// An API key and the secret that signs for it.
pub struct Credentials {
    key: String,
    /// Prepared once. `ring` wants an opaque key rather than raw bytes, which
    /// also means the secret is not sitting in this struct as a `String` anybody
    /// could print.
    signing: hmac::Key,
}

impl Credentials {
    pub const KEY_VAR: &'static str = "BINANCE_API_KEY";
    pub const SECRET_VAR: &'static str = "BINANCE_API_SECRET";

    /// Read both from the environment.
    ///
    /// # Errors
    ///
    /// [`CredentialError`] naming the variable, when one is absent, empty, or
    /// not text. Empty counts as absent: `export BINANCE_API_SECRET=` is a
    /// mistake that would otherwise produce a valid-looking signature over an
    /// empty key and fail at the venue with an authentication error nobody could
    /// trace back to the shell.
    pub fn from_env() -> Result<Self, CredentialError> {
        let key = read(Self::KEY_VAR)?;
        let secret = read(Self::SECRET_VAR)?;
        Ok(Self::new(key, &secret))
    }

    /// Build from values the caller already has. Used by the tests, which must
    /// not depend on the environment of the machine running them.
    #[must_use]
    pub fn new(key: String, secret: &str) -> Self {
        Self {
            key,
            signing: hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes()),
        }
    }

    /// The key, for the `X-MBX-APIKEY` header.
    ///
    /// Public because it is: the key identifies the account and travels in every
    /// request header. The secret never leaves this type.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Lowercase hex HMAC-SHA256 of the query string, which is what Binance
    /// checks.
    ///
    /// The *query string* and not the URL: Binance signs the parameters only, so
    /// signing a full URL produces a valid-looking signature the venue rejects —
    /// and the rejection says nothing about why.
    #[must_use]
    pub fn sign(&self, query: &str) -> String {
        let tag = hmac::sign(&self.signing, query.as_bytes());
        let mut out = String::with_capacity(tag.as_ref().len() * 2);
        for byte in tag.as_ref() {
            // `write!` to a String cannot fail, and `unwrap` in a signing path
            // is a panic in the one place a panic is most expensive.
            out.push(HEX[usize::from(byte >> 4)]);
            out.push(HEX[usize::from(byte & 0x0f)]);
        }
        out
    }

    /// `query` with its signature appended, ready to send.
    #[must_use]
    pub fn signed_query(&self, query: &str) -> String {
        let signature = self.sign(query);
        format!("{query}&signature={signature}")
    }
}

const HEX: [char; 16] = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
];

/// Redacted, deliberately and in full.
///
/// Not "first four characters" — a prefix is enough to confirm a guess, and the
/// thing an operator actually needs from a log is *which* credential was in use,
/// which `key` already answers through its own accessor when someone chooses to
/// print it.
impl core::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Credentials")
            .field("key", &"<redacted>")
            .field("secret", &"<redacted>")
            .finish()
    }
}

fn read(name: &'static str) -> Result<String, CredentialError> {
    match std::env::var(name) {
        Ok(value) => non_empty(name, value),
        Err(std::env::VarError::NotPresent) => Err(CredentialError::Missing(name)),
        Err(std::env::VarError::NotUnicode(_)) => Err(CredentialError::NotText(name)),
    }
}

/// A set-but-blank variable counts as absent.
///
/// `export BINANCE_API_SECRET=` otherwise signs every request with an empty key:
/// valid-looking, refused at the venue, and traceable to nothing. Its own
/// function so a test can exercise **this** path rather than a copy of it —
/// the environment is process-global and two tests racing on it is a flake this
/// suite does not need.
fn non_empty(name: &'static str, value: String) -> Result<String, CredentialError> {
    if value.trim().is_empty() {
        return Err(CredentialError::Missing(name));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::{CredentialError, Credentials};

    /// RFC 4231 test case 2, which is the authoritative HMAC-SHA256 vector.
    ///
    /// Pinned against the RFC rather than against a value this implementation
    /// produced, which would be the test agreeing with itself — and a signing
    /// routine that is wrong in a self-consistent way fails only at the venue,
    /// as an authentication error that says nothing about why.
    #[test]
    fn the_signature_matches_the_rfc_4231_vector() {
        let creds = Credentials::new("key".to_owned(), "Jefe");
        assert_eq!(
            creds.sign("what do ya want for nothing?"),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn the_signature_is_over_the_query_only_not_the_url() {
        // Binance signs the parameters. Signing a whole URL yields a
        // valid-looking signature the venue rejects, with an error that does not
        // say which of the two mistakes was made.
        //
        // This is the weaker of the two guards and says so: a signer that
        // *always* prepended the URL would still produce different outputs for
        // these two inputs and this would stay green. Sabotage confirmed it.
        // What actually pins the signature is the RFC 4231 vector above; this
        // only catches a signer that collapses distinct inputs.
        let creds = Credentials::new("k".to_owned(), "s");
        assert_ne!(
            creds.sign("symbol=BTCUSDT&timestamp=1"),
            creds.sign("https://api.binance.com/api/v3/order?symbol=BTCUSDT&timestamp=1")
        );
    }

    #[test]
    fn a_signed_query_appends_rather_than_replaces() {
        let creds = Credentials::new("k".to_owned(), "s");
        let signed = creds.signed_query("symbol=BTCUSDT&timestamp=1");
        assert!(signed.starts_with("symbol=BTCUSDT&timestamp=1&signature="));
        assert_eq!(
            signed.matches("signature=").count(),
            1,
            "signing twice would send two signatures and the venue checks the first"
        );
    }

    #[test]
    fn debug_never_prints_the_secret() {
        // The accident this type exists to prevent. A `#[derive(Debug)]` three
        // levels up, an `anyhow` context, a `tracing` field — all of them reach
        // `Debug`, and none of them would fail a test.
        let creds = Credentials::new("AKIAEXAMPLEKEY".to_owned(), "super-secret-value");
        let shown = format!("{creds:?}");
        assert!(!shown.contains("super-secret-value"), "{shown}");
        assert!(!shown.contains("AKIAEXAMPLEKEY"), "{shown}");
        assert!(shown.contains("redacted"), "{shown}");
    }

    #[test]
    fn an_error_names_the_variable_and_quotes_no_value() {
        // A diagnostic that echoed what it found would leak the secret the first
        // time someone exported the two variables the wrong way round.
        let why = CredentialError::Missing(Credentials::SECRET_VAR);
        let text = why.to_string();
        assert!(text.contains("BINANCE_API_SECRET"), "{text}");
        assert!(text.contains("never in a file"), "{text}");
    }

    #[test]
    fn an_empty_variable_is_missing_rather_than_a_key() {
        // `export BINANCE_API_SECRET=` otherwise signs every request with an
        // empty key: valid-looking, rejected at the venue, and traceable to
        // nothing.
        //
        // Exercises `non_empty` directly rather than `from_env`: the
        // environment is process-global and two tests racing on it is a flake
        // this suite does not need. It is the real path, not a copy of it.
        let v = Credentials::SECRET_VAR;
        assert!(matches!(
            super::non_empty(v, String::new()),
            Err(CredentialError::Missing(_))
        ));
        assert!(matches!(
            super::non_empty(v, "   ".to_owned()),
            Err(CredentialError::Missing(_))
        ));
        assert_eq!(super::non_empty(v, "abc".to_owned()), Ok("abc".to_owned()));
    }
}
