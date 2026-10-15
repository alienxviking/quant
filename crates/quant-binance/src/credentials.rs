//! The API key, the Ed25519 private key that signs for it, and the signatures
//! they produce.
//!
//! Everything in this file exists because a private key is a different kind of
//! value from the rest of this codebase, and the differences are all about
//! where it must *not* go.
//!
//! # Why Ed25519, and not the HMAC secret this began with
//!
//! Slice (b) signed with an HMAC secret. Since then the venue has closed every
//! route the live run needs to that key type but one, and deprecated the key
//! type itself. Three facts from its documentation at commit `0556f1a`, quoted
//! in `docs/live-run.md` §5:
//!
//! - **Listen keys are gone.** `POST /api/v3/userDataStream` and the WebSocket
//!   API's `userDataStream.start` stopped answering on 2026-02-20
//!   (CHANGELOG.md:405-418), so the account stream is `session.logon` and then
//!   `userDataStream.subscribe`.
//! - **Session logon is Ed25519 only**: *"Only _Ed25519_ keys are supported for
//!   this feature"* (web-socket-api.md:1299, :3371), and
//!   `userDataStream.subscribe` needs a connection authenticated that way
//!   (web-socket-api.md:8212).
//! - **HMAC keys are deprecated** (`faqs/api_key_types.md:49`).
//!
//! So one key type signs both the REST orders and the account stream's logon,
//! and there is one signing path: this one. The HMAC path was removed rather
//! than kept beside it, because a second signer is a second thing that must
//! agree with the venue forever, and nothing would notice when it stopped.
//!
//! # Why this is a type and not two `String`s
//!
//! A bare `String` key can be logged, formatted into an error, serialized into a
//! journal line, or printed by a `#[derive(Debug)]` on some struct that happens
//! to hold it three levels up. Every one of those is a plausible accident and
//! none of them fails a test. [`Credentials`] has a hand-written `Debug` that
//! redacts, no `Display`, and no `Serialize` — so the compiler stops the first
//! three and the fourth cannot reach it. The private key is held only as
//! `ring`'s opaque key pair, never as text, so there is no text of it here for
//! anybody to print.
//!
//! The key also never travels with the public-data client.
//! [`crate::rest::SnapshotClient`] fetches depth and server time and needs no
//! credentials at all; giving it a key it does not use would mean a key in the
//! recorder's address space for the whole of a seven-day capture that places no
//! orders. Signing lives behind its own type, held only by the thing that
//! trades.
//!
//! # Why the key is read from the environment and nowhere else
//!
//! Not a file, not a flag. A flag is in `ps` output and in the shell history; a
//! file is a thing to accidentally commit, and this repository is public. The
//! environment is not perfect — it is readable from `/proc` on Linux by the same
//! user — but it is the option whose failure modes are the least silent.
//!
//! So [`Credentials::PRIVATE_KEY_VAR`] holds the key's *text*, not a path to it.
//! A path would have this read a file after all, with the rule moved one
//! indirection away. The PEM file stays wherever the operator keeps it, outside
//! the repository, and the shell that starts the run reads it into the
//! variable:
//!
//! ```text
//! export BINANCE_ED25519_PRIVATE_KEY="$(cat key.pem)"            # bash
//! $env:BINANCE_ED25519_PRIVATE_KEY = Get-Content -Raw key.pem    # PowerShell
//! ```
//!
//! Two forms are read. One is the PEM text, `-----BEGIN PRIVATE KEY-----` to
//! `-----END PRIVATE KEY-----`, with LF or CRLF line ends (PowerShell's
//! `Get-Content -Raw` keeps CRLF) and any whitespace around it. The other is the
//! bare base64 body on one line, for a shell that handles multi-line values
//! badly. Anything else is refused, loudly, by an error that names the variable
//! and quotes no part of its value: an encrypted key, any other PEM label,
//! malformed base64, or a document `ring` will not take as an Ed25519 private
//! key.
//!
//! # Making a key pair
//!
//! ```text
//! openssl genpkey -algorithm ed25519 -out key.pem    # the private key: outside the repository
//! openssl pkey -in key.pem -pubout                   # the public key: register this with Binance
//! ```
//!
//! Binance also documents its own way to make one: the FIX API's pages link a
//! tutorial on setting up an Ed25519 key pair (fix-api.md:116-117). Either way
//! only the public half is registered, and the API key Binance issues for it is
//! what goes in [`Credentials::KEY_VAR`]. Live and Spot Testnet need separate
//! pairs (`docs/live-run.md` §5), each registered where it is used.
//! [`Credentials::public_key_pem`] gives the public half of whatever key was
//! loaded, in the form `openssl pkey -pubout` writes, so an operator can
//! compare it with the registered one character for character.
//!
//! `openssl genpkey` writes PKCS#8 v1, which does not carry the public key, and
//! `ring`'s `Ed25519KeyPair::from_pkcs8` accepts only v2. So this loads with
//! `from_pkcs8_maybe_unchecked`, which takes both. "Unchecked" is about v1's
//! missing public key: there is nothing in a v1 document for the derived public
//! key to disagree with, and a v2 document is still checked against the one it
//! carries.
//!
//! # Why signing is `ring`, and encoding is `base64`, rather than new crates
//!
//! `ring` is already compiled into this crate's graph: `rustls` is configured
//! with its `ring` backend precisely to avoid `aws-lc-rs`'s cmake and nasm, and
//! `install_crypto_provider` installs it at startup. Its `Ed25519KeyPair` adds no
//! new build surface at all, where `ed25519-dalek` would be several more crates
//! to do what is already linked. `base64` 0.22 is in the graph already as well,
//! through `reqwest`, so naming it adds no crate to the build either.

use std::ffi::OsString;

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use ring::signature::{Ed25519KeyPair, KeyPair as _};

/// What went wrong before a single request was made.
///
/// No variant carries any part of a value. Each is a fixed kind, so an error's
/// text is a function of what went wrong and of nothing that was read — which
/// is what lets every one of them be printed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialError {
    /// The variable is absent, empty, or nothing but whitespace.
    ///
    /// Carries the variable's *name*, never any part of a value — a diagnostic
    /// that quoted what it found would put a key in a log the moment someone
    /// set two variables the wrong way round.
    Missing(&'static str),
    /// The variable is set but is not usable text.
    NotText(&'static str),
    /// [`Credentials::PRIVATE_KEY_VAR`] is missing, and the variable the HMAC
    /// path read, [`Credentials::RETIRED_SECRET_VAR`], is set: a setup from
    /// before the live path moved to Ed25519.
    HmacSecretInstead,
    /// The private key is encrypted. It has to be decrypted before it can be
    /// read, and decrypting is the operator's step, not this program's.
    Encrypted,
    /// A PEM block, but not an unencrypted PKCS#8 private key: some other
    /// label. Which label is not said, because the label is part of the value.
    NotPrivateKeyPem,
    /// It begins like a PEM private key but is not one well-formed block.
    MalformedPem,
    /// The body is not base64.
    NotBase64,
    /// The document is well-formed base64, but `ring` will not take it as an
    /// Ed25519 private key.
    ///
    /// `reason` is `ring`'s own word for why (`WrongAlgorithm`,
    /// `InvalidEncoding`, ...): `KeyRejected`'s `Display` writes one of a fixed
    /// set of names (ring 0.17 `src/error/key_rejected.rs`), never any part of
    /// the key.
    NotEd25519 { reason: String },
}

impl core::fmt::Display for CredentialError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let var = Credentials::PRIVATE_KEY_VAR;
        match self {
            Self::Missing(name) => write!(
                f,
                "{name} is not set. Live trading needs {} and {var}; export them in the shell \
                 that starts the run, never in a file this repository can see.",
                Credentials::KEY_VAR,
            ),
            Self::NotText(name) => write!(f, "{name} is set but is not valid text"),
            Self::HmacSecretInstead => write!(
                f,
                "{var} is not set, and {} is. The live path no longer signs with an HMAC \
                 secret: Binance has deprecated HMAC keys, and the session logon the account \
                 stream needs takes an Ed25519 key only. Make a key pair outside this \
                 repository -- `openssl genpkey -algorithm ed25519 -out key.pem`, then `openssl \
                 pkey -in key.pem -pubout` for the public key -- register the public key with \
                 Binance, set {} to the API key it issues for that key, and load the private key \
                 with `export {var}=\"$(cat key.pem)\"`. {} is not used, and can be unset.",
                Credentials::RETIRED_SECRET_VAR,
                Credentials::KEY_VAR,
                Credentials::RETIRED_SECRET_VAR,
            ),
            Self::Encrypted => write!(
                f,
                "{var} holds an encrypted private key. Decrypt it first -- `openssl pkey -in \
                 encrypted.pem -out key.pem` asks for its passphrase -- and load the decrypted \
                 text, keeping both files outside this repository."
            ),
            Self::NotPrivateKeyPem => write!(
                f,
                "{var} holds a PEM block that is not an unencrypted PKCS#8 private key. It must \
                 begin -----BEGIN PRIVATE KEY-----, as `openssl genpkey -algorithm ed25519` \
                 writes it; a public key, a certificate, or an RSA or EC key in its own format \
                 is refused."
            ),
            Self::MalformedPem => write!(
                f,
                "{var} begins like a PEM private key but is not one: it must be a single block \
                 ending -----END PRIVATE KEY-----, with nothing after it but whitespace."
            ),
            Self::NotBase64 => write!(
                f,
                "{var} is not base64. It must be the PEM text of the key, or the base64 body \
                 between its BEGIN and END lines on its own."
            ),
            Self::NotEd25519 { reason } => write!(
                f,
                "{var} is not an Ed25519 private key that can be used ({reason}). It must be \
                 unencrypted PKCS#8, as `openssl genpkey -algorithm ed25519` writes it."
            ),
        }
    }
}

impl std::error::Error for CredentialError {}

/// An API key and the Ed25519 private key that signs for it.
pub struct Credentials {
    key: String,
    /// The private key, as `ring` holds it: opaque, so not sitting in this
    /// struct as text anybody could print.
    key_pair: Ed25519KeyPair,
}

impl Credentials {
    /// The API key Binance issued for the registered public key.
    pub const KEY_VAR: &'static str = "BINANCE_API_KEY";
    /// The Ed25519 private key's text: PEM, or the bare base64 body.
    pub const PRIVATE_KEY_VAR: &'static str = "BINANCE_ED25519_PRIVATE_KEY";
    /// The HMAC secret slice (b) signed with.
    ///
    /// Never used to sign. Read only to tell someone who set it, and not
    /// [`Self::PRIVATE_KEY_VAR`], that the live path has moved, rather than
    /// that a variable they have never heard of is missing.
    pub const RETIRED_SECRET_VAR: &'static str = "BINANCE_API_SECRET";

    /// Read both from the environment.
    ///
    /// # Errors
    ///
    /// [`CredentialError`] naming the variable, when one is absent, empty, or
    /// not text, or when the private key cannot be read as an Ed25519 key; and
    /// [`CredentialError::HmacSecretInstead`] when the key is missing but the
    /// retired HMAC secret is set. Empty counts as absent:
    /// `export BINANCE_ED25519_PRIVATE_KEY=` is a mistake that should say so,
    /// not surface as some other refusal nobody could trace back to the shell.
    pub fn from_env() -> Result<Self, CredentialError> {
        Self::from_lookup(std::env::var_os)
    }

    /// [`Self::from_env`], with `lookup` in place of the process environment:
    /// `lookup(name)` is what is set under `name`, or `None` when nothing is.
    ///
    /// Everything `from_env` does but the one call that reads the environment,
    /// so a test can drive the real wiring -- which variable is read, and
    /// where each result goes -- without setting a variable every other test
    /// in the process would see.
    fn from_lookup(
        lookup: impl Fn(&'static str) -> Option<OsString>,
    ) -> Result<Self, CredentialError> {
        let read = |name| read_value(name, lookup(name));
        let key = read(Self::KEY_VAR)?;
        let text = private_key_text(
            read(Self::PRIVATE_KEY_VAR),
            read(Self::RETIRED_SECRET_VAR).is_ok(),
        )?;
        Self::new(key, &text)
    }

    /// Build from values the caller already has. Used by the tests, which must
    /// not depend on the environment of the machine running them.
    ///
    /// `private_key` is the text [`Self::PRIVATE_KEY_VAR`] would hold, in
    /// either form the module docs describe, and errors name that variable.
    ///
    /// # Errors
    ///
    /// [`CredentialError`] when the text is not an unencrypted Ed25519 private
    /// key in one of those forms.
    pub fn new(key: String, private_key: &str) -> Result<Self, CredentialError> {
        Ok(Self {
            key,
            key_pair: parse_private_key(private_key)?,
        })
    }

    /// The API key, for the `X-MBX-APIKEY` header.
    ///
    /// Public because it is: the key identifies the account and travels in every
    /// request header. The private key never leaves this type.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// The Ed25519 signature of `payload`, as standard base64: the RFC 4648
    /// alphabet, `+` and `/`, padded with `=`.
    ///
    /// What both of the venue's signed APIs take — base64 of the signature
    /// (rest-api.md:621-624, web-socket-api.md:1161-1164) — and nothing more:
    /// a REST query needs it percent-encoded as well, which is
    /// [`Self::signed_query`]'s job, and a WebSocket request puts it in JSON as
    /// it stands.
    #[must_use]
    pub fn sign(&self, payload: &[u8]) -> String {
        STANDARD.encode(self.key_pair.sign(payload))
    }

    /// `query` with its signature appended, ready to send.
    ///
    /// What is signed is `query` exactly as it is sent, and nothing else: the
    /// venue signs the parameters, not the URL, and the payload is the query
    /// string as it goes on the wire, percent-encoding included
    /// (rest-api.md:555, :592-595). Signing a full URL produces a valid-looking
    /// signature the venue rejects, and the rejection says nothing about why.
    ///
    /// The signature is then percent-encoded (rest-api.md:644-650). Base64's
    /// `+`, `/` and `=` all mean something else in a query string — a `+` is
    /// read back as a space — so sent raw, the venue would check a different
    /// signature than the one made.
    #[must_use]
    pub fn signed_query(&self, query: &str) -> String {
        let signature = percent_encode(&self.sign(query.as_bytes()));
        format!("{query}&signature={signature}")
    }

    /// The signature for a WebSocket API request with these `params`.
    ///
    /// Signs [`ws_signature_payload`]'s string and returns the base64 as it
    /// stands, **not** percent-encoded: it goes into the request's JSON
    /// `params` as the value of `signature` (web-socket-api.md:1190-1210).
    /// `session.logon`'s are `apiKey`, `timestamp` and, optionally, `recvWindow`
    /// (web-socket-api.md:3399-3406).
    #[must_use]
    pub fn sign_ws_params(&self, params: &[(&str, &str)]) -> String {
        self.sign(ws_signature_payload(params).as_bytes())
    }

    /// The public half of the loaded key, as PEM: what `openssl pkey -in key.pem
    /// -pubout` prints, so it can be compared with the key registered with the
    /// venue.
    ///
    /// Not secret: it is the half the venue is given.
    #[must_use]
    pub fn public_key_pem(&self) -> String {
        format!(
            "-----BEGIN PUBLIC KEY-----\n{}\n-----END PUBLIC KEY-----\n",
            STANDARD.encode(self.public_key_spki())
        )
    }

    /// The public key as DER `SubjectPublicKeyInfo` (RFC 8410 §4): the fixed
    /// [`ED25519_SPKI_PREFIX`] and then the key's 32 bytes, 44 in all.
    fn public_key_spki(&self) -> [u8; 44] {
        let mut der = [0; 44];
        let (prefix, key) = der.split_at_mut(ED25519_SPKI_PREFIX.len());
        prefix.copy_from_slice(&ED25519_SPKI_PREFIX);
        key.copy_from_slice(self.key_pair.public_key().as_ref());
        der
    }
}

/// The DER that comes before an Ed25519 public key's 32 bytes in a
/// `SubjectPublicKeyInfo` (RFC 8410 §4).
///
/// `SEQUENCE` of 42 bytes; in it a `SEQUENCE` of 5 holding the algorithm's OID,
/// 1.3.101.112 (`06 03 2b 65 70`); then a `BIT STRING` of 33 bytes, whose first
/// says no bits of the last byte are unused, and whose other 32 are the key.
const ED25519_SPKI_PREFIX: [u8; 12] = [
    0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
];

/// The string a WebSocket API request's signature is made over.
///
/// Every param except `signature`, sorted by name, each written `name=value`,
/// joined by `&` (web-socket-api.md:761 and, for Ed25519, :1115-1153). Sorted
/// by the names' bytes, which is the order Binance's own example sorts in
/// (Python's `sorted`, web-socket-api.md:1272-1273); for the ASCII names the
/// venue uses, that is alphabetical with capitals first. Values go in as they
/// are, not percent-encoded (web-socket-api.md:1276, *"no percent encoding
/// here!"*).
///
/// A JSON object names each field once, so `params` should too. If it repeats
/// one, both are signed, in the order given, and the venue — which reads one —
/// refuses the signature.
#[must_use]
pub fn ws_signature_payload(params: &[(&str, &str)]) -> String {
    let mut signed: Vec<(&str, &str)> = params
        .iter()
        .copied()
        .filter(|&(name, _)| name != "signature")
        .collect();
    signed.sort_by(|a, b| a.0.cmp(b.0));
    let mut payload = String::new();
    for (i, (name, value)) in signed.iter().enumerate() {
        if i > 0 {
            payload.push('&');
        }
        payload.push_str(name);
        payload.push('=');
        payload.push_str(value);
    }
    payload
}

/// Percent-encode everything outside RFC 3986's unreserved characters, in
/// upper-case hex as the venue's own examples write it (rest-api.md:649).
///
/// For base64 that is exactly `+`, `/` and `=`, the three characters
/// rest-api.md:644-650 encodes as `%2B`, `%2F` and `%3D`.
fn percent_encode(text: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(text.len() + 16);
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(byte));
        } else {
            out.push('%');
            out.push(char::from(HEX[usize::from(byte >> 4)]));
            out.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
    }
    out
}

/// Read the Ed25519 private key out of the text [`Credentials::PRIVATE_KEY_VAR`]
/// holds.
///
/// PEM or the bare body, with any whitespace around either and inside the body
/// ignored: base64 has none of its own, so the only whitespace in a body is line
/// ends, CR or LF, and the spaces some shells put where a value's newlines were.
/// Nothing that comes back quotes the text.
fn parse_private_key(text: &str) -> Result<Ed25519KeyPair, CredentialError> {
    let text = text.trim();
    if text.is_empty() {
        return Err(CredentialError::Missing(Credentials::PRIVATE_KEY_VAR));
    }
    let body = match text.strip_prefix("-----BEGIN ") {
        Some(rest) => {
            let (label, rest) = rest
                .split_once("-----")
                .ok_or(CredentialError::MalformedPem)?;
            match label {
                "PRIVATE KEY" => {}
                "ENCRYPTED PRIVATE KEY" => return Err(CredentialError::Encrypted),
                _ => return Err(CredentialError::NotPrivateKeyPem),
            }
            // The END line must close the text, and be the only boundary inside
            // it: a second block, or anything else after the first, is refused
            // rather than ignored.
            let body = rest
                .strip_suffix("-----END PRIVATE KEY-----")
                .ok_or(CredentialError::MalformedPem)?;
            if body.contains("-----") {
                return Err(CredentialError::MalformedPem);
            }
            body
        }
        None => text,
    };
    let body: String = body.chars().filter(|c| !c.is_ascii_whitespace()).collect();
    // `DecodeError`'s own text names the offending byte and its offset, which
    // is a piece of the key. It is dropped here, not formatted.
    let document = STANDARD
        .decode(body)
        .map_err(|_| CredentialError::NotBase64)?;
    Ed25519KeyPair::from_pkcs8_maybe_unchecked(&document).map_err(|rejected| {
        CredentialError::NotEd25519 {
            reason: rejected.to_string(),
        }
    })
}

/// Redacted, deliberately and in full.
///
/// Not "first four characters" — a prefix is enough to confirm a guess, and the
/// thing an operator actually needs from a log is *which* credential was in use,
/// which `key` and [`Credentials::public_key_pem`] already answer when someone
/// chooses to print them.
impl core::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Credentials")
            .field("key", &"<redacted>")
            .field("private_key", &"<redacted>")
            .finish()
    }
}

/// What is set under `name`, given as `value`, read the way `std::env::var`
/// reads it: absent is [`CredentialError::Missing`], and set but not Unicode
/// is [`CredentialError::NotText`]. Text then goes through [`non_empty`].
fn read_value(name: &'static str, value: Option<OsString>) -> Result<String, CredentialError> {
    match value.map(OsString::into_string) {
        Some(Ok(value)) => non_empty(name, &value),
        Some(Err(_)) => Err(CredentialError::NotText(name)),
        None => Err(CredentialError::Missing(name)),
    }
}

/// A set-but-blank variable counts as absent, and whitespace around a value is
/// not part of it.
///
/// `export BINANCE_API_KEY=` is a mistake that should be named as one, not sent
/// as an empty header, and an API key read with PowerShell's `Get-Content -Raw`
/// would otherwise carry its file's CRLF into a request header. (The key parser
/// says the same of blank key text on its own, for text that reaches it through
/// [`Credentials::new`].) Its own function so a test can exercise **this** path
/// rather than a copy of it — the environment is process-global and two tests
/// racing on it is a flake this suite does not need.
fn non_empty(name: &'static str, value: &str) -> Result<String, CredentialError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(CredentialError::Missing(name));
    }
    Ok(trimmed.to_owned())
}

/// The private key's text, or why there is none: `read` is what reading
/// [`Credentials::PRIVATE_KEY_VAR`] gave, and `retired_secret_set` whether
/// [`Credentials::RETIRED_SECRET_VAR`] is set.
///
/// A missing key beside a set HMAC secret is someone on the old path, and
/// "this variable is missing" would send them looking for a value they do not
/// have. Its own function, like [`non_empty`], so a test can drive it without
/// touching the process environment.
fn private_key_text(
    read: Result<String, CredentialError>,
    retired_secret_set: bool,
) -> Result<String, CredentialError> {
    match read {
        Err(CredentialError::Missing(_)) if retired_secret_set => {
            Err(CredentialError::HmacSecretInstead)
        }
        other => other,
    }
}

/// Keys and helpers for tests, in this crate and only there.
#[cfg(test)]
pub(crate) mod fixture {
    use super::Credentials;

    /// The base64 body of the Ed25519 private key Binance published for
    /// checking a signature implementation (fix-api.md:297-306).
    ///
    /// Public, illustrative and registered to nobody, which makes it the one
    /// private key it is safe to commit to this repository.
    pub(crate) const EXAMPLE_KEY_BODY: &str =
        "MC4CAQAwBQYDK2VwBCIEIIJEYWtGBrhACmb9Dvy+qa8WEf0lQOl1s4CLIAB9m89u";

    /// [`EXAMPLE_KEY_BODY`] as the PEM text `openssl genpkey` writes, LF line
    /// ends, exactly as fix-api.md:303-305 shows it.
    pub(crate) const EXAMPLE_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
         MC4CAQAwBQYDK2VwBCIEIIJEYWtGBrhACmb9Dvy+qa8WEf0lQOl1s4CLIAB9m89u\n\
         -----END PRIVATE KEY-----";

    /// The example key's public half, the 32 bytes after the `SubjectPublicKeyInfo`
    /// prefix in what `openssl pkey -pubout -outform DER` writes for it (OpenSSL
    /// 3.5.5). Derived by OpenSSL rather than by this crate, so a test can check
    /// a signature with something the signer does not share.
    pub(crate) const EXAMPLE_PUBLIC_KEY: &[u8; 32] = &[
        0x19, 0x42, 0x0a, 0xbf, 0xd7, 0x52, 0xa5, 0x65, 0xc1, 0x56, 0x25, 0xe9, 0xda, 0x6e, 0xaf,
        0x00, 0x86, 0xfc, 0x40, 0xc1, 0x49, 0x60, 0x18, 0x32, 0x14, 0x44, 0x2c, 0x14, 0x99, 0x6a,
        0x80, 0x0d,
    ];

    /// Credentials holding the example key.
    pub(crate) fn example() -> Credentials {
        Credentials::new("the-api-key".to_owned(), EXAMPLE_KEY_PEM)
            .unwrap_or_else(|why| panic!("the documented example key must load: {why}"))
    }

    /// Undo [`super::percent_encode`]: a test-side decoder, so that what the
    /// encoder sends is checked by something it does not share.
    pub(crate) fn percent_decode(text: &str) -> String {
        let bytes = text.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%' {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).expect("two hex digits");
                out.push(u8::from_str_radix(hex, 16).expect("hex"));
                i += 3;
            } else {
                out.push(bytes[i]);
                i += 1;
            }
        }
        String::from_utf8(out).expect("utf-8")
    }

    /// Every 8-character run of `text`, which is ASCII: what an error or a
    /// `Debug` that leaked any meaningful piece of a key would contain.
    pub(crate) fn eight_char_windows(text: &str) -> impl Iterator<Item = &str> {
        (0..text.len().saturating_sub(7)).map(move |i| &text[i..i + 8])
    }
}

#[cfg(test)]
mod tests {
    use super::fixture::{
        eight_char_windows, example, percent_decode, EXAMPLE_KEY_BODY, EXAMPLE_KEY_PEM,
    };
    use super::{
        non_empty, private_key_text, ws_signature_payload, CredentialError, Credentials,
        ED25519_SPKI_PREFIX,
    };
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine as _;
    use ring::signature::{KeyPair as _, UnparsedPublicKey, ED25519};
    use std::ffi::OsString;

    /// The payload of Binance's own Ed25519 example (fix-api.md:287-312): the
    /// FIX Logon's `MsgType`, `SenderCompID`, `TargetCompID`, `MsgSeqNum` and
    /// `SendingTime`, joined by SOH, byte 0x01.
    const FIX_PAYLOAD: &[u8] = b"A\x01EXAMPLE\x01SPOT\x011\x0120240627-11:17:25.223";

    /// The signature the venue publishes for [`FIX_PAYLOAD`] under the example
    /// key (fix-api.md:308-311). OpenSSL 3.5.5 reproduces it byte for byte.
    const FIX_SIGNATURE: &str =
        "4MHXelVVcpkdwuLbl6n73HQUXUf1dse2PCgT1DYqW9w8AVZ1RACFGM+5UdlGPrQHrgtS3CvsRURC1oj73j8gCA==";

    /// Binance's Ed25519 WebSocket API example payload (web-socket-api.md:1152).
    const WS_EXAMPLE_PAYLOAD: &str = "apiKey=4yNzx3yWC5bS6YTwEkSRaC0nRmSQIIStAUOh1b6kqaBrTLIhjC\
        pI5lJH8q8R8WNO&price=52000.00&quantity=0.01000000&recvWindow=100&side=SELL&symbol=BTC\
        USDT&timeInForce=GTC&timestamp=1645423376532&type=LIMIT";

    /// Pinned against the venue's own published vector rather than against a
    /// value this implementation produced, which would be the test agreeing
    /// with itself — and a signer that is wrong in a self-consistent way fails
    /// only at the venue, as an authentication error that says nothing about
    /// why. This one key and payload exercise the whole path: reading PKCS#8 v1
    /// as `openssl` writes it, signing, and the base64 alphabet and padding.
    #[test]
    fn the_signature_matches_binances_own_ed25519_vector() {
        assert_eq!(example().sign(FIX_PAYLOAD), FIX_SIGNATURE);
    }

    #[test]
    fn the_key_reads_the_same_from_crlf_pem_padded_pem_and_the_bare_body() {
        // PowerShell's `Get-Content -Raw` keeps a file's CRLF, a shell can leave
        // whitespace either side, and a shell that handles multi-line values
        // badly gets the body alone. Each must be the same key.
        let forms = [
            ("CRLF", EXAMPLE_KEY_PEM.replace('\n', "\r\n") + "\r\n"),
            ("padded", format!(" \t\n{EXAMPLE_KEY_PEM}\n\n  ")),
            ("bare", EXAMPLE_KEY_BODY.to_owned()),
            ("bare, padded", format!("  {EXAMPLE_KEY_BODY}\r\n")),
            // Newlines a shell turned into spaces, as some do when a value
            // crosses into the environment.
            ("one line", EXAMPLE_KEY_PEM.replace('\n', " ")),
        ];
        for (form, text) in forms {
            let creds = Credentials::new("k".to_owned(), &text)
                .unwrap_or_else(|why| panic!("{form}: {why}"));
            assert_eq!(creds.sign(FIX_PAYLOAD), FIX_SIGNATURE, "{form}");
        }
    }

    #[test]
    fn a_rest_signature_is_percent_encoded_and_decodes_back_to_the_base64() {
        // What `TradeClient` signs for `GET /api/v3/account` at this instant.
        // Under the example key its signature holds all three of the characters
        // base64 has and a query string does not carry as they are: `+`, `/`,
        // and the `=` that 64 bytes always pad with.
        let creds = example();
        let query = "timestamp=1700000000000&recvWindow=5000";
        let raw = creds.sign(query.as_bytes());
        // OpenSSL 3.5.5 gives the same, signing these bytes with the example key
        // (`openssl pkeyutl -sign -rawin`).
        assert_eq!(
            raw,
            "xmyZz90mk/QlhtKo1XqnpC4Lki2UdcSWKZE1omVY2mESFPRE/wpzlWcQo+v5TVF1aN7lHdICZKEI/HWpEVI4Dg=="
        );
        for c in ['+', '/', '='] {
            assert!(raw.contains(c), "the fixture must exercise {c:?}: {raw}");
        }

        let signed = creds.signed_query(query);
        let sent = signed
            .strip_prefix(&format!("{query}&signature="))
            .unwrap_or_else(|| panic!("appended to the query as it was: {signed}"));
        for c in ['+', '/', '='] {
            assert!(!sent.contains(c), "{c:?} sent raw: {sent}");
        }
        assert_eq!(percent_decode(sent), raw);
        // Upper-case hex, as the venue's own examples write it.
        assert_eq!(
            sent,
            "xmyZz90mk%2FQlhtKo1XqnpC4Lki2UdcSWKZE1omVY2mESFPRE%2FwpzlWcQo%2Bv5TVF1aN7lHdICZKEI%2FHWp\
             EVI4Dg%3D%3D"
        );
    }

    #[test]
    fn the_signature_is_over_the_query_only_not_the_url() {
        // Binance signs the parameters. Signing a whole URL yields a
        // valid-looking signature the venue rejects, with an error that does not
        // say which of the two mistakes was made.
        //
        // This is the weaker of the guards and says so: a signer that *always*
        // prepended the URL would still produce different outputs for these two
        // inputs and this would stay green. What pins the signature is the
        // venue's vector above, and the signature verifying over exactly the
        // query it follows, in `trade.rs`; this only catches a signer that
        // collapses distinct inputs.
        let creds = example();
        assert_ne!(
            creds.sign(b"symbol=BTCUSDT&timestamp=1"),
            creds.sign(b"https://api.binance.com/api/v3/order?symbol=BTCUSDT&timestamp=1")
        );
    }

    #[test]
    fn a_signed_query_appends_rather_than_replaces() {
        let signed = example().signed_query("symbol=BTCUSDT&timestamp=1");
        assert!(signed.starts_with("symbol=BTCUSDT&timestamp=1&signature="));
        assert_eq!(
            signed.matches("signature=").count(),
            1,
            "signing twice would send two signatures and the venue checks the first"
        );
    }

    #[test]
    fn a_websocket_payload_is_every_param_but_the_signature_sorted_by_name() {
        // The venue's Ed25519 example (web-socket-api.md:1076-1091), its params
        // shuffled, with the `signature` placeholder its request carries before
        // signing.
        let api_key = "4yNzx3yWC5bS6YTwEkSRaC0nRmSQIIStAUOh1b6kqaBrTLIhjCpI5lJH8q8R8WNO";
        let scrambled = [
            ("type", "LIMIT"),
            ("timestamp", "1645423376532"),
            ("signature", "------ FILL ME ------"),
            ("symbol", "BTCUSDT"),
            ("apiKey", api_key),
            ("timeInForce", "GTC"),
            ("side", "SELL"),
            ("recvWindow", "100"),
            ("quantity", "0.01000000"),
            ("price", "52000.00"),
        ];
        assert_eq!(ws_signature_payload(&scrambled), WS_EXAMPLE_PAYLOAD);

        // The signature goes into JSON as plain base64, and verifies over that
        // payload under the loaded key's public half. Binance did not publish
        // the private key behind its own example signature -- the page names
        // only the file it is kept in (web-socket-api.md:1067) -- so this is a
        // round trip rather than a pinned value; the FIX vector pins the
        // arithmetic.
        let creds = example();
        let signature = creds.sign_ws_params(&scrambled);
        let bytes = STANDARD
            .decode(&signature)
            .unwrap_or_else(|e| panic!("plain base64, not percent-encoded: {signature}: {e}"));
        UnparsedPublicKey::new(&ED25519, creds.key_pair.public_key().as_ref())
            .verify(WS_EXAMPLE_PAYLOAD.as_bytes(), &bytes)
            .expect("the signature is over the documented payload");
    }

    #[test]
    fn the_public_key_is_the_spki_openssl_writes_and_verifies_the_venues_vector() {
        let creds = example();
        let der = creds.public_key_spki();
        assert_eq!(der.len(), 44);
        assert_eq!(
            der[..12],
            [0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00]
        );
        assert_eq!(der[..12], ED25519_SPKI_PREFIX);

        // The venue's signature verifies under the key this says it holds: the
        // public half is the right half.
        let signature = STANDARD.decode(FIX_SIGNATURE).expect("base64");
        UnparsedPublicKey::new(&ED25519, &der[12..])
            .verify(FIX_PAYLOAD, &signature)
            .expect("the published signature verifies under the derived public key");

        // What `openssl pkey -in key.pem -pubout` prints for the example key
        // (OpenSSL 3.5.5), which is what an operator registers and compares.
        assert_eq!(
            creds.public_key_pem(),
            "-----BEGIN PUBLIC KEY-----\n\
             MCowBQYDK2VwAyEAGUIKv9dSpWXBViXp2m6vAIb8QMFJYBgyFEQsFJlqgA0=\n\
             -----END PUBLIC KEY-----\n"
        );
    }

    #[test]
    fn debug_never_prints_either_key() {
        // The accident this type exists to prevent. A `#[derive(Debug)]` three
        // levels up, an `anyhow` context, a `tracing` field — all of them reach
        // `Debug`, and none of them would fail a test.
        //
        // Pinned whole, not searched for what must be absent. A search finds
        // only the forms it was written for: a `Debug` that printed the API
        // key's first four characters, or the private key's bytes as hex or as
        // decimal, holds no run of either key as it was given, and passed the
        // search this replaced.
        let creds = Credentials::new("AKIAEXAMPLEKEY".to_owned(), EXAMPLE_KEY_PEM).expect("loads");
        assert_eq!(
            format!("{creds:?}"),
            r#"Credentials { key: "<redacted>", private_key: "<redacted>" }"#
        );
        // And the multi-line form, which `{:#?}` and pretty loggers print.
        assert_eq!(
            format!("{creds:#?}"),
            "Credentials {\n    key: \"<redacted>\",\n    private_key: \"<redacted>\",\n}"
        );
    }

    #[test]
    fn every_loading_error_names_the_variable_and_quotes_no_part_of_the_value() {
        // A diagnostic that echoed what it found would put a key in a log the
        // moment someone loaded the wrong file. Each case is checked against an
        // error built here without the value, which proves its text cannot
        // depend on what was read; and against every 8-character run of the
        // base64 it was given, which would catch a quoted piece of it.
        let body = EXAMPLE_KEY_BODY;
        // The example key, encrypted: `openssl pkcs8 -topk8 -v2 aes-256-cbc`.
        let encrypted_body = "MIGjMF8GCSqGSIb3DQEFDTBSMDEGCSqGSIb3DQEFDDAkBBAk6Uh8gxkojScdrSB2\
                              /LQjAgIIADAMBggqhkiG9w0CCQUAMB0GCWCGSAFlAwQBKgQQbVsgOobxvv6fxUn/\
                              7ZJe8ARA9d2xaq25M5v22n2YDu/iA3QT9LYcgnbbX9nBBZdU8xDY0vSwOnKfRIjx\
                              t8jr+LSUrXnrCr79DnKMsCgX+lWMLg==";
        // The example key's document with its algorithm changed to X25519,
        // 1.3.101.110: a well-formed PKCS#8 private key, of the wrong kind.
        let x25519_body = "MC4CAQAwBQYDK2VuBCIEIIJEYWtGBrhACmb9Dvy+qa8WEf0lQOl1s4CLIAB9m89u";
        // The example key's public half, loaded where the private half belongs.
        let public_body = "MCowBQYDK2VwAyEAGUIKv9dSpWXBViXp2m6vAIb8QMFJYBgyFEQsFJlqgA0=";
        let pem = |label: &str, body: &str| {
            format!("-----BEGIN {label}-----\n{body}\n-----END {label}-----\n")
        };
        let not_ed25519 = |reason: &str| CredentialError::NotEd25519 {
            reason: reason.to_owned(),
        };

        let cases = [
            (
                "encrypted",
                pem("ENCRYPTED PRIVATE KEY", encrypted_body),
                encrypted_body,
                CredentialError::Encrypted,
            ),
            (
                "an RSA label",
                pem("RSA PRIVATE KEY", body),
                body,
                CredentialError::NotPrivateKeyPem,
            ),
            (
                "a public key",
                pem("PUBLIC KEY", public_body),
                public_body,
                CredentialError::NotPrivateKeyPem,
            ),
            (
                "no END line",
                format!("-----BEGIN PRIVATE KEY-----\n{body}\n"),
                body,
                CredentialError::MalformedPem,
            ),
            (
                "a second block after the first",
                format!("{EXAMPLE_KEY_PEM}\n{}", pem("PRIVATE KEY", x25519_body)),
                body,
                CredentialError::MalformedPem,
            ),
            (
                "text after the END line",
                format!("{EXAMPLE_KEY_PEM}\n{body}"),
                body,
                CredentialError::MalformedPem,
            ),
            (
                "a body one character short",
                body[..body.len() - 1].to_owned(),
                &body[..body.len() - 1],
                CredentialError::NotBase64,
            ),
            (
                "a character base64 does not have",
                body.replacen('+', "!", 1),
                body,
                CredentialError::NotBase64,
            ),
            (
                "an X25519 key",
                pem("PRIVATE KEY", x25519_body),
                x25519_body,
                not_ed25519("WrongAlgorithm"),
            ),
            (
                "base64 that is not PKCS#8",
                "AAAAAAAAAAAAAAAA".to_owned(),
                "AAAAAAAAAAAAAAAA",
                not_ed25519("InvalidEncoding"),
            ),
        ];
        for (what, text, given, expected) in cases {
            let why = Credentials::new("k".to_owned(), &text).expect_err(what);
            assert_eq!(why, expected, "{what}");
            let said = why.to_string();
            let debug = format!("{why:?}");
            assert!(
                said.contains(Credentials::PRIVATE_KEY_VAR),
                "{what}: {said}"
            );
            for window in eight_char_windows(given) {
                assert!(!said.contains(window), "{what}: {window} in {said}");
                assert!(!debug.contains(window), "{what}: {window} in {debug}");
            }
        }
    }

    #[test]
    fn an_encrypted_key_says_how_to_decrypt_it() {
        let said = CredentialError::Encrypted.to_string();
        assert!(said.contains("openssl pkey"), "{said}");
        assert!(said.contains("BINANCE_ED25519_PRIVATE_KEY"), "{said}");
    }

    #[test]
    fn a_missing_variable_is_named_and_no_value_is_quoted() {
        // A diagnostic that echoed what it found would leak a key the first time
        // someone exported two variables the wrong way round.
        let why = CredentialError::Missing(Credentials::PRIVATE_KEY_VAR);
        let text = why.to_string();
        assert!(text.contains("BINANCE_ED25519_PRIVATE_KEY"), "{text}");
        assert!(text.contains("BINANCE_API_KEY"), "{text}");
        assert!(text.contains("never in a file"), "{text}");
    }

    #[test]
    fn a_retired_hmac_secret_without_a_key_says_what_changed_and_how_to_make_a_pair() {
        // Someone with slice (b)'s setup has an HMAC secret and no Ed25519 key.
        // "BINANCE_ED25519_PRIVATE_KEY is not set" would send them looking for a
        // value they do not have; this says why it is needed and how to make it.
        let missing = || Err(CredentialError::Missing(Credentials::PRIVATE_KEY_VAR));
        let why = private_key_text(missing(), true).expect_err("refused");
        assert_eq!(why, CredentialError::HmacSecretInstead);
        let said = why.to_string();
        for needle in [
            "BINANCE_ED25519_PRIVATE_KEY is not set",
            "BINANCE_API_SECRET",
            "no longer signs with an HMAC secret",
            "deprecated",
            "session logon",
            "Ed25519",
            "openssl genpkey -algorithm ed25519 -out key.pem",
            "openssl pkey -in key.pem -pubout",
            "BINANCE_API_KEY",
        ] {
            assert!(said.contains(needle), "{needle:?} missing from: {said}");
        }

        // Without the old secret it is the ordinary missing variable.
        assert_eq!(
            private_key_text(missing(), false),
            Err(CredentialError::Missing(Credentials::PRIVATE_KEY_VAR))
        );
        // A key that is there is used, whatever else is set, and any other
        // failure to read it is reported as itself.
        assert_eq!(
            private_key_text(Ok("text".to_owned()), true),
            Ok("text".to_owned())
        );
        let not_text = CredentialError::NotText(Credentials::PRIVATE_KEY_VAR);
        assert_eq!(private_key_text(Err(not_text.clone()), true), Err(not_text));
    }

    #[test]
    fn from_env_reads_each_variable_and_hands_each_where_it_belongs() {
        // `from_env`'s own wiring, run over a lookup that stands in for the
        // process environment: setting real variables here would race every
        // other test that reads them. The cases above test `private_key_text`
        // on its own; these test that `from_env` reads the retired secret's
        // variable, and nothing else, to decide whether it is set.
        fn load(vars: &[(&'static str, &str)]) -> Result<(), CredentialError> {
            let vars: Vec<(&'static str, OsString)> = vars
                .iter()
                .map(|&(name, value)| (name, OsString::from(value)))
                .collect();
            Credentials::from_lookup(move |name| {
                vars.iter()
                    .find(|(set, _)| *set == name)
                    .map(|(_, value)| value.clone())
            })
            .map(|_| ())
        }
        let key = Credentials::KEY_VAR;
        let private = Credentials::PRIVATE_KEY_VAR;
        let retired = Credentials::RETIRED_SECRET_VAR;

        // The HMAC hint when the retired secret is set and the key is not...
        assert_eq!(
            load(&[(key, "k"), (retired, "s")]),
            Err(CredentialError::HmacSecretInstead)
        );
        // ...and the plain missing variable when it is not set, whatever else
        // is: the API key being set is not the retired secret being set.
        assert_eq!(load(&[(key, "k")]), Err(CredentialError::Missing(private)));
        // A retired secret that is only whitespace is not set, as
        // `ops/preflight.sh` reads it too.
        assert_eq!(
            load(&[(key, "k"), (retired, " \r\n")]),
            Err(CredentialError::Missing(private))
        );
        // A key that is there is used, the retired secret notwithstanding.
        assert_eq!(
            load(&[(key, "k"), (private, EXAMPLE_KEY_PEM), (retired, "s")]),
            Ok(())
        );
        // The API key is read first, and its absence named as itself.
        assert_eq!(
            load(&[(private, EXAMPLE_KEY_PEM), (retired, "s")]),
            Err(CredentialError::Missing(key))
        );

        // Each value goes where it belongs: the API key, less what a shell
        // left around it, to the header, and the private key to the signer.
        let creds = Credentials::from_lookup(|name| match name {
            n if n == key => Some(OsString::from(" the-api-key\r\n")),
            n if n == private => Some(OsString::from(EXAMPLE_KEY_PEM)),
            _ => None,
        })
        .expect("loads");
        assert_eq!(creds.key(), "the-api-key");
        assert_eq!(creds.sign(FIX_PAYLOAD), FIX_SIGNATURE);

        // A key that is set but is not text is said to be that, and is not
        // turned into the HMAC hint, which is for a key that is missing.
        #[cfg(windows)]
        let odd = {
            use std::os::windows::ffi::OsStringExt;
            OsString::from_wide(&[0xd800])
        };
        #[cfg(unix)]
        let odd = {
            use std::os::unix::ffi::OsStringExt;
            OsString::from_vec(vec![0xff])
        };
        let not_text = Credentials::from_lookup(|name| match name {
            n if n == private => Some(odd.clone()),
            _ => Some(OsString::from("set")),
        });
        assert_eq!(not_text.map(|_| ()), Err(CredentialError::NotText(private)));
    }

    #[test]
    fn an_empty_variable_is_missing_rather_than_a_key() {
        // `export BINANCE_ED25519_PRIVATE_KEY=` must say the variable is
        // missing, not reach the key parser and be refused as something else.
        //
        // Exercises `non_empty` directly rather than `from_env`: the
        // environment is process-global and two tests racing on it is a flake
        // this suite does not need. It is the real path, not a copy of it.
        let v = Credentials::PRIVATE_KEY_VAR;
        for blank in ["", "   ", "\r\n", " \t\r\n "] {
            assert_eq!(
                non_empty(v, blank),
                Err(CredentialError::Missing(v)),
                "{blank:?}"
            );
        }
        assert_eq!(non_empty(v, "abc"), Ok("abc".to_owned()));
        // What PowerShell's `Get-Content -Raw` leaves on a value read from a
        // file is not part of it.
        assert_eq!(non_empty(v, " abc\r\n"), Ok("abc".to_owned()));
        // And `new`, which the tests and any caller holding text use, says the
        // same of blank text.
        assert_eq!(
            Credentials::new("k".to_owned(), " \r\n").map(|_| ()),
            Err(CredentialError::Missing(v))
        );
    }
}
