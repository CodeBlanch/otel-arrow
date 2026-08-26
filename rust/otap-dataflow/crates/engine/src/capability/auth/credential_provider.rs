// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The `CredentialProvider` capability.

use futures::Stream;
use otel_arrow_dfe_engine_macros::capability;
use secrecy::{ExposeSecret, SecretString};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::capability::CapabilityError;

/// How close to [`Credential::expires_on`] a credential stops being usable.
///
/// Part of the capability contract rather than either side's private tuning,
/// because both sides have to agree on it:
///
/// - A **provider** must not serve a credential inside this window, and must
///   schedule its refresh far enough ahead of expiry to publish a replacement
///   before the current one enters it. A provider whose refresh lead time is
///   smaller than this margin strands its consumers: the credential it is still
///   serving has already stopped being usable.
/// - A **consumer** must stop sending requests once its cached credential is
///   inside this window, so a request cannot outlive the credential it carries
///   while in flight, in the presence of clock skew between the consumer, the
///   credential issuer and the service.
///
/// Fixed rather than configurable so a provider can validate its own refresh
/// settings against the same value every consumer enforces. It has to cover a
/// request's own duration plus that clock skew; 30s matches the default
/// credential endpoint timeout.
pub const CREDENTIAL_USABLE_MARGIN: Duration = Duration::from_secs(30);

/// A per-consumer subscription to credential refreshes.
///
/// The item is a plain [`Credential`], not a `Result`: a refresh failure does
/// not terminate the subscription. The stream simply does not emit until the
/// next successful refresh, and failures surface via
/// [`CredentialProvider::get_credential`] and telemetry instead. Because the
/// item is [`Clone`], a provider can fan one refreshed credential out to all
/// subscribers via a `watch`/`broadcast` channel.
///
/// Boxed to hide the concrete stream type so providers can back it differently
/// (e.g. a `watch` channel or an `unfold`) without changing the signature. The
/// `Send` bound is intentionally omitted: the subscription is always consumed
/// on the core that created it (thread-per-core), so it need not be `Send`. The
/// `#[capability]` macro emits this signature into both the `local` (`?Send`)
/// and `shared` (`Send`) trait variants unchanged.
pub type CredentialStream = Pin<Box<dyn Stream<Item = Credential> + 'static>>;

/// A credential.
///
/// The credential is wrapped in [`SecretString`]s, which zeroizes on drop and
/// masks itself in [`Debug`] output, so it cannot leak into logs or telemetry.
/// The `SecretString`s sit behind an [`Arc`] so cloning a credential (handing
/// it to multiple subscribers, or returning it from `get_credential` on the hot
/// path) is a cheap refcount bump that shares one plaintext allocation rather
/// than copying the secret bytes.
///
/// `expires_on` is a monotonic [`Instant`] -- an absolute wall-clock expiry is
/// converted to an `Instant` once, so the value is immune to wall-clock jumps
/// thereafter. `None` means no known expiry. The credential is opaque to this
/// type: an expiry is only ever what a caller supplies from the issuer's
/// response metadata, never parsed out of the credential itself.
#[derive(Clone, Debug)]
pub struct Credential {
    username: Arc<SecretString>,
    password: Arc<SecretString>,
    expires_on: Option<Instant>,
}

impl Credential {
    /// Creates a credential with **no known expiry**.
    #[must_use]
    pub fn without_expiry(
        username: impl Into<SecretString>,
        password: impl Into<SecretString>,
    ) -> Self {
        Self {
            username: Arc::new(username.into()),
            password: Arc::new(password.into()),
            expires_on: None,
        }
    }

    /// Creates a credential with an explicit optional monotonic expiry.
    #[must_use]
    pub fn with_expiry(
        username: impl Into<SecretString>,
        password: impl Into<SecretString>,
        expires_on: Option<Instant>,
    ) -> Self {
        Self {
            username: Arc::new(username.into()),
            password: Arc::new(password.into()),
            expires_on,
        }
    }

    /// Exposes the credential username secret, for the authorizer to validate
    /// or for injection into an `Authorization` header.
    ///
    /// Named `expose_username` (rather than a plain getter) so every plaintext
    /// access is explicit and greppable.
    #[must_use]
    pub fn expose_username(&self) -> &str {
        self.username.expose_secret()
    }

    /// Exposes the credential password secret, for the authorizer to validate
    /// or for injection into an `Authorization` header.
    ///
    /// Named `expose_password` (rather than a plain getter) so every plaintext
    /// access is explicit and greppable.
    #[must_use]
    pub fn expose_password(&self) -> &str {
        self.password.expose_secret()
    }

    /// The monotonic instant at which this token expires, if known.
    #[must_use]
    pub const fn expires_on(&self) -> Option<Instant> {
        self.expires_on
    }
}

/// Hands out credentials to data-path nodes.
#[capability(
    name = "credential_provider",
    description = "Provides credentials, refreshed in the background"
)]
pub trait CredentialProvider {
    /// Returns the current valid credential for the provider's configured
    /// scope(s).
    ///
    /// The fast path reads a cached credential; on a cache miss the provider
    /// performs a credential call. A provider that shares its cache and refresh
    /// state across cloned instances can coalesce concurrent misses into a
    /// single call -- but that is a provider implementation detail, not a
    /// guarantee of this trait. Returns a [`CapabilityError`] if no valid
    /// credential can be produced.
    ///
    /// The credential is scoped to the resource(s) the provider was configured
    /// for. There is no wiring-time check that a consumer's target resource
    /// matches the provider's scope, so a mismatch surfaces at the service as
    /// an auth failure (e.g. HTTP 401) rather than at startup. Consumers must
    /// bind to a provider configured for their resource.
    async fn get_credential(&self) -> Result<Credential, CapabilityError>;

    /// Subscribes to the stream of credential refreshes.
    ///
    /// Yields each newly published credential for the lifetime of the
    /// extension; each call returns an independent subscription. The stream
    /// does not carry errors: a failed refresh does not end the subscription,
    /// and the next successful refresh still yields a credential (see
    /// [`CredentialStream`]).
    ///
    /// # Contract
    ///
    /// A subscription created *after* a credential has already been published
    /// MUST immediately yield the current credential rather than block until
    /// the next refresh. This lets a consumer subscribe at any point (for
    /// example after the provider's readiness gate has fired) and obtain a
    /// usable credential without a separate
    /// [`get_credential`](Self::get_credential) call, avoiding a race between
    /// reading the current credential and subscribing to updates. A
    /// `tokio::sync::watch`-backed implementation satisfies this naturally,
    /// since a fresh receiver observes the channel's current value on its first
    /// poll.
    fn credential_stream(&self) -> CredentialStream;
}
