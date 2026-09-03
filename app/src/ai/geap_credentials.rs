use std::time::{Duration, SystemTime};

use ai::api_keys::{
    ApiKeyManager, GEAP_REFRESH_LEAD_TIME, GeapCredentials, GeapCredentialsState, GeapFederation,
    GeapMintBinding, GeapRefreshOutcome, LoadGeapCredentialsError,
};
use futures::channel::oneshot;
use serde::{Deserialize, Serialize};
use vec1::vec1;
use warp_errors::report_error;
use warp_managed_secrets::ManagedSecretManager;
use warp_managed_secrets::client::{IdentityTokenOptions, TaskIdentityToken};
use warpui::r#async::Timer;
use warpui::{AppContext, ModelContext, SingletonEntity};

use crate::auth::AuthStateProvider;
use crate::settings::{AISettings, AISettingsChangedEvent};
use crate::workspaces::user_workspaces::{
    GeminiEnterpriseBackgroundHost, TeamScope, UserWorkspaces, UserWorkspacesEvent,
};

const GEAP_IDENTITY_TOKEN_DURATION: Duration = Duration::from_secs(60 * 60);

/// Floor on the proactive refresh timer delay so a near-expired store
/// cannot spin mint -> store -> re-mint as a hot loop;
const GEAP_MIN_TIMER_DELAY: Duration = Duration::from_secs(60);

const STS_TOKEN_URL: &str = "https://sts.googleapis.com/v1/token";
const IAM_GENERATE_ACCESS_TOKEN_URL: &str = "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{sa_email}:generateAccessToken";
const CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";
const TOKEN_EXCHANGE_GRANT_TYPE: &str = "urn:ietf:params:oauth:grant-type:token-exchange";
const ID_TOKEN_TYPE: &str = "urn:ietf:params:oauth:token-type:id_token";
const ACCESS_TOKEN_TYPE: &str = "urn:ietf:params:oauth:token-type:access_token";
const SA_ACCESS_TOKEN_LIFETIME: &str = "3600s";

const GEAP_MINT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GeapPolicy {
    Disabled,
    Unconfigured,
    /// Two or more of the user's teams enable Gemini Enterprise against different Google
    /// Cloud projects, named here.
    Conflicting(Vec<String>),
    Mintable(GeapMintBinding),
}

impl GeapPolicy {
    pub(crate) fn mint_binding(self) -> Option<GeapMintBinding> {
        match self {
            GeapPolicy::Mintable(binding) => Some(binding),
            GeapPolicy::Disabled | GeapPolicy::Unconfigured | GeapPolicy::Conflicting(_) => None,
        }
    }
}

fn geap_mint_binding_from_parts(
    user_uid: String,
    gcp_audience: Option<&str>,
    gcp_sa_email: Option<&str>,
) -> Option<GeapMintBinding> {
    unimplemented!("TODO: Remove");
}

fn geap_policy_from_host_settings(
    settings: Option<&crate::workspaces::workspace::LlmHostSettings>,
    app: &AppContext,
) -> GeapPolicy {
    unimplemented!("TODO: Remove");
}

/// The GEAP policy for a request made under `scope`. Use this whenever a scope is available --
/// i.e. whenever the work is rooted in a window.
pub(crate) fn current_geap_policy<S: TeamScope + ?Sized>(
    scope: &S,
    app: &AppContext,
) -> GeapPolicy {
    unimplemented!("TODO: Remove");
}

/// The GEAP gate for the single, app-wide credential store.
///
/// Every trigger for a background mint -- a settings poll, a token nearing expiry, a
/// request-time safety net with no scope threaded to it yet -- has no window behind it, so this
/// deliberately takes no team scope and instead reads across all of the user's teams:
/// background GEAP work succeeds if any one of them enables it. See
/// [`UserWorkspaces::gemini_enterprise_host_for_any_enabling_team`].
pub(crate) fn current_geap_policy_for_any_team(app: &AppContext) -> GeapPolicy {
    unimplemented!("TODO: Remove");
}

pub trait GeapCredentialRefresher {
    fn subscribe_to_geap_settings_changes(&mut self, ctx: &mut ModelContext<Self>)
    where
        Self: Sized;
}

impl GeapCredentialRefresher for ApiKeyManager {
    fn subscribe_to_geap_settings_changes(&mut self, ctx: &mut ModelContext<Self>) {
        unimplemented!("TODO: Remove");
    }
}

/// Standard (non-forced) refresh: the skip-if-valid guard decides whether a
/// mint is actually needed.
pub(crate) fn refresh_geap_credentials(
    manager: &mut ApiKeyManager,
    ctx: &mut ModelContext<ApiKeyManager>,
) {
    unimplemented!("TODO: Remove");
}

pub(crate) fn force_refresh_geap_credentials(
    manager: &mut ApiKeyManager,
    ctx: &mut ModelContext<ApiKeyManager>,
) {
    unimplemented!("TODO: Remove");
}

/// The refresh guard + mint kickoff that all triggers funnel through.
fn refresh_geap_credentials_with_options(
    manager: &mut ApiKeyManager,
    force: bool,
    waiter: Option<oneshot::Sender<GeapRefreshOutcome>>,
    ctx: &mut ModelContext<ApiKeyManager>,
) {
    unimplemented!("TODO: Remove");
}

#[cfg(test)]
#[path = "geap_credentials_tests.rs"]
mod tests;
