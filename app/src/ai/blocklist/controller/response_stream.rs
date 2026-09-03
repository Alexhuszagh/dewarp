use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Local, TimeDelta};
use futures::channel::oneshot;
use uuid::Uuid;
use warp_errors::report_error;
#[cfg(not(target_family = "wasm"))]
use warp_multi_agent_api as maa_api;
use warp_multi_agent_api::response_event;
use warpui::r#async::Timer;
use warpui::{Entity, ModelContext, SingletonEntity};

use crate::ai::agent::api::{self, ConvertToAPITypeError, generate_multi_agent_output};
use crate::ai::agent::conversation::AIConversationId;
use crate::ai::agent::{AIIdentifiers, CancellationReason};
use crate::network::NetworkStatus;
use crate::send_telemetry_from_ctx;
use crate::server::retry_strategies::backoff_after_attempts;
use crate::server::server_api::{AIApiError, ServerApiProvider};
use crate::server::team_scope::RequestTeamScope;
#[cfg(test)]
use crate::workspaces::user_workspaces::TeamlessScopeForTest;

/// Maximum number of recovery attempts spent on one request before the failure is
/// surfaced.
///
/// Retries (the same request re-sent) and resumes (a fresh `ResumeConversation` request)
/// draw from this single budget. Giving resumes their own one-shot allowance, as this code
/// used to, left the effective post-action budget at exactly one attempt — and during a
/// rolling server deploy that one attempt lands inside the same window of transport resets
/// that killed the original request.
const MAX_RECOVERY_ATTEMPTS: usize = 3;

/// Maximum time to wait for a request-time Grok OAuth token refresh before
/// sending with the currently stored token. Bounded so a hung refresh can't
/// stall the request.
#[cfg(not(target_family = "wasm"))]
const GROK_REFRESH_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// How long a request will hold for a request-time GEAP credential mint before
/// giving up and sending anyway.
#[cfg(not(target_family = "wasm"))]
const GEAP_REFRESH_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// The recovery budget for one request and the retries and resumes that recover it,
/// carried forward across each of those attempts.
///
/// A retry keeps the budget inside the same [`ResponseStream`]; a resume hands it to the
/// `ResumeConversation` request the controller sends next. So the two share one counter
/// rather than getting a budget each, and a failure can no longer exhaust recovery in a
/// single attempt.
///
/// The scope is one request, not one agent turn: a turn spans many MAA requests (every
/// tool-result round trip is its own), and each starts with a [`Self::fresh`] budget, as it
/// did before retries and resumes were unified.
///
/// `pub` only to match [`ResponseStream::new`], which takes one; every constructor and
/// accessor is crate-internal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryBudget {
    attempts_used: usize,
    resume_allowed: bool,
}

impl RecoveryBudget {
    /// A full budget, for a request that is not itself recovering another.
    pub(crate) fn fresh() -> Self {
        Self {
            attempts_used: 0,
            resume_allowed: true,
        }
    }

    /// The same budget with resumes disallowed, for requests whose failures must stay
    /// silent and terminal (passive background requests).
    pub(crate) fn without_resume(self) -> Self {
        Self {
            resume_allowed: false,
            ..self
        }
    }

    /// Recovery attempts — retries and resumes — already spent recovering this request.
    pub(crate) fn attempts_used(self) -> usize {
        self.attempts_used
    }

    /// The budget for the next recovery attempt, with that attempt charged against it.
    pub(crate) fn next_attempt(self) -> Self {
        Self {
            attempts_used: self.attempts_used + 1,
            ..self
        }
    }

    fn has_remaining(self) -> bool {
        self.attempts_used < MAX_RECOVERY_ATTEMPTS
    }
}

/// A conversation resume scheduled for a failed request: the budget the resumed request
/// runs with, and how long to wait before sending it.
///
/// The wait is decided here, where the recovery decision is made, rather than recomputed
/// at send time — the schedule is jittered, so recomputing would produce a different
/// duration than the one that was logged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingResume {
    recovery: RecoveryBudget,
    backoff: Duration,
}

impl PendingResume {
    /// The budget the resumed request runs with, already charged for this resume.
    pub(crate) fn recovery(self) -> RecoveryBudget {
        self.recovery
    }

    /// How long to wait before sending the resume.
    pub(crate) fn backoff(self) -> Duration {
        self.backoff
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(recovery: RecoveryBudget, backoff: Duration) -> Self {
        Self { recovery, backoff }
    }
}

/// What to do about a failed or truncated MAA response attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryAction {
    /// Re-send the same request after a backoff.
    Retry,
    /// Re-send the same request once connectivity returns.
    RetryWhenOnline,
    /// Resume the conversation with a fresh request after the stream completes.
    Resume,
    /// Surface the error; the conversation ends in error.
    Fail(FailReason),
}

impl RecoveryAction {
    /// Which kind of recovery this is, for the recovery logs. Both retry variants share
    /// one label; the logged wait distinguishes a backed-off retry from a parked one.
    fn log_label(self) -> &'static str {
        match self {
            Self::Retry | Self::RetryWhenOnline => "retry",
            Self::Resume => "resume",
            Self::Fail(_) => "none",
        }
    }
}

/// Why a failed attempt is surfaced instead of recovered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailReason {
    /// The error is not transient, so a fresh attempt would fail identically.
    NotRecoverable,
    /// The shared retry/resume budget is spent.
    BudgetExhausted,
    /// Only a resume could recover this failure, and this request may not resume.
    ResumeNotAllowed,
}

impl FailReason {
    fn log_label(self) -> &'static str {
        match self {
            Self::NotRecoverable => "not_recoverable",
            Self::BudgetExhausted => "budget_exhausted",
            Self::ResumeNotAllowed => "resume_not_allowed",
        }
    }
}

/// Decides how to recover from a failed response-stream attempt.
///
/// Before any client actions have been received, the request can be re-sent verbatim
/// (after a backoff, or once connectivity returns). After actions have streamed,
/// re-sending is unsafe, so recovery uses a fresh `ResumeConversation` request. Both draw
/// from `recovery`, so the kind of recovery available can change mid-chain without handing
/// the request a second budget.
fn recovery_action(
    has_received_client_actions: bool,
    is_recoverable: bool,
    recovery: RecoveryBudget,
    is_online: bool,
) -> RecoveryAction {
    if !is_recoverable {
        return RecoveryAction::Fail(FailReason::NotRecoverable);
    }
    // Checked ahead of the budget so a request that could never have resumed reports that,
    // rather than whichever constraint happens to bind first: a passive request that spent
    // its budget on pre-action retries and then fails post-action is blocked by both, and
    // the ineligibility is the one worth knowing.
    if has_received_client_actions && !recovery.resume_allowed {
        return RecoveryAction::Fail(FailReason::ResumeNotAllowed);
    }
    if !recovery.has_remaining() {
        return RecoveryAction::Fail(FailReason::BudgetExhausted);
    }
    if !has_received_client_actions {
        return if is_online {
            RecoveryAction::Retry
        } else {
            RecoveryAction::RetryWhenOnline
        };
    }
    RecoveryAction::Resume
}

/// Whether a failed attempt is being recovered or surfaced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryOutcome {
    /// A recovery is in flight: the caller must not emit an error event or complete the
    /// stream for this attempt.
    InFlight,
    /// The failure has been reported and must be surfaced to the conversation.
    Surfaced,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResponseStreamId(String);

impl ResponseStreamId {
    pub fn for_shared_session(init_event: &response_event::StreamInit) -> Self {
        // Make the stream ID unique per viewing by appending a local UUID
        // This prevents collisions when replaying the same conversation multiple times
        // (either on close-and-reopen or when viewing the same shared session from multiple terminals)
        Self(format!("{}-{}", init_event.request_id, Uuid::new_v4()))
    }

    #[cfg(test)]
    pub fn new_for_test() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

/// Model wrapping an agent API response stream.
///
/// Emits events when the output corresponding to the stream is updated, typically after receiving
/// each response chunk.
///
/// Handles retries internally - retries are only attempted if no ClientActions events have been
/// received yet, ensuring we don't retry after the AI has started executing actions. Once actions
/// have streamed, recovery falls to the controller's conversation resume; both draw from the one
/// [`RecoveryBudget`] the stream carries.
pub struct ResponseStream {
    id: ResponseStreamId,
    params: api::RequestParams,
    /// The shared retry/resume budget for this request, inherited from the request this one
    /// recovers (if any) and charged for each retry sent from this stream.
    recovery: RecoveryBudget,
    /// In-request retries sent from this stream.
    ///
    /// Deliberately not derived from [`Self::recovery`]: that budget is inherited across a
    /// resume, so it counts attempts made before this request existed and would overstate
    /// the retries this request actually needed.
    retries_sent: usize,
    start_time: DateTime<Local>,
    time_to_latest_event: TimeDelta,
    cancellation_tx: Option<oneshot::Sender<()>>,
    /// Store the original error for telemetry when retries succeed
    original_error: Option<String>,
    /// Track whether we've received any client actions
    /// If true, we cannot retry on subsequent errors since actions may have been executed
    has_received_client_actions: bool,
    /// AI identifiers for telemetry emission
    ai_identifiers: AIIdentifiers,

    /// The resume to send once the stream finishes, if one was scheduled.
    ///
    /// This is set when a transient network/server failure occurs after client actions
    /// have been received (so an in-request retry is unsafe) and the shared recovery
    /// budget still permits a resume. Per-attempt state: a retry supersedes it.
    pending_resume: Option<PendingResume>,

    /// Whether a `StreamFinished` event was received for the current request. A
    /// stream that completes without one was truncated in transit.
    stream_finished_received: bool,

    /// Whether a terminal error event has already been emitted for the current
    /// request, so stream completion doesn't synthesize a second failure for it.
    error_event_emitted: bool,

    /// Whether a retry is parked waiting for a backoff or for connectivity. While set,
    /// completion of the failed attempt's underlying stream is ignored.
    deferred_retry_pending: bool,

    /// Unique, internal id for the current request.
    ///
    /// This ensures that the model never emits events for a request that was already cancelled (or
    /// retried) and is still receiving lagging events.
    ///
    /// Note this is unique compared to `id`; this is unique across retry requests while the response
    /// stream id remains stable.
    current_request_id: Option<Uuid>,

    /// Captured once at construction, so retries keep the team the request started on.
    team_scope: RequestTeamScope,
}

impl ResponseStream {
    /// Emits a synthetic successful response event through the normal controller subscription.
    #[cfg(test)]
    pub fn emit_response_event_for_test(
        &mut self,
        event: warp_multi_agent_api::ResponseEvent,
        ctx: &mut ModelContext<Self>,
    ) {
        unimplemented!("TODO: Remove");
    }

    /// Emits the natural-completion `AfterStreamFinished` event (no cancellation) through
    /// the normal controller subscription, mirroring what `on_response_stream_complete`
    /// emits once the real network stream ends. Lets a test drive the controller's
    /// post-stream-cleanup pending-events re-check without a real stream.
    #[cfg(test)]
    pub fn emit_after_stream_finished_for_test(&mut self, ctx: &mut ModelContext<Self>) {
        unimplemented!("TODO: Remove");
    }

    #[cfg(test)]
    pub fn new_for_test(id: ResponseStreamId) -> Self {
        unimplemented!("TODO: Remove");
    }

    pub fn new(
        params: api::RequestParams,
        ai_identifiers: AIIdentifiers,
        recovery: RecoveryBudget,
        team_scope: RequestTeamScope,
        ctx: &mut ModelContext<Self>,
    ) -> Self {
        unimplemented!("TODO: Remove");
    }

    pub fn id(&self) -> &ResponseStreamId {
        &self.id
    }

    /// Returns true if we should attempt to resume the conversation after the stream finishes.
    pub fn should_resume_conversation_after_stream_finished(&self) -> bool {
        self.pending_resume.is_some()
    }

    /// The resume to send once the stream finishes, if one was scheduled. It carries this
    /// request's budget with the resume already charged against it, so the resumed request
    /// can't restart recovery from scratch.
    pub(super) fn pending_resume(&self) -> Option<PendingResume> {
        self.pending_resume
    }

    /// Whether the request that just failed was the turn's own request or an automatic
    /// resume of it. Logged so `attempt=1/3` on a resume can't be misread as the first
    /// failure of the original request.
    fn failed_request_label(&self) -> &'static str {
        unimplemented!("TODO: Remove");
    }

    /// Helper function to emit AgentModeError telemetry for error that is retryable (not user visible).
    fn emit_retryable_agent_mode_error_telemetry(
        &self,
        error: String,
        ctx: &mut ModelContext<Self>,
    ) {
        unimplemented!("TODO: Remove");
    }

    fn retry(&mut self, ctx: &mut ModelContext<Self>) {
        unimplemented!("TODO: Remove");
    }

    /// Decides how to recover from `error` and starts the recovery, or reports the failure
    /// so the caller can surface it.
    fn begin_recovery(
        &mut self,
        error: &Arc<AIApiError>,
        ctx: &mut ModelContext<Self>,
    ) -> RecoveryOutcome {
        unimplemented!("TODO: Remove");
    }

    /// Logs a recovery decision.
    ///
    /// Retries and resumes log the same fields in the same shape, with the attempt number
    /// read against the one shared budget, so a single line says which kind of recovery ran
    /// and where in the budget it sits.
    fn log_recovery(&self, action: RecoveryAction, wait: &str, error: &Arc<AIApiError>) {
        unimplemented!("TODO: Remove");
    }

    fn spawn_request(
        request_id: Uuid,
        params: api::RequestParams,
        team_scope: RequestTeamScope,
        cancellation_rx: oneshot::Receiver<()>,
        ctx: &mut ModelContext<Self>,
    ) {
        unimplemented!("TODO: Remove");
    }

    /// Spawns the actual multi-agent request send for `request_id`.
    fn spawn_generate(
        request_id: Uuid,
        params: api::RequestParams,
        team_scope: RequestTeamScope,
        cancellation_rx: oneshot::Receiver<()>,
        ctx: &mut ModelContext<Self>,
    ) {
        unimplemented!("TODO: Remove");
    }

    /// Cancels the stream. The conversation_id is preserved in the emitted event for async handling.
    pub(super) fn cancel(
        &mut self,
        reason: CancellationReason,
        conversation_id: AIConversationId,
        ctx: &mut ModelContext<Self>,
    ) {
        unimplemented!("TODO: Remove");
    }

    fn on_response_stream_complete(&mut self, request_id: Uuid, ctx: &mut ModelContext<Self>) {
        unimplemented!("TODO: Remove");
    }

    fn report_request_failure(
        &self,
        error: &Arc<AIApiError>,
        is_online: bool,
        recovery_attempt: usize,
    ) {
        unimplemented!("TODO: Remove");
    }

    /// Parks a retry until connectivity returns; cancellation invalidates the parked
    /// retry through `current_request_id`.
    fn defer_retry_until_online(&mut self, ctx: &mut ModelContext<Self>) {
        unimplemented!("TODO: Remove");
    }

    /// Parks a retry behind the shared recovery backoff, so a re-send doesn't land in the
    /// same window of failures that killed the previous attempt.
    ///
    /// No `WaitingForNetwork` event is emitted: the failure hasn't been surfaced, the
    /// conversation is still in progress, and the wait is bounded to a couple of seconds.
    fn defer_retry_after_backoff(&mut self, delay: Duration, ctx: &mut ModelContext<Self>) {
        unimplemented!("TODO: Remove");
    }
}

/// Applies the result of a request-time GEAP mint to the request snapshot.
///
/// A successful mint swaps in the fresh credential.
#[cfg(not(target_family = "wasm"))]
fn apply_geap_refresh_to_params(
    params: &mut api::RequestParams,
    fresh_credentials: Option<maa_api::request::settings::api_keys::GoogleCloudCredentials>,
) {
    unimplemented!("TODO: Remove");
}

#[derive(Debug)]
pub struct Consumable<T> {
    value: Rc<RefCell<Option<T>>>,
}

impl<T> Consumable<T> {
    fn new(value: T) -> Self {
        Consumable {
            value: Rc::new(RefCell::new(Some(value))),
        }
    }

    pub(super) fn consume(&self) -> Option<T> {
        self.value.borrow_mut().take()
    }
}

impl<T> Clone for Consumable<T> {
    fn clone(&self) -> Self {
        Consumable {
            value: Rc::clone(&self.value),
        }
    }
}

/// Cancellation context preserved for async event handling.
/// Includes conversation_id because truncation can remove exchange mappings before the event is processed.
#[derive(Debug, Clone)]
pub struct StreamCancellation {
    pub reason: CancellationReason,
    pub conversation_id: AIConversationId,
}

#[derive(Debug, Clone)]
pub enum ResponseStreamEvent {
    ReceivedEvent(Consumable<api::Event>),
    /// A retry is parked until connectivity returns (`waiting: true`) or has just
    /// fired (`waiting: false`). The controller mirrors this on the conversation
    /// status (`TransientError` ↔ `InProgress`).
    ///
    /// Only emitted from `defer_retry_until_online`, i.e. always after a recoverable
    /// request failure while offline — never speculatively before an attempt. Consumers
    /// can therefore treat `waiting: true` as a transient-error (reconnecting) state.
    WaitingForNetwork {
        waiting: bool,
    },
    AfterStreamFinished {
        /// Some for cancellation (with context), None for natural completion (uses dynamic lookup).
        cancellation: Option<StreamCancellation>,
    },
}

impl Entity for ResponseStream {
    type Event = ResponseStreamEvent;
}

#[cfg(test)]
#[path = "response_stream_tests.rs"]
mod tests;
