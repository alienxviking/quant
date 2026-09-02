//! The chokepoint every order passes through.
//!
//! Empty until M6 and **present** from M3, because a chokepoint retrofitted into
//! a codebase that grew up without one means auditing every call site that
//! learned to go around it. `CLAUDE.md` puts it "between strategy and venue, not
//! a module the strategy politely calls", and the way that is enforced is
//! structural: a [`Strategy`](crate::Strategy) is handed a
//! [`Context`](crate::Context), never a venue, and `Context::submit` runs this
//! on the way out. There is no other path.

use quant_core::execution::{OrderRequest, RejectReason};
use quant_core::time::Ts;

/// Decides whether an order may leave.
pub trait RiskLayer {
    /// `None` to allow, `Some(reason)` to refuse.
    ///
    /// Takes `&mut self` because a real limit is stateful — notional traded
    /// today, open position, a tripped kill switch — and a checker that could
    /// not remember could only enforce per-order limits, which are the least
    /// useful kind.
    fn check(&mut self, request: &OrderRequest, now: Ts) -> Option<RejectReason>;
}

/// Allows everything.
///
/// M3's risk layer. Its job is to prove the path exists and that nothing can go
/// around it — not to have an opinion. A test wires a refuse-everything layer to
/// the same engine and asserts no order reaches the venue.
#[derive(Debug, Default, Clone, Copy)]
pub struct AllowAll;

impl RiskLayer for AllowAll {
    fn check(&mut self, _request: &OrderRequest, _now: Ts) -> Option<RejectReason> {
        None
    }
}
