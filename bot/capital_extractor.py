import time
import asyncio
from datetime import datetime
from typing import Optional, Dict, Any, Callable
from .config import settings
from .state import state, log_message
from .clob_trader import clob_trader
from .execution import close_open_position
from .telegram_service import send_telegram

async def maybe_auto_withdraw(equity: Optional[float], poly_snapshot: Dict[str, Any],
                              reflect_running_cb: Optional[Callable] = None):
    """Auto-withdrawal (capital extractor) state machine — LIVE mode only.

        ARMED --(EQUITY >= trigger)--> WAITING_FLAT --(close any open trade, go flat)-->
        WITHDRAWING --(submitted)--> WITHDRAW_SUBMITTED --> ARMED

    The trigger uses **equity** (cash + open position value).
    If a trade is open when the trigger fires it is CLOSED IMMEDIATELY.
    """
    if state["trading_mode"] != "live" or not settings.AUTO_WITHDRAW_ENABLED:
        if state["withdraw_state"] != "ARMED":
            state["withdraw_state"] = "ARMED"
        return

    st = state["withdraw_state"]
    cash = state["paper_balance"]

    if st == "ARMED":
        if equity is not None and equity >= settings.WITHDRAW_TRIGGER_BALANCE:
            state["withdraw_state"] = "WAITING_FLAT"
            state["withdraw_flat_since"] = None
            log_message(f"Auto-withdraw: equity ${equity:.2f} >= ${settings.WITHDRAW_TRIGGER_BALANCE:.2f} "
                        f"-> pausing entries and closing any open trade")

    elif st == "WAITING_FLAT":
        if state["active_trades"]:
            res = await close_open_position(poly_snapshot, "withdraw_close")
            if res:
                log_message(f"Auto-withdraw: closed {res['side']} @ {res['exit_price']:.2f} "
                            f"(P/L ${res['pl']:.2f}) to go flat")
                state["withdraw_flat_since"] = time.time()
                state["last_balance_refresh"] = 0
            return

        if state.get("withdraw_flat_since") is None:
            state["withdraw_flat_since"] = time.time()
            state["last_balance_refresh"] = 0
            return
        if time.time() - state["withdraw_flat_since"] < 5:
            return
        state["withdraw_flat_since"] = None
        state["withdraw_state"] = "WITHDRAWING"
        log_message("Auto-withdraw: account is flat -> withdrawing")

    elif st == "WITHDRAWING":
        recipient = settings.WITHDRAW_ADDRESS or clob_trader.get_eoa_address()
        if not recipient:
            log_message("Auto-withdraw aborted: no wallet/key available. Disarming.")
            state["withdraw_state"] = "ARMED"
            return
        amount = min(float(settings.WITHDRAW_AMOUNT), float(cash or 0))
        if amount <= 0:
            log_message("Auto-withdraw aborted: no cash balance to withdraw. Disarming.")
            state["withdraw_state"] = "ARMED"
            return
        result = await asyncio.to_thread(clob_trader.withdraw_pusd, recipient, amount)
        if result.get("ok"):
            when = datetime.now()
            tx = result.get("tx")
            state["last_withdrawal"] = {
                "amount": result.get("amount"),
                "tx": tx,
                "to": result.get("recipient"),
                "time": when.isoformat()
            }
            state["withdraw_state"] = "WITHDRAW_SUBMITTED"
            state["withdraw_submitted_at"] = time.time()
            log_message(f"Auto-withdraw: submitted ${amount:.2f} -> {recipient} (tx {tx})")
            await send_telegram(
                "💸 <b>Withdrawal completed</b>\n"
                f"Amount: <b>${amount:.2f}</b>\n"
                f"Time: {when.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"To: <code>{recipient}</code>"
                + (f"\nTx: <code>{tx}</code>" if tx else "")
            )
        else:
            log_message(f"Auto-withdraw FAILED: {result.get('error')}. Disarming.")
            state["withdraw_state"] = "ARMED"

    elif st == "WITHDRAW_SUBMITTED":
        state["last_balance_refresh"] = 0
        if str(settings.WITHDRAW_RESUME_AFTER).lower() == "confirmed":
            tx = (state.get("last_withdrawal") or {}).get("tx")
            waited = time.time() - state.get("withdraw_submitted_at", time.time())
            confirmed = await asyncio.to_thread(clob_trader.is_tx_confirmed, tx) if tx else None
            if confirmed is not True and waited < 180:
                return
            if confirmed is True:
                log_message(f"Auto-withdraw: tx confirmed on-chain ({tx})")
            else:
                log_message(f"Auto-withdraw: no confirmation after {waited:.0f}s; resuming anyway")
        if not settings.WITHDRAW_AUTO_RESUME:
            state["running"] = False
            if reflect_running_cb:
                reflect_running_cb()
            log_message("Auto-withdraw complete; auto-resume OFF -> bot STOPPED.")
        else:
            mkt_id = str(poly_snapshot["market"].get("id")) if poly_snapshot.get("ok") else None
            if mkt_id:
                state["withdraw_locked_market"] = mkt_id
            log_message("Auto-withdraw complete; trading resumes at the next 15m market.")
        state["withdraw_state"] = "ARMED"
