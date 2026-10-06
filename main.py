"""
النظام الكامل — بوت تيليجرام موحد لجميع المحافظ:
  - يكتشف مينتات اليوم على Robinhood + Ethereum
  - يشتري لجميع المحافظ المعرفة بالتوازي (Parallel Execution)
  - يرسل إشعارًا موحدًا يتضمن نتائج جميع المحافظ (الناجحة والفاشلة)
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta

import requests
import websockets
from dotenv import load_dotenv

from buyer import (
    get_web3,
    attempt_purchase_single_wallet,
    get_wallet_lock,
)
from twitter_checker import get_twitter_username_from_opensea

load_dotenv()

OPENSEA_API_KEY = os.environ["OPENSEA_API_KEY"]
BOT_ENABLED = os.environ.get("BOT_ENABLED", "false").lower() == "true"

# تفكيك المحافظ والمفاتيح
PRIVATE_KEYS = [k.strip() for k in os.environ.get("PRIVATE_KEYS", "").split(",") if k.strip()]
WALLETS = [w.strip() for w in os.environ.get("WALLETS", "").split(",") if w.strip()]

# إعدادات بوت التيليجرام الموحد
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

if len(PRIVATE_KEYS) != len(WALLETS):
    raise ValueError("أعداد المفاتيح والمحافظ غير متطابقة في ملف .env!")

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise ValueError("يرجى تحديد TELEGRAM_BOT_TOKEN و TELEGRAM_CHAT_ID في ملف .env!")

# إنشاء هيكلية المحافظ
WALLETS_DATA = []
for i in range(len(WALLETS)):
    WALLETS_DATA.append({
        "wallet": WALLETS[i],
        "private_key": PRIVATE_KEYS[i],
    })

# قراءة روابط الـ RPC المباشرة
ROBINHOOD_RPC_URL = os.environ.get("ROBINHOOD_RPC_URL", "https://rpc.mainnet.chain.robinhood.com")
ETHEREUM_RPC_URL = os.environ.get("ETHEREUM_RPC_URL", "https://eth.llamarpc.com")

STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
DROPS_API_BASE = "https://api.opensea.io/api/v2/drops"

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
LOCAL_TZ = timezone(timedelta(hours=3))

HEARTBEAT_INTERVAL = 20
RECV_TIMEOUT = 5
FREE_PRICE_THRESHOLD_USD = 0.000001
WATCH_POLL_INTERVAL_SECONDS = 15

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("auto-buyer")

CHAIN_CONFIGS = {
    "robinhood": {
        "stream_chain_name": "robinhood",
        "rpc_url": ROBINHOOD_RPC_URL,
        "max_gas_fee_usd": 0.18,
        "min_balance_reserve_usd": 0.02,
    },
    "ethereum": {
        "stream_chain_name": "ethereum",
        "rpc_url": ETHEREUM_RPC_URL,
        "max_gas_fee_usd": 0.50,
        "min_balance_reserve_usd": 0.10,
    },
}

W3_INSTANCES = {key: get_web3(cfg["rpc_url"]) for key, cfg in CHAIN_CONFIGS.items()}
STREAM_NAME_TO_CHAIN_KEY = {cfg["stream_chain_name"]: key for key, cfg in CHAIN_CONFIGS.items()}

# تتبع المحافظ التي اشترت بنجاح: slug -> set(wallet_address)
successful_mints: dict[str, set[str]] = {}
watchlist: dict[str, dict] = {}
in_flight: set[str] = set()

REJECTION_COOLDOWN_SECONDS = 120
rejected_cooldown: dict[str, float] = {}
attempted_slugs: set[str] = set()


def is_in_cooldown(slug: str) -> bool:
    ts = rejected_cooldown.get(slug)
    if ts is None:
        return False
    if time.time() - ts >= REJECTION_COOLDOWN_SECONDS:
        rejected_cooldown.pop(slug, None)
        return False
    return True


def mark_rejected(slug: str):
    rejected_cooldown[slug] = time.time()

_eth_price_cache = {"value": None, "ts": 0}


def get_eth_price_usd() -> float:
    now = time.time()
    if _eth_price_cache["value"] and (now - _eth_price_cache["ts"] < 300):
        return _eth_price_cache["value"]
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
            timeout=8,
        )
        price = resp.json()["ethereum"]["usd"]
        _eth_price_cache["value"] = price
        _eth_price_cache["ts"] = now
        return price
    except Exception as e:
        log.warning(f"[السعر] تعذر جلب سعر ETH: {e}")
        return _eth_price_cache["value"] or 3000.0


def fetch_drop_detail(slug: str):
    try:
        resp = requests.get(
            f"{DROPS_API_BASE}/{slug}",
            headers={"x-api-key": OPENSEA_API_KEY},
            timeout=10,
        )
        if resp.status_code == 200:
            return True, resp.json()
        if resp.status_code == 404:
            return False, None
        return None, None
    except Exception as e:
        log.warning(f"[Drops API] خطأ: {e}")
        return None, None


def parse_iso(ts: str):
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def started_today_local(stage: dict) -> bool:
    start = parse_iso(stage.get("start_time", ""))
    if not start:
        return False
    return start.astimezone(LOCAL_TZ).date() == datetime.now(LOCAL_TZ).date()


def stage_has_ended(stage: dict) -> bool:
    end = parse_iso(stage.get("end_time", ""))
    if not end:
        return False
    return datetime.now(timezone.utc) > end


def is_free_or_negligible(price_wei: int, eth_price_usd: float) -> bool:
    price_usd = (price_wei / 1e18) * eth_price_usd
    return price_usd < FREE_PRICE_THRESHOLD_USD


def has_viable_free_stage(detail: dict, eth_price_usd: float) -> bool:
    stages = detail.get("stages") or []
    if not stages:
        stage = detail.get("active_stage")
        if not stage:
            return False
        return is_free_or_negligible(int(stage.get("price") or 0), eth_price_usd)

    for stage in stages:
        if stage_has_ended(stage):
            continue
        if is_free_or_negligible(int(stage.get("price") or 0), eth_price_usd):
            return True
    return False


# ---------------------------------------------------------------------------
# إدارة رسائل البوت الموحد
# ---------------------------------------------------------------------------

send_queue: "asyncio.Queue[str]" = asyncio.Queue()


def enqueue_message(text: str):
    """إضافة إشعار لقائمة الإرسال للبوت الموحد"""
    send_queue.put_nowait(text)


async def telegram_sender():
    while True:
        text = await send_queue.get()
        try:
            telegram_api = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
            await asyncio.to_thread(
                requests.post,
                f"{telegram_api}/sendMessage",
                data={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
                timeout=10,
            )
        except Exception as e:
            log.error(f"خطأ إرسال تليجرام: {e}")
        send_queue.task_done()
        await asyncio.sleep(0.2)


FAILURE_REASON_LABELS = {
    "balance_too_low": "رصيد غير كافٍ",
    "gas_too_high": "رسوم الغاز أعلى من الحد المسموح",
    "simulation_failed": "رفضت محاكاة المعاملة",
    "insufficient_funds_for_total_cost": "الرصيد لا يغطي (السعر + الغاز)",
    "tx_error": "خطأ عند إرسال المعاملة",
    "invalid_address": "عنوان محفظة غير صالح",
    "already_bought": "تم الشراء مسبقًا لهذه المحفظة",
    "sold_out": "نفدت الكمية",
    "no_contract_address": "لا يوجد عنوان عقد لهذه المجموعة",
    "all_wallets_completed": "كل المحافظ اشترت مسبقًا",
    "not_eligible_or_sold_out": "غير مؤهل لأي مرحلة حاليًا (أو نفدت الكمية)",
    "not_eligible_for_current_stage": "غير مؤهل للمرحلة النشطة حاليًا",
    "stage_not_active": "لا توجد مرحلة نشطة حاليًا لهذا العقد",
    "mint_build_failed": "تعذر بناء معاملة الشراء عبر OpenSea",
    "rate_limited": "تجاوز حد طلبات OpenSea (Rate Limit)",
    "stage_not_free_for_wallet": "المرحلة المؤهلة لهذه المحفظة ليست مجانية",
}


def build_unified_purchase_msg(detail: dict, results: list[dict], chain_key: str) -> str:
    """بناء إشعار موحد يجمع المحافظ الناجحة والمحافظ الفاشلة في رسالة واحدة"""
    name = detail.get("collection_name") or detail.get("collection_slug")
    url = detail.get("opensea_url", "")
    chain_label = "Robinhood Chain" if chain_key == "robinhood" else "Ethereum Mainnet"

    successes = [r for r in results if r.get("success")]
    failures = [r for r in results if not r.get("success")]

    lines = [
        f"📊 <b>تقرير محاولة الشراء الموحد</b> ({chain_label})",
        f"المجموعة: <b>{name}</b>",
        f"🔗 <a href='{url}'>رابط المجموعة على OpenSea</a>\n"
    ]

    if successes:
        lines.append(f"✅ <b>المحافظ الناجحة ({len(successes)}):</b>")
        for res in successes:
            w_short = res['wallet'][:6] + "..." + res['wallet'][-4:]
            tx_link = res.get('tx_hash', '')
            lines.append(
                f"• <code>{w_short}</code> | الكمية: {res.get('quantity')} | الغاز: ${res.get('gas_fee_usd', 0):.4f}\n"
                f"  Tx: <code>{tx_link}</code>"
            )
        lines.append("")

    if failures:
        lines.append(f"❌ <b>المحافظ الفاشلة ({len(failures)}):</b>")
        for res in failures:
            w_short = (res.get('wallet') or 'عام')[:6] + "..." + (res.get('wallet') or 'عام')[-4:] if res.get('wallet') else "الكل"
            reason_key = res.get("reason")
            reason_label = FAILURE_REASON_LABELS.get(reason_key, reason_key or "غير معروف")
            error_detail = res.get("error")
            extra = f" ({error_detail})" if error_detail and reason_key in ("tx_error", "simulation_failed") else ""
            lines.append(f"• <code>{w_short}</code>: {reason_label}{extra}")

    return "\n".join(lines)


def build_watching_message(detail: dict, reason: str) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug")
    return f"👀 <b>تحت المراقبة</b>\n\nالمجموعة: <b>{name}</b>\nالسبب: {reason}\nسنحاول الشراء تلقائيًا فور توفر الفرصة."


def build_gaveup_message(detail: dict, reason: str) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug")
    return f"❌ <b>انتهت الفرصة</b>\n\nالمجموعة: <b>{name}</b>\nالسبب: {reason}"


# ---------------------------------------------------------------------------
# الشراء المتوازي
# ---------------------------------------------------------------------------

async def purchase_task_for_wallet(
    w3, item, slug, max_per_wallet, remaining, eth_price_usd, max_gas_fee_usd, min_balance_reserve_usd
):
    wallet_addr = item["wallet"]
    pk = item["private_key"]

    lock = get_wallet_lock(wallet_addr)
    async with lock:
        if wallet_addr in successful_mints.get(slug, set()):
            return {"success": False, "wallet": wallet_addr, "reason": "already_bought"}

        res = await asyncio.to_thread(
            attempt_purchase_single_wallet,
            w3, pk, wallet_addr,
            max_per_wallet, remaining,
            eth_price_usd, max_gas_fee_usd, min_balance_reserve_usd, slug, OPENSEA_API_KEY,
        )

        if res.get("success"):
            if slug not in successful_mints:
                successful_mints[slug] = set()
            successful_mints[slug].add(wallet_addr)

        return res


async def try_buy_now_multi_wallet(slug: str, chain_key: str, detail: dict) -> list[dict] | None:
    stage = detail.get("active_stage")
    if not stage:
        return None

    max_supply = int(detail.get("max_supply") or 0)
    total_supply = int(detail.get("total_supply") or 0)
    remaining = max_supply - total_supply
    if remaining <= 0:
        return [{"success": False, "reason": "sold_out"}]

    contract_address = detail.get("contract_address")
    if not contract_address:
        return [{"success": False, "reason": "no_contract_address"}]

    w3 = W3_INSTANCES[chain_key]
    eth_price_usd = get_eth_price_usd()

    stage_price_wei = int(stage.get("price") or 0)
    if not is_free_or_negligible(stage_price_wei, eth_price_usd):
        return None

    max_per_wallet_raw = stage.get("max_total_mintable_by_wallet") or stage.get("max_per_wallet")
    max_per_wallet = int(max_per_wallet_raw) if max_per_wallet_raw is not None else None
    max_gas_fee_usd = CHAIN_CONFIGS[chain_key]["max_gas_fee_usd"]
    min_balance_reserve_usd = CHAIN_CONFIGS[chain_key]["min_balance_reserve_usd"]

    already_bought_wallets = successful_mints.get(slug, set())
    pending_items = [item for item in WALLETS_DATA if item["wallet"] not in already_bought_wallets]

    if not pending_items:
        return [{"success": False, "reason": "all_wallets_completed"}]

    tasks = [
        purchase_task_for_wallet(
            w3, item, slug, max_per_wallet, remaining, eth_price_usd, max_gas_fee_usd, min_balance_reserve_usd
        )
        for item in pending_items
    ]

    results = await asyncio.gather(*tasks)
    return list(results)


# ---------------------------------------------------------------------------
# تقييم النتاجات وإدارة قائمة المراقبة
# ---------------------------------------------------------------------------

async def evaluate_new_mint(slug: str, chain_key: str):
    if (
        len(successful_mints.get(slug, set())) >= len(WALLETS_DATA)
        or slug in watchlist
        or slug in in_flight
        or slug in attempted_slugs
        or is_in_cooldown(slug)
    ):
        return

    in_flight.add(slug)
    try:
        found, detail = await asyncio.to_thread(fetch_drop_detail, slug)
        if not found or not detail or not detail.get("is_minting"):
            return

        stage = detail.get("active_stage")
        if not stage or not started_today_local(stage):
            return

        eth_price_usd = get_eth_price_usd()

        if not has_viable_free_stage(detail, eth_price_usd):
            return

        stage_price_wei = int(stage.get("price") or 0)
        if not is_free_or_negligible(stage_price_wei, eth_price_usd):
            watchlist[slug] = {"chain_key": chain_key, "detail": detail}
            return

        twitter_username = await asyncio.to_thread(get_twitter_username_from_opensea, slug, OPENSEA_API_KEY)
        if not twitter_username:
            log.info(f"⏭️ تجاهل '{slug}': لا يوجد حساب X مربوط.")
            mark_rejected(slug)
            return

        log.info(f"✅ '{slug}': يوجد حساب X مربوط (@{twitter_username}) — المتابعة للشراء.")

        results = await try_buy_now_multi_wallet(slug, chain_key, detail)

        if results is None:
            watchlist[slug] = {"chain_key": chain_key, "detail": detail}
            enqueue_message(build_watching_message(detail, "السعر الحالي مدفوع — تحت المراقبة."))
            return

        # إرسال إشعار موحد بنتيجة الشراء لجميع المحافظ
        enqueue_message(build_unified_purchase_msg(detail, results, chain_key))

        if results and "wallet" not in results[0]:
            attempted_slugs.add(slug)
        elif has_viable_free_stage(detail, eth_price_usd):
            watchlist[slug] = {"chain_key": chain_key, "detail": detail}
        else:
            attempted_slugs.add(slug)

    except Exception as e:
        log.error(f"خطأ بتقييم '{slug}': {e}")
    finally:
        in_flight.discard(slug)


async def process_watchlist_slug(slug: str):
    if slug in in_flight or len(successful_mints.get(slug, set())) >= len(WALLETS_DATA):
        watchlist.pop(slug, None)
        return

    entry = watchlist.get(slug)
    if not entry:
        return

    in_flight.add(slug)
    try:
        chain_key = entry["chain_key"]
        found, fresh_detail = await asyncio.to_thread(fetch_drop_detail, slug)

        if not found or not fresh_detail or not fresh_detail.get("is_minting"):
            watchlist.pop(slug, None)
            attempted_slugs.add(slug)
            enqueue_message(build_gaveup_message(entry["detail"], "المينت لم يعد نشطًا."))
            return

        eth_price_usd = get_eth_price_usd()
        if not has_viable_free_stage(fresh_detail, eth_price_usd):
            watchlist.pop(slug, None)
            attempted_slugs.add(slug)
            enqueue_message(build_gaveup_message(fresh_detail, "لم تعد هناك أي مرحلة مجانية متبقية في الجدول."))
            return

        stage = fresh_detail.get("active_stage")
        if not stage or (stage_has_ended(stage) and not fresh_detail.get("next_stage")):
            watchlist.pop(slug, None)
            attempted_slugs.add(slug)
            enqueue_message(build_gaveup_message(fresh_detail, "انتهت المرحلة."))
            return

        results = await try_buy_now_multi_wallet(slug, chain_key, fresh_detail)

        if results is None:
            watchlist[slug] = {"chain_key": chain_key, "detail": fresh_detail}
            return

        # إرسال إشعار موحد بنتيجة الشراء لجميع المحافظ
        enqueue_message(build_unified_purchase_msg(fresh_detail, results, chain_key))

        if results and "wallet" not in results[0]:
            watchlist.pop(slug, None)
            attempted_slugs.add(slug)
        elif has_viable_free_stage(fresh_detail, eth_price_usd):
            watchlist[slug] = {"chain_key": chain_key, "detail": fresh_detail}
        else:
            watchlist.pop(slug, None)
            attempted_slugs.add(slug)

    except Exception as e:
        log.error(f"خطأ بدورة مراقبة '{slug}': {e}")
    finally:
        in_flight.discard(slug)


async def watch_loop():
    while True:
        await asyncio.sleep(WATCH_POLL_INTERVAL_SECONDS)
        if not watchlist:
            continue

        slugs_snapshot = list(watchlist.keys())
        await asyncio.gather(*(process_watchlist_slug(slug) for slug in slugs_snapshot))


async def listen_opensea():
    msg_ref = 0
    while True:
        try:
            async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=15) as ws:
                log.info(f"متصل بـ OpenSea Stream — يراقب لـ {len(WALLETS_DATA)} محافظ.")
                join_ref = str(msg_ref)
                await ws.send(json.dumps([join_ref, join_ref, "collection:*", "phx_join", {}]))
                msg_ref += 1
                last_heartbeat = time.time()

                while True:
                    if time.time() - last_heartbeat > HEARTBEAT_INTERVAL:
                        hb_ref = str(msg_ref)
                        await ws.send(json.dumps([None, hb_ref, "phoenix", "heartbeat", {}]))
                        msg_ref += 1
                        last_heartbeat = time.time()

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT)
                    except asyncio.TimeoutError:
                        continue

                    try:
                        parsed = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    if isinstance(parsed, list) and len(parsed) == 5:
                        _jref, _ref, _topic, event_name, payload_wrapper = parsed
                    else:
                        continue

                    if event_name != "item_transferred":
                        continue

                    payload = (payload_wrapper or {}).get("payload") or {}
                    item = payload.get("item", {}) or {}
                    stream_chain_name = (item.get("chain", {}) or {}).get("name", "")

                    chain_key = STREAM_NAME_TO_CHAIN_KEY.get(stream_chain_name)
                    if chain_key is None:
                        continue

                    from_address = ((payload.get("from_account") or {}).get("address", "") or "").lower()
                    if from_address != ZERO_ADDRESS:
                        continue

                    slug = (payload.get("collection", {}) or {}).get("slug", "")
                    if not slug:
                        continue

                    asyncio.create_task(evaluate_new_mint(slug, chain_key))

        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            log.warning(f"انقطع الاتصال ({e}). إعادة الاتصال...")
            await asyncio.sleep(1)
        except Exception as e:
            log.error(f"خطأ غير متوقع: {e}.")
            await asyncio.sleep(5)


async def run():
    if not BOT_ENABLED:
        log.warning("🔴 BOT_ENABLED=false")
        enqueue_message("🔴 البوت شغّال لكن بوضع الإيقاف (BOT_ENABLED=false).")
        await telegram_sender()
        return

    enqueue_message(f"✅ تم تشغيل البوت الموحد بنجاح والمراقبة مفعلة لـ {len(WALLETS_DATA)} محافظ!")
    await asyncio.gather(listen_opensea(), watch_loop(), telegram_sender())


def main():
    backoff = 2
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            log.info("تم الإيقاف يدويًا.")
            break
        except Exception as e:
            log.critical(f"توقف غير متوقع: {e}.")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        else:
            break


if __name__ == "__main__":
    main()
