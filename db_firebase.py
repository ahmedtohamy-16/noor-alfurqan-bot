import logging
import json
from datetime import datetime, timedelta
import firebase_admin
from firebase_admin import credentials, firestore
import os

logger = logging.getLogger(__name__)

db = None

async def init_db():
    global db
    try:
        if not firebase_admin._apps:
            cred_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'serviceAccountKey.json')
            if os.path.exists(cred_path):
                cred = credentials.Certificate(cred_path)
                firebase_admin.initialize_app(cred)
            else:
                firebase_admin.initialize_app()
        db = firestore.client()
        logger.info("Firebase Firestore initialized")
    except Exception as e:
        logger.error(f"Failed to initialize Firebase: {e}")

async def _close_db():
    pass

def _get_doc_ref(collection: str, doc_id: str = None):
    if doc_id:
        return db.collection(collection).document(str(doc_id))
    return db.collection(collection)

# ==================== المستخدمون ====================

async def ensure_user(user_id: int, username: str = None, first_name: str = None):
    try:
        user_ref = _get_doc_ref("users", user_id)
        doc = user_ref.get()
        now = datetime.utcnow()
        if doc.exists:
            user_ref.update({
                "username": username or "",
                "first_name": first_name or "",
                "last_activity": now,
                "action_counts.start": firestore.Increment(1),
                "total_actions": firestore.Increment(1),
            })
            return False
        else:
            user_ref.set({
                "user_id": user_id,
                "username": username or "",
                "first_name": first_name or "",
                "language": "ar",
                "is_blocked": False,
                "joined_at": now,
                "last_activity": now,
                "points": 0,
                "streak": 0,
                "total_actions": 1,
                "action_counts": {"start": 1},
            })
            return True
    except Exception as e:
        logger.error(f"ensure_user error: {e}")
        return False

async def get_user(user_id: int):
    try:
        doc = _get_doc_ref("users", user_id).get()
        if doc.exists:
            data = doc.to_dict()
            for key, value in data.items():
                if isinstance(value, datetime):
                    data[key] = value.isoformat()
            return data
        return None
    except Exception as e:
        logger.error(f"get_user error: {e}")
        return None

async def update_user_setting(user_id: int, key: str, value):
    try:
        _get_doc_ref("users", user_id).update({key: value})
        return True
    except Exception as e:
        logger.error(f"update_user_setting error: {e}")
        return False

async def block_user(user_id: int):
    return await update_user_setting(user_id, "is_blocked", True)

async def unblock_user(user_id: int):
    return await update_user_setting(user_id, "is_blocked", False)

async def log_ban_action(admin_id: int, user_id: int, action: str, reason: str = ""):
    try:
        db.collection("admin_logs").add({
            "admin_id": admin_id,
            "target_user_id": user_id,
            "action": action,
            "reason": reason,
            "timestamp": datetime.utcnow()
        })
        return True
    except Exception as e:
        logger.error(f"log_ban_action error: {e}")
        return False

async def log_admin_action(admin_id: int, action: str, details: str = ""):
    try:
        db.collection("admin_logs").add({
            "admin_id": admin_id,
            "action": action,
            "details": details,
            "timestamp": datetime.utcnow()
        })
        return True
    except Exception as e:
        logger.error(f"log_admin_action error: {e}")
        return False

async def get_admin_actions(limit=20):
    try:
        docs = db.collection("admin_logs").order_by("timestamp", direction=firestore.Query.DESCENDING).limit(limit).stream()
        return [doc.to_dict() for doc in docs]
    except Exception as e:
        logger.error(f"get_admin_actions error: {e}")
        return []

async def get_admin_actions_count():
    try:
        docs = db.collection("admin_logs").stream()
        return sum(1 for _ in docs)
    except Exception as e:
        logger.error(f"get_admin_actions_count error: {e}")
        return 0

async def get_ban_history(user_id: int):
    try:
        docs = db.collection("admin_logs").where("target_user_id", "==", user_id).order_by("timestamp", direction=firestore.Query.DESCENDING).stream()
        return [doc.to_dict() for doc in docs]
    except Exception as e:
        logger.error(f"get_ban_history error: {e}")
        return []

async def get_ban_count():
    try:
        docs = db.collection("users").where("is_blocked", "==", True).stream()
        return sum(1 for _ in docs)
    except Exception as e:
        logger.error(f"get_ban_count error: {e}")
        return 0

async def get_blocked_users_count():
    return await get_ban_count()

async def get_all_users_count():
    try:
        docs = db.collection("users").stream()
        return sum(1 for _ in docs)
    except Exception as e:
        logger.error(f"get_all_users_count error: {e}")
        return 0

async def get_active_users_count(days=7):
    try:
        since = datetime.utcnow() - timedelta(days=days)
        docs = db.collection("users").where("last_activity", ">=", since).stream()
        return sum(1 for _ in docs)
    except Exception as e:
        logger.error(f"get_active_users_count error: {e}")
        return 0

async def get_new_users_count(days=1):
    try:
        since = datetime.utcnow() - timedelta(days=days)
        docs = db.collection("users").where("joined_at", ">=", since).stream()
        return sum(1 for _ in docs)
    except Exception as e:
        logger.error(f"get_new_users_count error: {e}")
        return 0

async def get_interacting_users_count(days=1):
    return await get_active_users_count(days)

async def get_all_users(limit=10, offset=0):
    try:
        query = db.collection("users").order_by("joined_at", direction=firestore.Query.DESCENDING).limit(limit).offset(offset)
        docs = query.stream()
        users = []
        for doc in docs:
            data = doc.to_dict()
            for key, value in data.items():
                if isinstance(value, datetime):
                    data[key] = value.isoformat()
            users.append(data)
        return users
    except Exception as e:
        logger.error(f"get_all_users error: {e}")
        return []

async def search_user_by_query(query: str):
    try:
        if query.isdigit():
            user = await get_user(int(query))
            return [user] if user else []
        docs = db.collection("users").where("username", ">=", query).where("username", "<=", query + '\uf8ff').stream()
        users = [doc.to_dict() for doc in docs]
        if not users:
            docs = db.collection("users").where("first_name", ">=", query).where("first_name", "<=", query + '\uf8ff').stream()
            users = [doc.to_dict() for doc in docs]
        return users
    except Exception as e:
        logger.error(f"search_user_by_query error: {e}")
        return []

# ==================== إعدادات البوت ====================

async def get_bot_setting(key: str, default=""):
    try:
        doc = _get_doc_ref("bot_settings", key).get()
        if doc.exists:
            return doc.to_dict().get("value", default)
        return default
    except Exception as e:
        logger.error(f"get_bot_setting error: {e}")
        return default

async def set_bot_setting(key: str, value: str, admin_id: int = None):
    try:
        data = {"key": key, "value": str(value), "updated_at": datetime.utcnow()}
        if admin_id:
            data["updated_by"] = admin_id
        _get_doc_ref("bot_settings", key).set(data, merge=True)
        return True
    except Exception as e:
        logger.error(f"set_bot_setting error: {e}")
        return False

# ==================== المشرفون ====================

async def is_admin(user_id: int) -> bool:
    try:
        doc = _get_doc_ref("admins", user_id).get()
        return doc.exists
    except Exception as e:
        logger.error(f"is_admin error: {e}")
        return False

async def get_admin_permissions(user_id: int) -> dict:
    try:
        doc = _get_doc_ref("admins", user_id).get()
        if doc.exists:
            perms = doc.to_dict().get("permissions", {})
            return perms if isinstance(perms, dict) else json.loads(perms)
        return {}
    except Exception as e:
        logger.error(f"get_admin_permissions error: {e}")
        return {}

async def promote_admin(user_id: int, username: str, first_name: str, promoted_by: int, permissions: dict = None):
    try:
        data = {
            "user_id": user_id,
            "username": username or "",
            "first_name": first_name or "",
            "promoted_by": promoted_by,
            "permissions": permissions or {},
            "created_at": datetime.utcnow()
        }
        _get_doc_ref("admins", user_id).set(data)
        return True
    except Exception as e:
        logger.error(f"promote_admin error: {e}")
        return False

async def demote_admin(user_id: int):
    try:
        _get_doc_ref("admins", user_id).delete()
        return True
    except Exception as e:
        logger.error(f"demote_admin error: {e}")
        return False

async def get_all_admins():
    try:
        docs = db.collection("admins").stream()
        return [doc.to_dict() for doc in docs]
    except Exception as e:
        logger.error(f"get_all_admins error: {e}")
        return []

# ==================== الأقسام الديناميكية ====================

async def get_active_sections(parent="main"):
    try:
        query = db.collection("dynamic_sections")\
            .where("parent_menu", "==", parent)\
            .where("is_hidden", "==", False)\
            .where("is_deleted", "==", False)
        docs = query.stream()
        sections = [doc.to_dict() for doc in docs]
        sections.sort(key=lambda x: x.get("order_index", 0))
        return sections
    except Exception as e:
        logger.error(f"get_active_sections error: {e}")
        return []

async def toggle_section_visibility(section_id_or_key: str, is_active: bool):
    """يدعم doc_id مباشرة أو section_key"""
    try:
        ref = db.collection("dynamic_sections").document(section_id_or_key)
        if ref.get().exists:
            ref.update({"is_hidden": not is_active})
            return True
        for d in db.collection("dynamic_sections")\
                   .where("section_key", "==", section_id_or_key).stream():
            d.reference.update({"is_hidden": not is_active})
            return True
        return False
    except Exception as e:
        logger.error(f"toggle_section_visibility error: {e}")
        return False

async def delete_section(section_id: str):
    try:
        db.collection("dynamic_sections").document(section_id).update({"is_deleted": True})
        return True
    except Exception as e:
        logger.error(f"delete_section error: {e}")
        return False

# ==================== الختمة ====================

async def get_khatma(user_id: int):
    try:
        doc = db.collection("users").document(str(user_id)).collection("khatma_progress").document("current").get()
        if doc.exists:
            return doc.to_dict()
        return None
    except Exception as e:
        logger.error(f"get_khatma error: {e}")
        return None

async def update_khatma_plan(user_id: int, target_days, pages_per_day, reading_time, current_page, completed_khatmat):
    try:
        data = {
            "target_days": target_days,
            "pages_per_day": pages_per_day,
            "reading_time": reading_time,
            "current_page": current_page,
            "completed_khatmat": completed_khatmat,
            "started_at": datetime.utcnow()
        }
        db.collection("users").document(str(user_id)).collection("khatma_progress").document("current").set(data, merge=True)
        return True
    except Exception as e:
        logger.error(f"update_khatma_plan error: {e}")
        return False

async def update_khatma_page(user_id: int, page: int):
    try:
        db.collection("users").document(str(user_id)).collection("khatma_progress").document("current").update({"current_page": page})
        return True
    except Exception as e:
        logger.error(f"update_khatma_page error: {e}")
        return False

async def log_khatma_daily_read(user_id: int, pages: int, date_str: str):
    try:
        read_ref = db.collection("users").document(str(user_id)).collection("khatma_reads").document(date_str)
        doc = read_ref.get()
        if doc.exists:
            read_ref.update({"pages_read": firestore.Increment(pages)})
        else:
            read_ref.set({
                "read_date": date_str,
                "pages_read": pages,
                "created_at": datetime.utcnow()
            })
        week_id = datetime.utcnow().strftime("%Y-W%U")
        leaderboard_ref = db.collection("leaderboards").document(week_id).collection("users").document(str(user_id))
        lb_doc = leaderboard_ref.get()
        if lb_doc.exists:
            leaderboard_ref.update({"pages_read": firestore.Increment(pages), "last_update": datetime.utcnow()})
        else:
            user_doc = db.collection("users").document(str(user_id)).get()
            user_data = user_doc.to_dict() if user_doc.exists else {}
            leaderboard_ref.set({
                "user_id": user_id,
                "first_name": user_data.get("first_name", "مستخدم"),
                "username": user_data.get("username", ""),
                "pages_read": pages,
                "khatmat_completed": 0,
                "last_update": datetime.utcnow()
            })
        return True
    except Exception as e:
        logger.error(f"log_khatma_daily_read error: {e}")
        return False

async def get_khatma_daily_reads(user_id: int, date_str: str):
    try:
        doc = db.collection("users").document(str(user_id)).collection("khatma_reads").document(date_str).get()
        if doc.exists:
            return doc.to_dict().get("pages_read", 0)
        return 0
    except Exception as e:
        logger.error(f"get_khatma_daily_reads error: {e}")
        return 0

async def get_khatma_all_daily_reads(user_id: int):
    try:
        docs = db.collection("users").document(str(user_id)).collection("khatma_reads").order_by("read_date", direction=firestore.Query.DESCENDING).stream()
        return [doc.to_dict() for doc in docs]
    except Exception as e:
        logger.error(f"get_khatma_all_daily_reads error: {e}")
        return []

async def complete_khatma(user_id: int):
    try:
        user_ref = db.collection("users").document(str(user_id)).collection("khatma_progress").document("current")
        user_ref.update({
            "completed_khatmat": firestore.Increment(1),
            "current_page": 1,
            "last_completed": datetime.utcnow()
        })
        await increment_khatma_completed(user_id)
        return True
    except Exception as e:
        logger.error(f"complete_khatma error: {e}")
        return False

async def increment_khatma_completed(user_id: int):
    try:
        week_id = datetime.utcnow().strftime("%Y-W%U")
        leaderboard_ref = db.collection("leaderboards").document(week_id).collection("users").document(str(user_id))
        if leaderboard_ref.get().exists:
            leaderboard_ref.update({"khatmat_completed": firestore.Increment(1)})
    except Exception as e:
        logger.error(f"increment_khatma_completed error: {e}")

async def log_page_view(user_id: int, page_num: int):
    try:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        page_ref = db.collection("users").document(str(user_id)).collection("page_views").document(f"{today}_{page_num}")
        if not page_ref.get().exists:
            page_ref.set({
                "page_num": page_num,
                "viewed_at": datetime.utcnow(),
                "date": today
            })
            return True
        return False
    except Exception as e:
        logger.error(f"log_page_view error: {e}")
        return False

async def get_top_users_by_points(limit: int = 10):
    """يُعيد أعلى المستخدمين بالنقاط — مع fallback يدوي يضمن عدم الفراغ
    حتى لو كان index الـ order_by مفقودًا أو الحقل غير موجود لبعض الوثائق.
    """
    try:
        try:
            docs = (db.collection("users")
                    .order_by("points", direction=firestore.Query.DESCENDING)
                    .limit(limit).stream())
            res = [d.to_dict() | {"_id": d.id} for d in docs
                   if (d.to_dict() or {}).get("points", 0) > 0]
            if res:
                return res[:limit]
        except Exception as e:
            logger.warning(f"top_users order_by failed, fallback: {e}")
        # Fallback يدوي: نقرأ الكل ونرتّب في Python
        docs = db.collection("users").stream()
        rows = []
        for d in docs:
            data = d.to_dict() or {}
            pts = int(data.get("points", 0) or 0)
            if pts > 0:
                rows.append({**data, "_id": d.id, "points": pts})
        rows.sort(key=lambda x: x["points"], reverse=True)
        return rows[:limit]
    except Exception as e:
        logger.error(f"get_top_users_by_points error: {e}")
        return []


async def ensure_weekly_top_snapshot(top_n: int = 10):
    """يدير snapshot المتصدرين بشكل صحيح زمنيًا:

    الفكرة: نحتفظ في bot_settings بمعرّف آخر أسبوع رصدنا فيه الحالة
    (`top_snapshot_marker_week`). عند أول فتح في أسبوع جديد، الحالة الراهنة
    تمثّل فعليًا «نهاية الأسبوع السابق» — لأنه لم تمرّ أيام/نقاط جديدة بعد.
    عندئذٍ نحفظ snapshot تحت الأسبوع السابق ونحدّث المؤشّر.

    لا يُكتب أبدًا snapshot لأسبوع ليس هو الأسبوع المنصرم مباشرة.
    """
    try:
        from datetime import datetime as _dt
        current_week = _dt.utcnow().strftime("%Y-W%U")
        marker_doc = db.collection("bot_settings").document("top_snapshot_marker").get()
        marker = (marker_doc.to_dict() or {}).get("week_id") if marker_doc.exists else None

        if marker is None:
            # أول مرة على الإطلاق: ضع المؤشّر بدون snapshot — سيتولّد عند تغيّر الأسبوع.
            db.collection("bot_settings").document("top_snapshot_marker").set({
                "week_id": current_week,
                "updated_at": _dt.utcnow(),
            })
            return

        if marker == current_week:
            return  # ما زلنا في نفس الأسبوع — لا حاجة لـ snapshot

        # نحن في أسبوع جديد ولم نلتقط snapshot للأسبوع السابق بعد.
        # الحالة الراهنة = نهاية الأسبوع السابق (حدث الانتقال للتو من ناحية القراءة).
        snap_ref = db.collection("top_users_snapshots").document(marker)
        if not snap_ref.get().exists:
            top = await get_top_users_by_points(limit=top_n)
            snap_ref.set({
                "season_id": marker,
                "captured_at": _dt.utcnow(),
                "top": [
                    {"user_id": int(u.get("_id") or u.get("user_id") or 0),
                     "first_name": u.get("first_name", "مستخدم"),
                     "points": int(u.get("points", 0))}
                    for u in top
                ],
            })

        # حدّث المؤشّر للأسبوع الحالي
        db.collection("bot_settings").document("top_snapshot_marker").set({
            "week_id": current_week,
            "updated_at": _dt.utcnow(),
        })
    except Exception as e:
        logger.warning(f"ensure_weekly_top_snapshot: {e}")


async def get_last_week_top_snapshot():
    """يُعيد (top_list, week_id) لآخر snapshot صالح إن وُجد.
    نقرأ أحدث وثيقة في `top_users_snapshots` (مرتّبة حسب captured_at تنازليًا).
    """
    try:
        try:
            docs = list(db.collection("top_users_snapshots")
                        .order_by("captured_at", direction=firestore.Query.DESCENDING)
                        .limit(1).stream())
        except Exception:
            docs = list(db.collection("top_users_snapshots").stream())
            docs.sort(key=lambda d: (d.to_dict() or {}).get("captured_at") or 0,
                      reverse=True)
            docs = docs[:1]
        if not docs:
            return [], None
        data = docs[0].to_dict() or {}
        return (data.get("top", []) or []), data.get("season_id")
    except Exception as e:
        logger.error(f"get_last_week_top_snapshot: {e}")
        return [], None


async def get_weekly_top_readers(limit=5):
    try:
        week_id = datetime.utcnow().strftime("%Y-W%U")
        query = db.collection("leaderboards").document(week_id).collection("users")\
            .order_by("pages_read", direction=firestore.Query.DESCENDING).limit(limit)
        docs = query.stream()
        return [doc.to_dict() for doc in docs]
    except Exception as e:
        logger.error(f"get_weekly_top_readers error: {e}")
        return []

async def get_user_viewed_pages_count(user_id: int, date_str: str):
    try:
        docs = db.collection("users").document(str(user_id)).collection("page_views").where("date", "==", date_str).stream()
        return sum(1 for _ in docs)
    except Exception as e:
        logger.error(f"get_user_viewed_pages_count error: {e}")
        return 0


async def log_user_viewed_page(user_id: int, page_num: int, date_str: str):
    """يسجّل أن المستخدم فتح صفحة قرآن معيّنة في تاريخ معيّن.
    يُستخدم لتفعيل التحقّق الحقيقي قبل تسجيل صفحات الختمة."""
    try:
        ref = (db.collection("users").document(str(user_id))
                 .collection("page_views").document(f"{date_str}_{page_num}"))
        ref.set({
            "page": int(page_num),
            "date": date_str,
            "ts": datetime.utcnow(),
        }, merge=True)
        return True
    except Exception as e:
        logger.error(f"log_user_viewed_page error: {e}")
        return False

# ==================== الإحصائيات ====================

async def log_stat(user_id: int, action: str, extra: str = ""):
    try:
        stat_ref = db.collection("stats").document(datetime.utcnow().strftime("%Y-%m-%d"))
        stat_ref.set({action: firestore.Increment(1)}, merge=True)
        try:
            user_ref = _get_doc_ref("users", user_id)
            user_ref.update({
                f"action_counts.{action}": firestore.Increment(1),
                "total_actions": firestore.Increment(1),
                "last_activity": datetime.utcnow(),
            })
        except Exception:
            pass
        return True
    except Exception as e:
        logger.error(f"log_stat error: {e}")
        return False

async def add_user_points(user_id: int, points: int, reason: str, description: str = ""):
    try:
        user_ref = _get_doc_ref("users", user_id)
        user_ref.update({"points": firestore.Increment(points)})
        return True
    except Exception as e:
        logger.error(f"add_user_points error: {e}")
        return False

# ==================== القراء المفضلين ====================

async def get_favorite_reciters(user_id: int):
    try:
        doc = db.collection("users").document(str(user_id)).collection("favorites").document("reciters").get()
        if doc.exists:
            return doc.to_dict().get("ids", [])
        return []
    except Exception as e:
        logger.error(f"get_favorite_reciters error: {e}")
        return []

async def add_favorite_reciter(user_id: int, reciter_id: int):
    try:
        ref = db.collection("users").document(str(user_id)).collection("favorites").document("reciters")
        doc = ref.get()
        if doc.exists:
            ref.update({"ids": firestore.ArrayUnion([reciter_id])})
        else:
            ref.set({"ids": [reciter_id]})
        return True
    except Exception as e:
        logger.error(f"add_favorite_reciter error: {e}")
        return False

async def remove_favorite_reciter(user_id: int, reciter_id: int):
    try:
        ref = db.collection("users").document(str(user_id)).collection("favorites").document("reciters")
        ref.update({"ids": firestore.ArrayRemove([reciter_id])})
        return True
    except Exception as e:
        logger.error(f"remove_favorite_reciter error: {e}")
        return False

async def is_favorite_reciter(user_id: int, reciter_id: int):
    favs = await get_favorite_reciters(user_id)
    return reciter_id in favs


# ==================== الإذاعات المفضلة ====================

async def get_favorite_radios(user_id: int) -> list[str]:
    try:
        doc = db.collection("users").document(str(user_id)).collection("favorites").document("radios").get()
        if doc.exists:
            return list((doc.to_dict() or {}).get("ids", []))
        return []
    except Exception as e:
        logger.error(f"get_favorite_radios error: {e}")
        return []


async def add_favorite_radio(user_id: int, radio_id: str) -> bool:
    try:
        ref = db.collection("users").document(str(user_id)).collection("favorites").document("radios")
        doc = ref.get()
        if doc.exists:
            ref.update({"ids": firestore.ArrayUnion([str(radio_id)])})
        else:
            ref.set({"ids": [str(radio_id)]})
        return True
    except Exception as e:
        logger.error(f"add_favorite_radio error: {e}")
        return False


async def remove_favorite_radio(user_id: int, radio_id: str) -> bool:
    try:
        ref = db.collection("users").document(str(user_id)).collection("favorites").document("radios")
        ref.update({"ids": firestore.ArrayRemove([str(radio_id)])})
        return True
    except Exception as e:
        logger.error(f"remove_favorite_radio error: {e}")
        return False


async def is_favorite_radio(user_id: int, radio_id: str) -> bool:
    favs = await get_favorite_radios(user_id)
    return str(radio_id) in favs

# ==================== التحديات ====================
# المستند يُخزَّن باسم: {ctype}_{date_str}  (مثال: daily_2026-05-12 / weekly_2026-05-09)
# الحقول: ctype, challenge_text, challenge_date, completed_at, points_earned, [week_start, week_end]

def _challenge_doc_id(ctype: str, date_str: str) -> str:
    return f"{ctype}_{date_str}"

async def get_completed_challenges_count(user_id: int, ctype: str = None):
    try:
        col = db.collection("users").document(str(user_id)).collection("completed_challenges")
        docs = col.stream()
        if ctype:
            return sum(1 for d in docs if (d.to_dict() or {}).get("ctype") == ctype)
        return sum(1 for _ in docs)
    except Exception as e:
        logger.error(f"get_completed_challenges_count error: {e}")
        return 0

async def is_challenge_completed_today(user_id: int, ctype: str):
    """daily → اليوم نفسه (UTC). weekly → نفس السبت كبداية أسبوع."""
    try:
        if ctype == "daily":
            date_str = datetime.utcnow().strftime("%Y-%m-%d")
        else:
            from datetime import timedelta as _td
            today = datetime.utcnow()
            days_since_sat = (today.weekday() - 5) % 7
            date_str = (today - _td(days=days_since_sat)).strftime("%Y-%m-%d")
        doc = db.collection("users").document(str(user_id)).collection(
            "completed_challenges").document(_challenge_doc_id(ctype, date_str)).get()
        return doc.exists
    except Exception as e:
        logger.error(f"is_challenge_completed_today error: {e}")
        return False

async def save_completed_challenge(user_id: int, ctype: str, challenge_text: str,
                                   date_str: str, week_start: str = None,
                                   week_end: str = None):
    try:
        data = {
            "ctype": ctype,
            "challenge_text": challenge_text,
            "challenge_date": date_str,
            "completed_at": datetime.utcnow(),
        }
        if week_start: data["week_start"] = week_start
        if week_end:   data["week_end"]   = week_end
        db.collection("users").document(str(user_id)).collection(
            "completed_challenges").document(_challenge_doc_id(ctype, date_str)).set(data)
        return True
    except Exception as e:
        logger.error(f"save_completed_challenge error: {e}")
        return False

async def undo_challenge_completion(user_id: int, ctype: str, date_str: str):
    try:
        db.collection("users").document(str(user_id)).collection(
            "completed_challenges").document(_challenge_doc_id(ctype, date_str)).delete()
        return True
    except Exception as e:
        logger.error(f"undo_challenge_completion error: {e}")
        return False

async def get_completed_challenges_list(user_id: int, ctype: str = None, limit: int = 200):
    try:
        col = db.collection("users").document(str(user_id)).collection("completed_challenges")
        q = col.order_by("completed_at", direction=firestore.Query.DESCENDING).limit(limit)
        docs = q.stream()
        items = [d.to_dict() for d in docs]
        if ctype:
            items = [i for i in items if i.get("ctype") == ctype]
        return items
    except Exception as e:
        logger.error(f"get_completed_challenges_list error: {e}")
        return []

# ==================== 👥 نظام دعوة الأصدقاء (Referrals) ====================
# يخزَّن في حقل users/{uid}: referrer_id (مرة واحدة فقط)
# عداد invitees في users/{inviter}/referrals → list/count

async def get_referral_count(user_id: int) -> int:
    try:
        ref = db.collection("users").document(str(user_id)).collection("referrals").document("_summary").get()
        if ref.exists:
            return int((ref.to_dict() or {}).get("count", 0))
        return 0
    except Exception as e:
        logger.error(f"get_referral_count: {e}")
        return 0

async def register_referral(inviter_id: int, new_user_id: int) -> bool:
    """يسجِّل دعوة جديدة بأمان عبر Firestore Transaction (ذرّي + idempotent)."""
    import asyncio as _asyncio
    try:
        if int(inviter_id) == int(new_user_id):
            return False

        new_user_ref   = _get_doc_ref("users", new_user_id)
        invited_doc    = (db.collection("users").document(str(inviter_id))
                          .collection("referrals").document(str(new_user_id)))
        summary_doc    = (db.collection("users").document(str(inviter_id))
                          .collection("referrals").document("_summary"))

        @firestore.transactional
        def _txn(transaction):
            snap = new_user_ref.get(transaction=transaction)
            if snap.exists:
                existing = (snap.to_dict() or {}).get("referrer_id")
                if existing:
                    return False
            now = datetime.utcnow()
            transaction.set(new_user_ref, {
                "referrer_id": int(inviter_id),
                "referred_at": now,
            }, merge=True)
            transaction.set(invited_doc, {
                "user_id": int(new_user_id),
                "joined_at": now,
            })
            transaction.set(summary_doc, {"count": firestore.Increment(1)}, merge=True)
            return True

        # نشغّل المعاملة في thread لأنها blocking في Firestore SDK
        ok = await _asyncio.to_thread(_txn, db.transaction())
        if not ok:
            return False

        # المكافأة (+20 نقطة) خارج المعاملة — مع log idempotent
        try:
            await add_user_points(int(inviter_id), 20, "referral",
                                  f"دعوة المستخدم {new_user_id}")
        except Exception as e:
            logger.warning(f"referral points award failed: {e}")
        return True
    except Exception as e:
        logger.error(f"register_referral error: {e}")
        return False

async def get_top_referrers(limit: int = 5):
    """يعيد قائمة [{user_id, count, name?}] لأعلى الداعين."""
    try:
        users = db.collection("users").stream()
        rows = []
        for u in users:
            uid = u.id
            try:
                s = db.collection("users").document(uid).collection("referrals").document("_summary").get()
                cnt = int((s.to_dict() or {}).get("count", 0)) if s.exists else 0
                if cnt > 0:
                    name = (u.to_dict() or {}).get("first_name") or (u.to_dict() or {}).get("username") or "مستخدم"
                    rows.append({"user_id": int(uid), "count": cnt, "name": name})
            except Exception:
                continue
        rows.sort(key=lambda r: -r["count"])
        return rows[:limit]
    except Exception as e:
        logger.error(f"get_top_referrers: {e}")
        return []

# ==================== 📊 سجل اختبارات (Quiz history مفصّل) ====================

async def save_quiz_session(user_id: int, payload: dict):
    """يحفظ جلسة اختبار كاملة (الصح/الغلط/الأسئلة الخاطئة/الوقت/النسبة)."""
    try:
        col = db.collection("users").document(str(user_id)).collection("quiz_sessions")
        payload = {**payload, "created_at": datetime.utcnow()}
        col.add(payload)
        return True
    except Exception as e:
        logger.error(f"save_quiz_session: {e}")
        return False

async def get_quiz_sessions(user_id: int, limit: int = 20):
    try:
        col = db.collection("users").document(str(user_id)).collection("quiz_sessions")
        docs = col.order_by("created_at", direction=firestore.Query.DESCENDING).limit(limit).stream()
        return [d.to_dict() for d in docs]
    except Exception as e:
        logger.error(f"get_quiz_sessions: {e}")
        return []

# ==================== تنبيهات الصلاة ====================

async def get_users_with_prayer_alerts():
    try:
        docs = db.collection("users").stream()
        return [doc.to_dict() for doc in docs]
    except Exception as e:
        logger.error(f"get_users_with_prayer_alerts error: {e}")
        return []

async def log_alert_sent(user_id: int, alert_type: str, date_str: str = None):
    try:
        if not date_str:
            date_str = datetime.utcnow().strftime("%Y-%m-%d")
        db.collection("users").document(str(user_id)).collection("alerts_sent").document(f"{date_str}_{alert_type}").set({
            "alert_type": alert_type,
            "sent_at": datetime.utcnow(),
            "date": date_str
        })
        return True
    except Exception as e:
        logger.error(f"log_alert_sent error: {e}")
        return False

async def was_alert_sent(user_id: int, alert_type: str, date_str: str = None):
    try:
        if not date_str:
            date_str = datetime.utcnow().strftime("%Y-%m-%d")
        doc = db.collection("users").document(str(user_id)).collection("alerts_sent").document(f"{date_str}_{alert_type}").get()
        return doc.exists
    except Exception as e:
        logger.error(f"was_alert_sent error: {e}")
        return False

# ==================== الإدارة ====================

async def get_broadcast_by_id(broadcast_id: str):
    try:
        doc = db.collection("broadcasts").document(broadcast_id).get()
        return doc.to_dict() if doc.exists else None
    except Exception as e:
        logger.error(f"get_broadcast_by_id error: {e}")
        return None

async def delete_broadcast_log(broadcast_id: str):
    try:
        db.collection("broadcasts").document(broadcast_id).delete()
        return True
    except Exception as e:
        logger.error(f"delete_broadcast_log error: {e}")
        return False

async def save_broadcast_log(admin_id: int, message_text: str, target_count: int, success_count: int, failed_count: int):
    try:
        db.collection("broadcasts").add({
            "admin_id": admin_id,
            "message_text": message_text,
            "target_count": target_count,
            "success_count": success_count,
            "failed_count": failed_count,
            "timestamp": datetime.utcnow()
        })
        return True
    except Exception as e:
        logger.error(f"save_broadcast_log error: {e}")
        return False

async def get_broadcast_logs(limit=10):
    try:
        docs = db.collection("broadcasts").order_by("timestamp", direction=firestore.Query.DESCENDING).limit(limit).stream()
        return [doc.to_dict() for doc in docs]
    except Exception as e:
        logger.error(f"get_broadcast_logs error: {e}")
        return []

async def get_last_broadcast():
    try:
        docs = db.collection("broadcasts").order_by("timestamp", direction=firestore.Query.DESCENDING).limit(1).get()
        if docs:
            return docs[0].to_dict()
        return None
    except Exception as e:
        logger.error(f"get_last_broadcast error: {e}")
        return None

async def get_mandatory_channels():
    try:
        docs = db.collection("mandatory_channels").where("is_active", "==", True).stream()
        return [doc.to_dict() for doc in docs]
    except Exception as e:
        logger.error(f"get_mandatory_channels error: {e}")
        return []

async def add_mandatory_channel(channel_id: str, title: str, invite_link: str):
    try:
        db.collection("mandatory_channels").document(channel_id).set({
            "channel_id": channel_id,
            "channel_name": title,
            "invite_link": invite_link,
            "is_active": True,
            "created_at": datetime.utcnow()
        })
        return True
    except Exception as e:
        logger.error(f"add_mandatory_channel error: {e}")
        return False

async def remove_mandatory_channel(channel_id: str):
    try:
        db.collection("mandatory_channels").document(channel_id).update({"is_active": False})
        return True
    except Exception as e:
        logger.error(f"remove_mandatory_channel error: {e}")
        return False

async def get_notification_settings(user_id: int):
    try:
        doc = db.collection("users").document(str(user_id)).collection("notifications").document("settings").get()
        if doc.exists:
            return doc.to_dict()
        return None
    except Exception as e:
        logger.error(f"get_notification_settings error: {e}")
        return None

async def update_notifications(user_id: int, settings: dict):
    try:
        db.collection("users").document(str(user_id)).collection("notifications").document("settings").set(settings, merge=True)
        return True
    except Exception as e:
        logger.error(f"update_notifications error: {e}")
        return False

# ==================== المسابقات ====================

async def save_quiz_answer(user_id: int, category: str, question: str, user_answer: str, correct_answer: str, is_correct: bool):
    try:
        db.collection("users").document(str(user_id)).collection("quiz_answers").add({
            "category": category,
            "question": question,
            "user_answer": user_answer,
            "correct_answer": correct_answer,
            "is_correct": is_correct,
            "answered_at": datetime.utcnow()
        })
        return True
    except Exception as e:
        logger.error(f"save_quiz_answer error: {e}")
        return False

async def save_quiz_result(user_id: int, category: str, total: int, correct: int, wrong: int, percentage: float):
    try:
        db.collection("users").document(str(user_id)).collection("quiz_results").add({
            "category": category,
            "total_questions": total,
            "correct_answers": correct,
            "wrong_answers": wrong,
            "percentage": percentage,
            "completed_at": datetime.utcnow()
        })
        return True
    except Exception as e:
        logger.error(f"save_quiz_result error: {e}")
        return False

async def get_quiz_answered_questions(user_id: int, category: str):
    try:
        query = db.collection("users").document(str(user_id)).collection("quiz_answers")
        if category != "الكل":
            query = query.where("category", "==", category)
        docs = query.stream()
        return [doc.to_dict().get("question") for doc in docs]
    except Exception as e:
        logger.error(f"get_quiz_answered_questions error: {e}")
        return []

async def get_last_quiz_result(user_id: int, category: str):
    try:
        query = db.collection("users").document(str(user_id)).collection("quiz_results")
        if category != "الكل":
            query = query.where("category", "==", category)
        docs = query.order_by("completed_at", direction=firestore.Query.DESCENDING).limit(1).get()
        if docs:
            return docs[0].to_dict()
        return None
    except Exception as e:
        logger.error(f"get_last_quiz_result error: {e}")
        return None

async def clear_quiz_progress(user_id: int, category: str):
    try:
        query = db.collection("users").document(str(user_id)).collection("quiz_answers")
        if category != "الكل":
            query = query.where("category", "==", category)
        docs = query.stream()
        for doc in docs:
            doc.reference.delete()
        return True
    except Exception as e:
        logger.error(f"clear_quiz_progress error: {e}")
        return False

async def get_user_full_stats(user_id: int):
    try:
        user_doc = db.collection("users").document(str(user_id)).get()
        user_data = user_doc.to_dict() if user_doc.exists else {}

        khatma_doc = db.collection("users").document(str(user_id)).collection("khatma_progress").document("current").get()
        khatma_data = khatma_doc.to_dict() if khatma_doc.exists else {}

        quiz_results = list(db.collection("users").document(str(user_id)).collection("quiz_results").stream())
        quiz_answers = list(db.collection("users").document(str(user_id)).collection("quiz_answers").stream())
        challenges = list(db.collection("users").document(str(user_id)).collection("completed_challenges").stream())

        action_counts = user_data.get("action_counts", {})

        return {
            "points": user_data.get("points", 0),
            "streak": user_data.get("streak", 0),
            "khatma_page": khatma_data.get("current_page", 0),
            "khatma_completed": khatma_data.get("completed_khatmat", 0),
            "quiz_results_count": len(quiz_results),
            "quiz_answers_count": len(quiz_answers),
            "challenges_count": len(challenges),
            "total_actions": user_data.get("total_actions", 0),
            "action_counts": action_counts,
            "top_actions": sorted(action_counts.items(), key=lambda x: x[1], reverse=True)[:5] if action_counts else [],
        }
    except Exception as e:
        logger.error(f"get_user_full_stats error: {e}")
        return {}

async def get_detailed_stats():
    try:
        total_users = await get_all_users_count()
        active_7d = await get_active_users_count(7)
        active_24h = await get_active_users_count(1)
        new_today = await get_new_users_count(1)
        banned = await get_ban_count()
        return {
            "total_users": total_users,
            "active_7d": active_7d,
            "active_24h": active_24h,
            "new_today": new_today,
            "banned": banned,
        }
    except Exception as e:
        logger.error(f"get_detailed_stats error: {e}")
        return {}

# ==================== القوائم المتصدرة الشهرية ====================

async def get_messages_count(days=1):
    try:
        cutoff = datetime.utcnow() - timedelta(days=days)
        cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%S")
        query = db.collection("stats").where("timestamp", ">=", cutoff_str)
        docs = query.stream()
        return sum(1 for _ in docs)
    except Exception as e:
        logger.error(f"get_messages_count error: {e}")
        return 0

async def get_monthly_top_readers(limit=5):
    try:
        month_id = datetime.utcnow().strftime("%Y-%m")
        query = db.collection("monthly_leaderboards").document(month_id).collection("users")\
            .order_by("pages_read", direction=firestore.Query.DESCENDING).limit(limit)
        docs = query.stream()
        return [doc.to_dict() for doc in docs]
    except Exception as e:
        logger.error(f"get_monthly_top_readers error: {e}")
        return []

# ==================== [v2] إضافات لوحة الأدمن المتقدمة ====================

# ---------- نظام الرتب والصلاحيات ----------
ADMIN_ROLES = {
    "owner":      {"label": "👑 مالك البوت",  "level": 100},
    "partner":    {"label": "⭐ شريك",         "level": 90},
    "admin1":     {"label": "🛡 مشرف أول",   "level": 80},
    "admin2":     {"label": "⚙️ مشرف ثاني",   "level": 60},
    "observer":   {"label": "📋 مراقب",       "level": 30},
}

DEFAULT_PERMISSIONS = {
    "broadcasts":         False,
    "ban_users":          False,
    "manage_users":       False,
    "edit_settings":      False,
    "view_stats":         True,
    "manage_forced_sub":  False,
    "view_user_msgs":     False,
    "notify_new_users":   False,
    "edit_sections":      False,
    "manage_protection":  False,
    "manage_alerts":      False,
    "manage_admins":      False,
}

ROLE_PERMISSIONS = {
    "owner": {k: True for k in DEFAULT_PERMISSIONS},
    "partner": {**{k: True for k in DEFAULT_PERMISSIONS}, "manage_admins": False},
    "admin1": {**DEFAULT_PERMISSIONS, "broadcasts": True, "ban_users": True,
               "manage_users": True, "edit_settings": True, "manage_forced_sub": True,
               "view_user_msgs": True, "notify_new_users": True,
               "manage_protection": True, "manage_alerts": True},
    "admin2": {**DEFAULT_PERMISSIONS, "broadcasts": True, "ban_users": True,
               "manage_users": True, "view_user_msgs": True, "notify_new_users": True},
    "observer": {**DEFAULT_PERMISSIONS, "view_stats": True, "view_user_msgs": True},
}

PERMISSION_LABELS = {
    "broadcasts":        "📢 الإذاعات",
    "ban_users":         "🚫 الحظر",
    "manage_users":      "👥 إدارة المستخدمين",
    "edit_settings":     "⚙️ تعديل الإعدادات",
    "view_stats":        "📊 رؤية الإحصائيات",
    "manage_forced_sub": "🚸 الاشتراك الإجباري",
    "view_user_msgs":    "📩 رسائل المستخدمين",
    "notify_new_users":  "👾 إشعارات الدخول",
    "edit_sections":     "📂 تعديل الأقسام",
    "manage_protection": "🛡 إدارة الحماية",
    "manage_alerts":     "🔔 إدارة التنبيهات",
    "manage_admins":     "👑 إدارة المشرفين",
}


async def get_admin(user_id: int):
    """يرجع وثيقة المشرف أو None"""
    try:
        d = db.collection("admins").document(str(user_id)).get()
        return d.to_dict() if d.exists else None
    except Exception:
        return None


async def get_admin_role(user_id: int) -> str:
    try:
        from config import ADMIN_ID
        if user_id == ADMIN_ID:
            return "owner"
        doc = _get_doc_ref("admins", user_id).get()
        if doc.exists:
            return doc.to_dict().get("role", "admin2")
        return ""
    except Exception as e:
        logger.error(f"get_admin_role error: {e}")
        return ""


async def has_permission(user_id: int, perm: str) -> bool:
    try:
        from config import ADMIN_ID, ADMIN_IDS
        if user_id == ADMIN_ID or user_id in ADMIN_IDS:
            return True
        doc = _get_doc_ref("admins", user_id).get()
        if not doc.exists:
            return False
        data = doc.to_dict()
        if data.get("is_blocked"):
            return False
        role = data.get("role", "admin2")
        perms = data.get("permissions") or ROLE_PERMISSIONS.get(role, DEFAULT_PERMISSIONS)
        return bool(perms.get(perm, False))
    except Exception as e:
        logger.error(f"has_permission error: {e}")
        return False


async def promote_admin_v2(user_id: int, username: str, first_name: str,
                            promoted_by: int, role: str = "admin2",
                            permissions: dict = None):
    try:
        if not permissions:
            permissions = ROLE_PERMISSIONS.get(role, DEFAULT_PERMISSIONS).copy()
        data = {
            "user_id": user_id,
            "username": username or "",
            "first_name": first_name or "",
            "role": role,
            "permissions": permissions,
            "promoted_by": promoted_by,
            "is_blocked": False,
            "stats": {"commands_used": 0, "broadcasts_sent": 0,
                      "bans_issued": 0, "edits_made": 0},
            "created_at": datetime.utcnow(),
        }
        _get_doc_ref("admins", user_id).set(data, merge=True)
        return True
    except Exception as e:
        logger.error(f"promote_admin_v2 error: {e}")
        return False


async def update_admin_role(user_id: int, role: str):
    try:
        perms = ROLE_PERMISSIONS.get(role, DEFAULT_PERMISSIONS).copy()
        _get_doc_ref("admins", user_id).update({"role": role, "permissions": perms})
        return True
    except Exception as e:
        logger.error(f"update_admin_role error: {e}")
        return False


async def update_admin_permission(user_id: int, perm: str, value: bool):
    try:
        _get_doc_ref("admins", user_id).update({f"permissions.{perm}": value})
        return True
    except Exception as e:
        logger.error(f"update_admin_permission error: {e}")
        return False


async def block_admin(user_id: int, blocked: bool = True):
    try:
        _get_doc_ref("admins", user_id).update({"is_blocked": blocked})
        return True
    except Exception as e:
        logger.error(f"block_admin error: {e}")
        return False


async def increment_admin_stat(user_id: int, stat_key: str, by: int = 1):
    try:
        if not await is_admin(user_id):
            return
        _get_doc_ref("admins", user_id).update({
            f"stats.{stat_key}": firestore.Increment(by),
            "last_action_at": datetime.utcnow(),
        })
    except Exception as e:
        logger.error(f"increment_admin_stat error: {e}")


# ---------- سجل الإذاعات v2 ----------
async def save_broadcast_v2(broadcast_data: dict) -> str:
    try:
        ref = db.collection("broadcasts").document()
        broadcast_data["id"] = ref.id
        broadcast_data.setdefault("timestamp", datetime.utcnow())
        ref.set(broadcast_data)
        return ref.id
    except Exception as e:
        logger.error(f"save_broadcast_v2 error: {e}")
        return ""


async def update_broadcast(broadcast_id: str, updates: dict):
    try:
        db.collection("broadcasts").document(broadcast_id).update(updates)
        return True
    except Exception as e:
        logger.error(f"update_broadcast error: {e}")
        return False


async def get_broadcasts_paginated(limit: int = 10, offset: int = 0):
    try:
        docs = db.collection("broadcasts")\
            .order_by("timestamp", direction=firestore.Query.DESCENDING)\
            .limit(limit).offset(offset).stream()
        items = []
        for d in docs:
            data = d.to_dict()
            data["id"] = d.id
            items.append(data)
        return items
    except Exception as e:
        logger.error(f"get_broadcasts_paginated error: {e}")
        return []


async def count_broadcasts() -> int:
    try:
        return sum(1 for _ in db.collection("broadcasts").stream())
    except Exception:
        return 0


async def get_broadcast(broadcast_id: str):
    try:
        doc = db.collection("broadcasts").document(broadcast_id).get()
        if not doc.exists:
            return None
        data = doc.to_dict()
        data["id"] = doc.id
        return data
    except Exception as e:
        logger.error(f"get_broadcast error: {e}")
        return None


async def delete_broadcast(broadcast_id: str):
    try:
        db.collection("broadcasts").document(broadcast_id).delete()
        return True
    except Exception as e:
        logger.error(f"delete_broadcast error: {e}")
        return False


# ---------- رسائل المستخدمين الواردة للبوت ----------
async def log_user_message(user_id: int, user_name: str, username: str,
                            message_text: str, message_type: str = "text",
                            file_id: str = None):
    try:
        data = {
            "user_id": user_id,
            "user_name": user_name or "",
            "username": username or "",
            "message_text": (message_text or "")[:1000],
            "message_type": message_type,
            "file_id": file_id or "",
            "timestamp": datetime.utcnow(),
        }
        db.collection("user_messages").add(data)
        # كذلك في sub-collection للمستخدم
        db.collection("users").document(str(user_id))\
            .collection("messages_sent").add(data)
        return True
    except Exception as e:
        logger.error(f"log_user_message error: {e}")
        return False


async def get_user_messages(user_id: int, limit: int = 20):
    try:
        docs = db.collection("users").document(str(user_id))\
            .collection("messages_sent")\
            .order_by("timestamp", direction=firestore.Query.DESCENDING)\
            .limit(limit).stream()
        return [d.to_dict() for d in docs]
    except Exception as e:
        logger.error(f"get_user_messages error: {e}")
        return []


async def count_user_messages(user_id: int) -> int:
    try:
        return sum(1 for _ in db.collection("users").document(str(user_id))
                                .collection("messages_sent").stream())
    except Exception:
        return 0


# ---------- إحصائيات شاملة ----------
async def get_total_action_count(action: str = None) -> int:
    """مجموع كل العمليات أو نوع محدد عبر كل المستخدمين"""
    try:
        total = 0
        for d in db.collection("users").stream():
            data = d.to_dict()
            if action:
                total += int(data.get("action_counts", {}).get(action, 0))
            else:
                total += int(data.get("total_actions", 0))
        return total
    except Exception as e:
        logger.error(f"get_total_action_count error: {e}")
        return 0


async def get_action_breakdown() -> dict:
    """توزيع العمليات حسب النوع عبر كل المستخدمين"""
    try:
        breakdown = {}
        for d in db.collection("users").stream():
            counts = d.to_dict().get("action_counts", {})
            for k, v in counts.items():
                breakdown[k] = breakdown.get(k, 0) + int(v)
        return breakdown
    except Exception as e:
        logger.error(f"get_action_breakdown error: {e}")
        return {}


async def get_users_by_country() -> dict:
    try:
        countries = {}
        for d in db.collection("users").stream():
            country = d.to_dict().get("country", "غير محدد")
            countries[country] = countries.get(country, 0) + 1
        return dict(sorted(countries.items(), key=lambda x: -x[1]))
    except Exception as e:
        logger.error(f"get_users_by_country error: {e}")
        return {}


async def get_silent_users_count(days: int = 30) -> int:
    """مستخدمون غير نشطين منذ X يوم"""
    try:
        cutoff = datetime.utcnow() - timedelta(days=days)
        return sum(1 for d in db.collection("users").stream()
                   if d.to_dict().get("last_activity")
                   and d.to_dict()["last_activity"] < cutoff)
    except Exception as e:
        logger.error(f"get_silent_users_count error: {e}")
        return 0


async def get_top_reciter_id() -> tuple:
    """أكثر قارئ استماعاً (بناءً على default_reciter_id)"""
    try:
        counts = {}
        for d in db.collection("users").stream():
            rid = d.to_dict().get("default_reciter_id")
            if rid:
                counts[rid] = counts.get(rid, 0) + 1
        if not counts:
            return (None, 0)
        top = max(counts.items(), key=lambda x: x[1])
        return top
    except Exception as e:
        logger.error(f"get_top_reciter_id error: {e}")
        return (None, 0)


# ---------- القنوات المستهدفة ----------
async def update_channel_target(channel_id: str, target_members: int):
    try:
        db.collection("mandatory_channels").document(channel_id).update({
            "target_members": target_members,
            "milestones_sent": [],
        })
        return True
    except Exception as e:
        logger.error(f"update_channel_target error: {e}")
        return False


async def update_channel_count(channel_id: str, current_count: int):
    try:
        db.collection("mandatory_channels").document(channel_id).update({
            "current_members": current_count,
            "last_check": datetime.utcnow(),
        })
        return True
    except Exception:
        return False


# ---------- المواسم ----------
async def reset_weekly_leaderboard():
    """يحفظ نسخة من المتصدرين الأسبوع الحالي ثم يصفّر"""
    try:
        week_id = datetime.utcnow().strftime("%Y-W%U")
        users = list(db.collection("leaderboards").document(week_id)
                       .collection("users").stream())
        snap_ref = db.collection("leaderboard_seasons").document(f"weekly_{week_id}")
        winners = []
        for d in users[:10]:
            winners.append(d.to_dict())
        snap_ref.set({
            "season_type": "weekly",
            "season_id": week_id,
            "ended_at": datetime.utcnow(),
            "winners": winners,
        })
        # تصفير
        for d in users:
            d.reference.delete()
        return True
    except Exception as e:
        logger.error(f"reset_weekly_leaderboard error: {e}")
        return False


async def reset_monthly_leaderboard():
    try:
        month_id = datetime.utcnow().strftime("%Y-%m")
        users = list(db.collection("monthly_leaderboards").document(month_id)
                       .collection("users").stream())
        snap_ref = db.collection("leaderboard_seasons").document(f"monthly_{month_id}")
        winners = [d.to_dict() for d in users[:10]]
        snap_ref.set({
            "season_type": "monthly",
            "season_id": month_id,
            "ended_at": datetime.utcnow(),
            "winners": winners,
        })
        for d in users:
            d.reference.delete()
        return True
    except Exception as e:
        logger.error(f"reset_monthly_leaderboard error: {e}")
        return False


async def remove_user_from_leaderboard(user_id: int):
    try:
        week_id = datetime.utcnow().strftime("%Y-W%U")
        month_id = datetime.utcnow().strftime("%Y-%m")
        db.collection("leaderboards").document(week_id).collection("users")\
            .document(str(user_id)).delete()
        db.collection("monthly_leaderboards").document(month_id).collection("users")\
            .document(str(user_id)).delete()
        return True
    except Exception as e:
        logger.error(f"remove_user_from_leaderboard error: {e}")
        return False


# ---------- نشاط المشرفين ----------
async def get_admin_actions_by_admin(admin_id: int, limit: int = 20):
    try:
        docs = db.collection("admin_logs")\
            .where("admin_id", "==", admin_id)\
            .order_by("timestamp", direction=firestore.Query.DESCENDING)\
            .limit(limit).stream()
        return [d.to_dict() for d in docs]
    except Exception as e:
        logger.error(f"get_admin_actions_by_admin error: {e}")
        return []


async def count_admin_actions_by_admin(admin_id: int) -> int:
    try:
        return sum(1 for _ in db.collection("admin_logs")
                   .where("admin_id", "==", admin_id).stream())
    except Exception:
        return 0


async def reset_user_points(user_id: int):
    try:
        _get_doc_ref("users", user_id).update({"points": 0})
        return True
    except Exception:
        return False


async def delete_user_data(user_id: int):
    """حذف بيانات المستخدم بالكامل (يشمل sub-collections)"""
    try:
        user_ref = db.collection("users").document(str(user_id))
        # حذف كل sub-collections المعروفة
        for sub in [
            "khatma_progress", "messages_sent", "completed_challenges",
            "quiz_results", "quiz_answers", "favorite_reciters",
            "fav_reciters", "notifications", "settings",
            "stats", "history", "challenges_progress",
        ]:
            try:
                for d in user_ref.collection(sub).stream():
                    d.reference.delete()
            except Exception:
                pass
        user_ref.delete()
        # إزالة من المتصدرين أيضاً
        try:
            await remove_user_from_leaderboard(user_id)
        except Exception:
            pass
        return True
    except Exception as e:
        logger.error(f"delete_user_data error: {e}")
        return False


# ==================== [v3] CRUD الأقسام + تتبع مستلمي الإذاعات ====================

DEFAULT_MAIN_SECTIONS = [
    {"section_key": "menu_quran",      "title_ar": "القرآن الكريم",  "emoji": "📖", "order_index": 1},
    {"section_key": "menu_reciters",   "title_ar": "القراء",          "emoji": "🎧", "order_index": 2},
    {"section_key": "menu_tafsir",     "title_ar": "التفسير",         "emoji": "📚", "order_index": 3},
    {"section_key": "menu_adhkar",     "title_ar": "الأذكار",         "emoji": "📿", "order_index": 4},
    {"section_key": "menu_prayer",     "title_ar": "مواقيت الصلاة",   "emoji": "🕌", "order_index": 5},
    {"section_key": "menu_khatma",     "title_ar": "الختمة",          "emoji": "📖", "order_index": 6},
    {"section_key": "menu_quiz",       "title_ar": "المسابقات",       "emoji": "🏆", "order_index": 7},
    {"section_key": "menu_challenges", "title_ar": "التحديات",        "emoji": "🔥", "order_index": 8},
    {"section_key": "top_users",       "title_ar": "أفضل المستخدمين", "emoji": "🏆", "order_index": 9},
    {"section_key": "menu_more",       "title_ar": "المزيد",          "emoji": "📜", "order_index": 10},
]


async def ensure_default_sections(parent: str = "main") -> bool:
    """يستورد الأقسام الافتراضية إذا لم توجد أقسام بعد لهذه القائمة"""
    try:
        any_doc = list(db.collection("dynamic_sections")
                         .where("parent_menu", "==", parent).limit(1).stream())
        if any_doc:
            return False
        for s in DEFAULT_MAIN_SECTIONS:
            doc_id = s["section_key"]
            db.collection("dynamic_sections").document(doc_id).set({
                **s,
                "parent_menu": parent,
                "is_hidden": False,
                "is_deleted": False,
                "visibility": "all",
                "created_at": datetime.utcnow(),
            })
        return True
    except Exception as e:
        logger.error(f"ensure_default_sections error: {e}")
        return False


async def get_all_sections(parent: str = "main", include_hidden: bool = True):
    """يرجع جميع الأقسام (المخفية أيضاً) مرتبة"""
    try:
        docs = db.collection("dynamic_sections")\
            .where("parent_menu", "==", parent).stream()
        secs = []
        for d in docs:
            data = d.to_dict()
            if data.get("is_deleted"):
                continue
            if not include_hidden and data.get("is_hidden"):
                continue
            data["doc_id"] = d.id
            secs.append(data)
        secs.sort(key=lambda x: x.get("order_index", 999))
        return secs
    except Exception as e:
        logger.error(f"get_all_sections error: {e}")
        return []


async def upsert_section(doc_id: str, data: dict) -> bool:
    try:
        db.collection("dynamic_sections").document(doc_id).set(data, merge=True)
        return True
    except Exception as e:
        logger.error(f"upsert_section error: {e}")
        return False


async def update_section_field(doc_id: str, field: str, value) -> bool:
    try:
        db.collection("dynamic_sections").document(doc_id).update({field: value})
        return True
    except Exception as e:
        logger.error(f"update_section_field error: {e}")
        return False


async def hard_delete_section(doc_id: str) -> bool:
    try:
        db.collection("dynamic_sections").document(doc_id).delete()
        return True
    except Exception as e:
        logger.error(f"hard_delete_section error: {e}")
        return False


async def move_section_order(doc_id: str, parent: str, direction: int) -> bool:
    """direction: -1 للأعلى، +1 للأسفل (تبديل ترتيب مع الجار)"""
    try:
        secs = await get_all_sections(parent, include_hidden=True)
        idx = next((i for i, s in enumerate(secs) if s["doc_id"] == doc_id), -1)
        if idx < 0:
            return False
        new_idx = idx + direction
        if new_idx < 0 or new_idx >= len(secs):
            return False
        a, b = secs[idx], secs[new_idx]
        a_order = a.get("order_index", idx)
        b_order = b.get("order_index", new_idx)
        if a_order == b_order:
            b_order = a_order + direction
        await update_section_field(a["doc_id"], "order_index", b_order)
        await update_section_field(b["doc_id"], "order_index", a_order)
        return True
    except Exception as e:
        logger.error(f"move_section_order error: {e}")
        return False


# ---------- مستلمو الإذاعات (لحذف الرسائل من محادثاتهم) ----------
async def save_broadcast_recipients_batch(broadcast_id: str, items: list):
    """items: قائمة (user_id, message_id) — يكتب على دفعات 400"""
    try:
        if not items:
            return
        col = db.collection("broadcasts").document(broadcast_id).collection("recipients")
        chunk = 400
        for i in range(0, len(items), chunk):
            batch = db.batch()
            for uid, mid in items[i:i + chunk]:
                ref = col.document(str(uid))
                batch.set(ref, {"user_id": uid, "message_id": mid})
            batch.commit()
    except Exception as e:
        logger.error(f"save_broadcast_recipients_batch error: {e}")


async def get_broadcast_recipients(broadcast_id: str):
    try:
        docs = db.collection("broadcasts").document(broadcast_id)\
                 .collection("recipients").stream()
        return [d.to_dict() for d in docs]
    except Exception as e:
        logger.error(f"get_broadcast_recipients error: {e}")
        return []


async def delete_broadcast_recipients(broadcast_id: str) -> bool:
    try:
        col = db.collection("broadcasts").document(broadcast_id).collection("recipients")
        for d in col.stream():
            d.reference.delete()
        return True
    except Exception as e:
        logger.error(f"delete_broadcast_recipients error: {e}")
        return False


# ==================== 🛡️ سجل الحماية + التحذيرات ====================

async def log_protection_violation(user_id: int, kind: str, action: str, snippet: str = "") -> bool:
    """يسجّل مخالفة حماية + يزيد عدّاد التحذيرات للمستخدم."""
    try:
        db.collection("protection_log").add({
            "user_id": int(user_id),
            "kind": kind,
            "action": action,
            "snippet": (snippet or "")[:300],
            "at": datetime.utcnow(),
        })
        # عدّاد تحذيرات
        uref = db.collection("users").document(str(user_id))
        snap = uref.get()
        warns = 0
        if snap.exists:
            warns = int((snap.to_dict() or {}).get("prot_warnings", 0) or 0)
        warns += 1
        uref.set({"prot_warnings": warns, "prot_last_at": datetime.utcnow()}, merge=True)
        return True
    except Exception as e:
        logger.error(f"log_protection_violation error: {e}")
        return False


async def get_protection_log(page: int = 1, per_page: int = 8):
    """يرجع (items, total) — مرتبة الأحدث أولاً."""
    try:
        from google.cloud.firestore import Query
        col = db.collection("protection_log")
        total = sum(1 for _ in col.stream())
        q = col.order_by("at", direction=Query.DESCENDING).limit(per_page * page)
        items = []
        for d in q.stream():
            data = d.to_dict() or {}
            data["doc_id"] = d.id
            items.append(data)
        # خذ الصفحة المطلوبة
        start = (page - 1) * per_page
        return items[start:start + per_page], total
    except Exception as e:
        logger.error(f"get_protection_log error: {e}")
        return [], 0


async def get_user_warnings(user_id: int) -> int:
    try:
        snap = db.collection("users").document(str(user_id)).get()
        if not snap.exists:
            return 0
        return int((snap.to_dict() or {}).get("prot_warnings", 0) or 0)
    except Exception as e:
        logger.error(f"get_user_warnings error: {e}")
        return 0


async def reset_user_warnings(user_id: int) -> bool:
    try:
        db.collection("users").document(str(user_id)).set(
            {"prot_warnings": 0}, merge=True)
        return True
    except Exception:
        return False


async def mute_user(user_id: int, hours: int = 24) -> bool:
    try:
        until = datetime.utcnow() + timedelta(hours=hours)
        db.collection("users").document(str(user_id)).set(
            {"muted_until": until}, merge=True)
        return True
    except Exception as e:
        logger.error(f"mute_user error: {e}")
        return False


async def is_user_muted(user_id: int) -> bool:
    try:
        snap = db.collection("users").document(str(user_id)).get()
        if not snap.exists:
            return False
        data = snap.to_dict() or {}
        until = data.get("muted_until")
        if not until:
            return False
        if isinstance(until, str):
            try:
                until = datetime.fromisoformat(until.replace("Z", "+00:00"))
            except Exception:
                return False
        if hasattr(until, "tzinfo") and until.tzinfo:
            until = until.replace(tzinfo=None)
        return until > datetime.utcnow()
    except Exception:
        return False


# ============================================================
# 👥 الختمة الجماعية — Group Khatma
# ============================================================
import secrets as _secrets

KHATMA_GROUP_TOTAL_PAGES = 604
KHATMA_GROUP_MAX_PAGES_PER_USER = 100  # حدّ أقصى لحجز صفحات في طلب واحد


def _gen_invite_code() -> str:
    """رمز دعوة من 6 خانات (أحرف كبيرة وأرقام)، بدون 0/O/1/I لتجنّب الالتباس."""
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(_secrets.choice(alphabet) for _ in range(6))


async def create_khatma_group(owner_id: int, owner_name: str, name: str):
    """ينشئ ختمة جماعية جديدة. يعيد (group_id, invite_code) أو (None, None)."""
    try:
        for _ in range(5):
            invite_code = _gen_invite_code()
            existing = list(db.collection("khatma_groups")
                              .where("invite_code", "==", invite_code).limit(1).stream())
            if not existing:
                break
        else:
            return None, None
        doc_ref = db.collection("khatma_groups").document()
        doc_ref.set({
            "name": (name or "ختمة جماعية").strip()[:60],
            "owner_id": int(owner_id),
            "owner_name": owner_name or "صاحب الختمة",
            "invite_code": invite_code,
            "members": [int(owner_id)],
            "members_meta": {str(owner_id): {"name": owner_name or "صاحب الختمة"}},
            "assignments": {},
            "is_completed": False,
            "created_at": datetime.utcnow(),
        })
        # ربط بالمستخدم
        db.collection("users").document(str(owner_id))\
          .collection("khatma_groups").document(doc_ref.id)\
          .set({"group_id": doc_ref.id, "joined_at": datetime.utcnow(),
                "role": "owner"}, merge=True)
        return doc_ref.id, invite_code
    except Exception as e:
        logger.error(f"create_khatma_group error: {e}")
        return None, None


async def get_khatma_group(group_id: str):
    try:
        doc = db.collection("khatma_groups").document(group_id).get()
        if not doc.exists:
            return None
        data = doc.to_dict() or {}
        data["_id"] = doc.id
        return data
    except Exception as e:
        logger.error(f"get_khatma_group error: {e}")
        return None


async def get_khatma_group_by_invite(invite_code: str):
    try:
        invite_code = (invite_code or "").strip().upper()
        if not invite_code:
            return None
        docs = list(db.collection("khatma_groups")
                      .where("invite_code", "==", invite_code).limit(1).stream())
        if not docs:
            return None
        data = docs[0].to_dict() or {}
        data["_id"] = docs[0].id
        return data
    except Exception as e:
        logger.error(f"get_khatma_group_by_invite error: {e}")
        return None


async def join_khatma_group(group_id: str, user_id: int, user_name: str):
    """يضيف مستخدمًا للمجموعة. يعيد True إذا انضم، False إذا فشل أو موجود مسبقًا."""
    try:
        ref = db.collection("khatma_groups").document(group_id)
        doc = ref.get()
        if not doc.exists:
            return False
        data = doc.to_dict() or {}
        members = data.get("members", []) or []
        uid = int(user_id)
        if uid in members:
            return True
        members.append(uid)
        meta = data.get("members_meta", {}) or {}
        meta[str(uid)] = {"name": user_name or "عضو"}
        ref.update({"members": members, "members_meta": meta})
        db.collection("users").document(str(uid))\
          .collection("khatma_groups").document(group_id)\
          .set({"group_id": group_id, "joined_at": datetime.utcnow(),
                "role": "member"}, merge=True)
        return True
    except Exception as e:
        logger.error(f"join_khatma_group error: {e}")
        return False


async def leave_khatma_group(group_id: str, user_id: int):
    """يخرج المستخدم ويُحرّر صفحاته غير المقروءة."""
    try:
        ref = db.collection("khatma_groups").document(group_id)
        doc = ref.get()
        if not doc.exists:
            return False
        data = doc.to_dict() or {}
        uid = int(user_id)
        # المالك لا يستطيع الخروج (يحذف بدلاً من ذلك)
        if int(data.get("owner_id", 0)) == uid:
            return False
        members = [m for m in (data.get("members", []) or []) if int(m) != uid]
        meta = data.get("members_meta", {}) or {}
        meta.pop(str(uid), None)
        # تحرير الصفحات غير المقروءة
        assignments = data.get("assignments", {}) or {}
        new_assignments = {}
        for page_str, info in assignments.items():
            if int(info.get("user_id", 0)) == uid and not info.get("is_read"):
                continue  # حرّر هذه الصفحة
            new_assignments[page_str] = info
        ref.update({"members": members, "members_meta": meta,
                    "assignments": new_assignments})
        db.collection("users").document(str(uid))\
          .collection("khatma_groups").document(group_id).delete()
        return True
    except Exception as e:
        logger.error(f"leave_khatma_group error: {e}")
        return False


async def take_pages_in_group(group_id: str, user_id: int, user_name: str, count: int):
    """يحجز أوّل N صفحة متاحة للمستخدم. يعيد قائمة الصفحات المحجوزة."""
    try:
        count = max(1, min(KHATMA_GROUP_MAX_PAGES_PER_USER, int(count)))
        ref = db.collection("khatma_groups").document(group_id)
        doc = ref.get()
        if not doc.exists:
            return []
        data = doc.to_dict() or {}
        if int(user_id) not in (data.get("members", []) or []):
            return []
        assignments = data.get("assignments", {}) or {}
        taken = []
        for p in range(1, KHATMA_GROUP_TOTAL_PAGES + 1):
            if str(p) not in assignments:
                assignments[str(p)] = {
                    "user_id": int(user_id),
                    "user_name": user_name or "عضو",
                    "is_read": False,
                    "assigned_at": datetime.utcnow(),
                }
                taken.append(p)
                if len(taken) >= count:
                    break
        if taken:
            ref.update({"assignments": assignments})
        return taken
    except Exception as e:
        logger.error(f"take_pages_in_group error: {e}")
        return []


async def mark_group_page_read(group_id: str, page: int, user_id: int):
    """يؤشر صفحة كمقروءة. يعيد (success, all_completed)."""
    try:
        ref = db.collection("khatma_groups").document(group_id)
        doc = ref.get()
        if not doc.exists:
            return False, False
        data = doc.to_dict() or {}
        assignments = data.get("assignments", {}) or {}
        info = assignments.get(str(page))
        if not info or int(info.get("user_id", 0)) != int(user_id):
            return False, False
        if info.get("is_read"):
            return True, False  # سبق تأشيرها
        info["is_read"] = True
        info["read_at"] = datetime.utcnow()
        assignments[str(page)] = info
        # هل اكتملت كل الصفحات؟
        all_done = (
            len(assignments) >= KHATMA_GROUP_TOTAL_PAGES and
            all(a.get("is_read") for a in assignments.values())
        )
        update = {"assignments": assignments}
        if all_done and not data.get("is_completed"):
            update["is_completed"] = True
            update["completed_at"] = datetime.utcnow()
        ref.update(update)
        return True, all_done
    except Exception as e:
        logger.error(f"mark_group_page_read error: {e}")
        return False, False


async def get_user_khatma_groups(user_id: int):
    """يعيد قائمة مجموعات المستخدم (id + name + role)."""
    try:
        docs = db.collection("users").document(str(user_id))\
                 .collection("khatma_groups").stream()
        out = []
        for d in docs:
            entry = d.to_dict() or {}
            gid = entry.get("group_id") or d.id
            g = await get_khatma_group(gid)
            if g:
                out.append({
                    "group_id": gid,
                    "name": g.get("name", "ختمة جماعية"),
                    "role": entry.get("role", "member"),
                    "is_completed": g.get("is_completed", False),
                    "members_count": len(g.get("members", []) or []),
                })
            else:
                # المجموعة لم تعد موجودة، نظّف المرجع
                try:
                    db.collection("users").document(str(user_id))\
                      .collection("khatma_groups").document(d.id).delete()
                except Exception:
                    pass
        return out
    except Exception as e:
        logger.error(f"get_user_khatma_groups error: {e}")
        return []


async def get_user_pages_in_group(group_id: str, user_id: int, only_unread: bool = False):
    """يعيد صفحات المستخدم في المجموعة، مرتّبة."""
    try:
        g = await get_khatma_group(group_id)
        if not g:
            return []
        assignments = g.get("assignments", {}) or {}
        uid = int(user_id)
        out = []
        for page_str, info in assignments.items():
            if int(info.get("user_id", 0)) != uid:
                continue
            if only_unread and info.get("is_read"):
                continue
            out.append({"page": int(page_str), "is_read": bool(info.get("is_read"))})
        out.sort(key=lambda x: x["page"])
        return out
    except Exception as e:
        logger.error(f"get_user_pages_in_group error: {e}")
        return []


async def delete_khatma_group(group_id: str, owner_id: int):
    """يحذف المجموعة (المالك فقط) ويُحرّر مراجع الأعضاء."""
    try:
        ref = db.collection("khatma_groups").document(group_id)
        doc = ref.get()
        if not doc.exists:
            return False
        data = doc.to_dict() or {}
        if int(data.get("owner_id", 0)) != int(owner_id):
            return False
        for uid in (data.get("members", []) or []):
            try:
                db.collection("users").document(str(uid))\
                  .collection("khatma_groups").document(group_id).delete()
            except Exception:
                pass
        ref.delete()
        return True
    except Exception as e:
        logger.error(f"delete_khatma_group error: {e}")
        return False
