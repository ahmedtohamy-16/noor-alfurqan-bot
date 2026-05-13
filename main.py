'''النقطة الرئيسية لتشغيل البوت - مع دعم حماية المحتوى والتحقق من الصيانة'''
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from telegram import Update, constants
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ConversationHandler, MessageHandler, filters, ContextTypes,
    TypeHandler, ApplicationHandlerStop
)
from config import BOT_TOKEN, ADMIN_ID
from db_firebase import init_db, get_user, is_admin as db_is_admin, get_bot_setting

from handlers.start_handler import (
    start_command, handle_main_menu, handle_more_menu,
    language_callback, sources_info, check_maintenance, check_forced_subscription,
    fsub_recheck
)
from handlers.quran_handler import (
    quran_menu, surah_list_page, surah_list_select_page,
    surah_detail, verse_list_page, verse_list_select_page,
    verse_detail, quran_search_start, quran_search_input,
    quran_search_cancel, SEARCH_INPUT,
    quran_pages, quran_pages_index, quran_page_view, quran_listen, verse_listen,
    listen_menu, listen_surahs_page, listen_surah_open, listen_view,
    verse_image,
)
from handlers.reciter_handler import (
    reciters_list, reciters_select_page, reciter_detail,
    fav_add, fav_remove, reciters_favorites,
    moshaf_detail, moshaf_page, surah_audio,
    reciter_search_start, reciter_search_input, reciter_search_cancel,
    RECITER_SEARCH_INPUT
)
from handlers.tafsir_handler import (
    tafsir_menu, tafsir_surah_list, tafsir_surah_verses,
    tafsir_select, tafsir_view
)
from handlers.prayer_handler import (
    prayer_times, country_list_callback, country_select_callback,
    city_select_callback, change_location_start, change_location_input,
    change_location_cancel, LOCATION_INPUT
)
from handlers.prayer_calc_handler import calc_method_menu, calc_method_set

from handlers.adhkar_handler import (
    adhkar_menu, adhkar_morning, adhkar_evening,
    adhkar_categories, adhkar_category_detail
)
from handlers.khatma_handler import (
    khatma_menu, khatma_choose_days, khatma_set_days, khatma_set_time,
    khatma_read, khatma_log, khatma_reset_confirm, khatma_reset_yes,
    khatma_continue,
)
from handlers.khatma_group_handler import (
    khatma_group_menu, khatma_group_view, khatma_group_take,
    khatma_group_my_pages, khatma_group_read_page, khatma_group_done_page,
    khatma_group_noop, khatma_group_leave_confirm, khatma_group_leave_yes,
    khatma_group_delete_confirm, khatma_group_delete_yes,
    khatma_group_create_start, khatma_group_create_save,
    khatma_group_join_start, khatma_group_join_save, khatma_group_cancel,
    KHATMA_GROUP_NAME_INPUT, KHATMA_GROUP_INVITE_INPUT,
)
from handlers.adhkar_handler import adhkar_categories_jump
from handlers.quiz_handler import quiz_cats_jump
from handlers.tafsir_handler import tafsir_sl_jump, tafsir_sv_jump
from handlers.read_tafsir_handler import rt_sl_jump, rt_sv_jump
from handlers.challenges_handler import chist_jump
from handlers.quiz_handler import (
    quiz_menu, quiz_categories_page, quiz_select_category,
    quiz_set_count, quiz_answer, quiz_next,
    quiz_get_result, quiz_reset, quiz_restart
)
from handlers.challenges_handler import (
    challenges_menu, challenge_daily, challenge_weekly,
    challenge_complete, challenge_undo, challenge_stats, challenge_history
)
from handlers.extras_handler import (
    names_of_allah, names_callback, name_view, name_image, names_pdf, names_noop,
    hadith_menu, hadith_bukhari, hadith_book_detail, hadith_view, hadith_muslim,
    ruqyah_list, prophets_list, prophet_detail,
    tajweed_list
)
from handlers.radio_handler import (
    radio_menu, radio_live, radio_live_jump, radio_archive, radio_favs,
    radio_play, radio_fav_add, radio_fav_remove, radio_noop,
)
from handlers.notifications_handler import (
    settings_menu, settings_reciter, set_default_reciter,
    sreciter_select, sreciter_confirm, sreciter_set_moshaf,
    settings_tafsir, set_default_tafsir,
    settings_language, set_language,
    settings_location, settings_request_gps, handle_location_message,
    settings_notifications, toggle_notification,
    change_alert_minutes, set_alert_minutes,
    settings_salawat, salawat_toggle, salawat_set_times, salawat_set_times_value,
    salawat_set_quiet, salawat_set_quiet_value,
    notif_test_menu, notif_test_send
)
from handlers.user_stats_handler import my_stats
from handlers.referral_handler import referral_menu, referral_top
from handlers.prayer_reminder import check_prayer_alerts
from handlers.admin_handler import (
    admin_command, admin_menu_callback, admin_stats,
    admin_stats_countries, admin_stats_actions,
    admin_broadcast_start, admin_broadcast_input,
    admin_broadcast_log_view, admin_broadcast_view, admin_broadcast_delete,
    admin_users_list, admin_inspect_user,
    admin_search_user_start, admin_search_user_input,
    admin_toggle_block, admin_unblock_user, admin_ban_log,
    admin_user_messages, admin_user_notifs, admin_user_activity,
    admin_reset_points, admin_lb_remove,
    admin_user_delete, admin_user_delete_confirm,
    admin_prompt_points, admin_add_points_input,
    admin_reply_start, admin_reply_send,
    admin_promote_user, admin_demote_user, admin_toggle_rank,
    admin_list_admins, admin_view_admin, admin_chrole, admin_setrole,
    admin_perms, admin_perm_toggle, admin_admin_block, admin_admin_logs,
    admin_add_admin_start, admin_add_admin_input,
    admin_activity, admin_recent_actions,
    admin_settings, admin_set_toggle, admin_bot_operation,
    admin_edit_maint_msg_start, admin_edit_maint_msg_input,
    admin_edit_start_view, admin_edit_start_text_start,
    admin_edit_start_text_save, admin_edit_start_reset,
    admin_edit_start_preview,
    admin_content_protection, admin_manage_sections, admin_sect_toggle,
    admin_sect_view, admin_sect_up, admin_sect_down, admin_sect_seed,
    admin_sect_del, admin_sect_delyes,
    admin_sect_add_start, admin_sect_add_input,
    admin_sect_rename_start, admin_sect_rename_input,
    admin_sect_cancel_to_list, admin_sect_pages_picker,
    admin_prot_action, admin_prot_setact,
    admin_prot_words, admin_prot_words_edit_start, admin_prot_words_save,
    admin_prot_log, PROT_WORDS_INPUT,
    admin_forced_sub, admin_channels_list, admin_ch_view, admin_ch_remove,
    admin_ch_target_start, admin_ch_target_save,
    admin_add_channel_start, admin_add_channel_input, admin_test_sub,
    admin_leaderboard, admin_lb_reset_weekly, admin_lb_reset_monthly,
    admin_lb_reset_weekly_yes, admin_lb_reset_monthly_yes,
    admin_broadcast_delete_menu, admin_broadcast_delete_log_confirm,
    admin_broadcast_delete_log_yes,
    admin_broadcast_delete_users_confirm, admin_broadcast_delete_users_yes,
    admin_broadcast_delete_all_confirm, admin_broadcast_delete_all_yes,
    admin_cancel, handle_user_message,
    BROADCAST_INPUT, USER_SEARCH_INPUT, ADMIN_REPLY_INPUT,
    ADD_ADMIN_INPUT, ADD_POINTS_INPUT,
    EDIT_START_TEXT, ADD_CHANNEL_INPUT, EDIT_MAINT_MSG, SET_CHANNEL_TARGET,
    ADD_SECTION_INPUT, RENAME_SECTION_INPUT,
)
from handlers.wudu_handler import (
    wudu_menu, wudu_quick, wudu_step, wudu_command
)
from handlers.read_tafsir_handler import (
    read_tafsir_surah_list, read_tafsir_verse_list,
    read_tafsir_view, read_tafsir_change, read_tafsir_set_and_view
)
from handlers.top_users_handler import top_users_menu, top_weekly, top_monthly, top_last_week
from handlers.quran_image_handler import (
    quran_image_menu, quran_image_page_input, quran_image_ayah_input,
    quran_image_ayahs_input, handle_quran_image_input, random_ayah_image,
    quran_image_cancel, alt_image_handler, quran_image_daily, QURAN_IMAGE_INPUT
)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

BLOCKED_TEXT = (
    "🚫 *تم حظر حسابك*\n\n"
    "لقد تم حظرك من استخدام بوت نور الفرقان.\n\n"
    "إذا كنت تعتقد أن هذا خطأ، يرجى التواصل معنا:\n"
    "📩 @tohamy_official"
)


async def is_user_blocked(user_id: int) -> bool:
    user = await get_user(user_id)
    return bool(user and user.get("is_blocked"))


async def send_blocked_message(update: Update):
    if update.callback_query:
        await update.callback_query.answer("🚫 تم حظر حسابك من استخدام البوت", show_alert=True)
        try:
            await update.callback_query.edit_message_text(BLOCKED_TEXT, parse_mode="Markdown")
        except Exception:
            pass
    elif update.message:
        await update.message.reply_text(BLOCKED_TEXT, parse_mode="Markdown")


async def global_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return
    user_id = update.effective_user.id

    if user_id == ADMIN_ID or await db_is_admin(user_id):
        return

    if await is_user_blocked(user_id):
        await send_blocked_message(update)
        raise ApplicationHandlerStop

    if not await check_maintenance(update):
        raise ApplicationHandlerStop


async def post_init(application):
    await init_db()
    is_prot = await get_bot_setting("content_protection_enabled", "false")
    if is_prot.lower() == "true":
        pass


async def post_shutdown(application):
    from db_firebase import _close_db
    await _close_db()


def main():
    from telegram.request import HTTPXRequest
    # ⚡ تحسين الأداء: pool كبير + timeouts قصيرة + http2
    request = HTTPXRequest(
        connection_pool_size=256,
        connect_timeout=10,
        read_timeout=20,
        write_timeout=20,
        pool_timeout=5,
        http_version="1.1",
    )
    get_updates_request = HTTPXRequest(
        connection_pool_size=64,
        connect_timeout=10,
        read_timeout=40,
        pool_timeout=5,
        http_version="1.1",
    )
    app = (Application.builder()
           .token(BOT_TOKEN)
           .request(request)
           .get_updates_request(get_updates_request)
           .concurrent_updates(True)
           .post_init(post_init)
           .post_shutdown(post_shutdown)
           .build())

    # ===================== Global Check =====================
    app.add_handler(TypeHandler(Update, global_check), group=-1)

    # ===================== Commands =====================
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(CommandHandler("settings", settings_menu))
    app.add_handler(CommandHandler("wudu", wudu_command))

    # ===================== Conversation Handlers =====================
    conv_handlers = [
        ConversationHandler(
            entry_points=[CallbackQueryHandler(quran_search_start, pattern=r"^quran_search$")],
            states={SEARCH_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, quran_search_input)]},
            fallbacks=[CallbackQueryHandler(quran_search_cancel, pattern=r"^menu_quran$"), CommandHandler("start", start_command)],
            per_message=False,
        ),
        ConversationHandler(
            entry_points=[CallbackQueryHandler(reciter_search_start, pattern=r"^reciter_search$")],
            states={RECITER_SEARCH_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, reciter_search_input)]},
            fallbacks=[CallbackQueryHandler(reciter_search_cancel, pattern=r"^reciters_list_page_\d+$"), CommandHandler("start", start_command)],
            per_message=False,
        ),
        ConversationHandler(
            entry_points=[CallbackQueryHandler(change_location_start, pattern=r"^loc_manual$")],
            states={LOCATION_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, change_location_input)]},
            fallbacks=[CallbackQueryHandler(change_location_cancel, pattern=r"^loc_country_list_1$"), CommandHandler("start", start_command)],
            per_message=False,
        ),
        ConversationHandler(
            entry_points=[CallbackQueryHandler(admin_broadcast_start, pattern=r"^admin_broadcast$")],
            states={BROADCAST_INPUT: [MessageHandler(
                (filters.TEXT | filters.PHOTO | filters.VIDEO | filters.Document.ALL
                 | filters.AUDIO | filters.VOICE) & ~filters.COMMAND,
                admin_broadcast_input)]},
            fallbacks=[CallbackQueryHandler(admin_cancel, pattern=r"^admin_menu$"), CommandHandler("start", start_command)],
            per_message=False,
        ),
        ConversationHandler(
            entry_points=[CallbackQueryHandler(admin_edit_maint_msg_start, pattern=r"^admin_edit_maint_msg$")],
            states={EDIT_MAINT_MSG: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_edit_maint_msg_input)]},
            fallbacks=[CallbackQueryHandler(admin_cancel, pattern=r"^admin_bot_operation$"), CommandHandler("start", start_command)],
            per_message=False,
        ),
        ConversationHandler(
            entry_points=[CallbackQueryHandler(admin_edit_start_text_start, pattern=r"^admin_edit_start_text$")],
            states={EDIT_START_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_edit_start_text_save)]},
            fallbacks=[CallbackQueryHandler(admin_cancel, pattern=r"^admin_edit_start$"), CommandHandler("start", start_command)],
            per_message=False,
        ),
        ConversationHandler(
            entry_points=[CallbackQueryHandler(admin_add_channel_start, pattern=r"^admin_add_channel$")],
            states={ADD_CHANNEL_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_channel_input)]},
            fallbacks=[CallbackQueryHandler(admin_cancel, pattern=r"^admin_forced_sub$"), CommandHandler("start", start_command)],
            per_message=False,
        ),
        ConversationHandler(
            entry_points=[CallbackQueryHandler(admin_ch_target_start, pattern=r"^admin_ch_target_.+$")],
            states={SET_CHANNEL_TARGET: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_ch_target_save)]},
            fallbacks=[CallbackQueryHandler(admin_cancel, pattern=r"^admin_ch_view_.+$"), CommandHandler("start", start_command)],
            per_message=False,
        ),
        ConversationHandler(
            entry_points=[CallbackQueryHandler(admin_reply_start, pattern=r"^admin_reply_\d+$")],
            states={ADMIN_REPLY_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_reply_send)]},
            fallbacks=[
                CallbackQueryHandler(admin_cancel, pattern=r"^admin_menu$"),
                CallbackQueryHandler(admin_inspect_user, pattern=r"^admin_inspect_\d+$"),
                CommandHandler("start", start_command),
            ],
            per_message=False,
        ),
        ConversationHandler(
            entry_points=[CallbackQueryHandler(admin_search_user_start, pattern=r"^admin_search_user$")],
            states={USER_SEARCH_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_search_user_input)]},
            fallbacks=[CallbackQueryHandler(admin_cancel, pattern=r"^admin_menu$"), CommandHandler("start", start_command)],
            per_message=False,
        ),
        ConversationHandler(
            entry_points=[CallbackQueryHandler(admin_add_admin_start, pattern=r"^admin_add_admin$")],
            states={ADD_ADMIN_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_admin_input)]},
            fallbacks=[CallbackQueryHandler(admin_cancel, pattern=r"^admin_list_admins$"), CommandHandler("start", start_command)],
            per_message=False,
        ),
        ConversationHandler(
            entry_points=[CallbackQueryHandler(admin_prompt_points, pattern=r"^admin_prompt_points_\d+$")],
            states={ADD_POINTS_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_points_input)]},
            fallbacks=[
                CallbackQueryHandler(admin_cancel, pattern=r"^admin_menu$"),
                CallbackQueryHandler(admin_inspect_user, pattern=r"^admin_inspect_\d+$"),
                CommandHandler("start", start_command),
            ],
            per_message=False,
        ),
        # إضافة قسم جديد
        ConversationHandler(
            entry_points=[CallbackQueryHandler(admin_sect_add_start, pattern=r"^admin_sect_add$")],
            states={ADD_SECTION_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_sect_add_input)]},
            fallbacks=[CallbackQueryHandler(admin_sect_cancel_to_list, pattern=r"^admin_manage_sections$"),
                       CommandHandler("start", start_command)],
            per_message=False,
        ),
        # إعادة تسمية قسم
        ConversationHandler(
            entry_points=[CallbackQueryHandler(admin_sect_rename_start, pattern=r"^admin_sect_rename_\w+$")],
            states={RENAME_SECTION_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_sect_rename_input)]},
            fallbacks=[CallbackQueryHandler(admin_sect_cancel_to_list, pattern=r"^admin_manage_sections$"),
                       CommandHandler("start", start_command)],
            per_message=False,
        ),
        # ✏️ تعديل الكلمات المحظورة
        ConversationHandler(
            entry_points=[CallbackQueryHandler(admin_prot_words_edit_start, pattern=r"^admin_prot_words_edit$")],
            states={PROT_WORDS_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_prot_words_save)]},
            fallbacks=[
                CallbackQueryHandler(admin_prot_words, pattern=r"^admin_prot_words$"),
                CallbackQueryHandler(admin_content_protection, pattern=r"^admin_content_protection$"),
                CallbackQueryHandler(admin_settings, pattern=r"^admin_settings$"),
                CommandHandler("start", start_command),
                CommandHandler("cancel", admin_cancel),
            ],
            per_message=False,
        ),
        # 👥 إنشاء ختمة جماعية (طلب اسم)
        ConversationHandler(
            entry_points=[CallbackQueryHandler(khatma_group_create_start, pattern=r"^khg_create$")],
            states={KHATMA_GROUP_NAME_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, khatma_group_create_save)]},
            fallbacks=[
                CallbackQueryHandler(khatma_group_cancel, pattern=r"^khatma_group_menu$"),
                CommandHandler("start", start_command),
            ],
            per_message=False,
        ),
        # 👥 الانضمام لمجموعة برمز
        ConversationHandler(
            entry_points=[CallbackQueryHandler(khatma_group_join_start, pattern=r"^khg_join$")],
            states={KHATMA_GROUP_INVITE_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, khatma_group_join_save)]},
            fallbacks=[
                CallbackQueryHandler(khatma_group_cancel, pattern=r"^khatma_group_menu$"),
                CommandHandler("start", start_command),
            ],
            per_message=False,
        ),
    ]
    app.add_handlers(conv_handlers)

    quran_img_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(quran_image_page_input, pattern=r"^qimg_page$"),
            CallbackQueryHandler(quran_image_ayah_input, pattern=r"^qimg_ayah$"),
            CallbackQueryHandler(quran_image_ayahs_input, pattern=r"^qimg_ayahs$"),
            CallbackQueryHandler(quran_image_ayahs_input, pattern=r"^qimg_multi$"),
        ],
        states={
            QURAN_IMAGE_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_quran_image_input)],
        },
        fallbacks=[
            CallbackQueryHandler(quran_image_cancel, pattern=r"^quran_images$"),
            CommandHandler("start", start_command),
        ],
        per_message=False,
    )
    app.add_handler(quran_img_conv)

    # ===================== Callback Query Handlers =====================
    callback_handlers = [
        # --- Navigation ---
        CallbackQueryHandler(handle_main_menu, pattern=r"^back_main$"),
        CallbackQueryHandler(handle_more_menu, pattern=r"^menu_more$"),
        CallbackQueryHandler(language_callback, pattern=r"^lang_\w+$"),
        CallbackQueryHandler(sources_info, pattern=r"^sources_info$"),
        # --- Forced Subscription Re-check ---
        CallbackQueryHandler(fsub_recheck, pattern=r"^fsub_recheck$"),

        # --- Quran ---
        CallbackQueryHandler(quran_menu, pattern=r"^menu_quran$"),
        CallbackQueryHandler(surah_list_page, pattern=r"^surah_list_page_\d+$"),
        CallbackQueryHandler(surah_list_select_page, pattern=r"^surah_list_select_page$"),
        CallbackQueryHandler(surah_detail, pattern=r"^surah_detail_\d+$"),
        CallbackQueryHandler(verse_list_page, pattern=r"^verselist_\d+_page_\d+$"),
        CallbackQueryHandler(verse_list_select_page, pattern=r"^verselist_\d+_select_page$"),
        CallbackQueryHandler(verse_detail, pattern=r"^verse_\d+_\d+$"),
        # 🎧 الاستماع بالتقليب — قسم منفصل
        CallbackQueryHandler(listen_menu, pattern=r"^listen_menu$"),
        CallbackQueryHandler(listen_surahs_page, pattern=r"^listen_surahs_\d+$"),
        CallbackQueryHandler(listen_surah_open, pattern=r"^listen_surah_\d+$"),
        CallbackQueryHandler(listen_view, pattern=r"^listen_view_\d+_\d+$"),
        CallbackQueryHandler(listen_view, pattern=r"^listen_next_\d+_\d+$"),
        CallbackQueryHandler(listen_view, pattern=r"^listen_prev_\d+_\d+$"),
        CallbackQueryHandler(quran_pages_index, pattern=r"^quran_pages_index$"),
        CallbackQueryHandler(quran_pages, pattern=r"^quran_pages_\d+$"),
        CallbackQueryHandler(quran_page_view, pattern=r"^quran_page_view_\d+$"),
        CallbackQueryHandler(quran_listen, pattern=r"^quran_listen_\d+$"),
        CallbackQueryHandler(verse_listen, pattern=r"^verse_listen_\d+_\d+$"),
        CallbackQueryHandler(verse_image, pattern=r"^verse_img_\d+_\d+$"),

        # --- Quran Image ---
        CallbackQueryHandler(quran_image_menu, pattern=r"^quran_image_menu$"),
        CallbackQueryHandler(quran_image_menu, pattern=r"^quran_images$"),
        CallbackQueryHandler(quran_image_daily, pattern=r"^qimg_daily$"),
        CallbackQueryHandler(random_ayah_image, pattern=r"^qimg_random$"),
        CallbackQueryHandler(alt_image_handler, pattern=r"^qimg_alt_.+$"),

        # --- Reciters ---
        CallbackQueryHandler(reciters_list, pattern=r"^menu_reciters$"),
        CallbackQueryHandler(reciters_list, pattern=r"^reciters_list_page_\d+$"),
        CallbackQueryHandler(reciters_select_page, pattern=r"^reciters_list_select_page$"),
        CallbackQueryHandler(reciter_detail, pattern=r"^reciter_\d+(_\d+)?$"),
        CallbackQueryHandler(fav_add, pattern=r"^fav_add_\d+_\d+$"),
        CallbackQueryHandler(fav_remove, pattern=r"^fav_remove_\d+_\d+$"),
        CallbackQueryHandler(reciters_favorites, pattern=r"^reciters_favorites$"),
        CallbackQueryHandler(moshaf_detail, pattern=r"^moshaf_\d+_\d+$"),
        CallbackQueryHandler(moshaf_page, pattern=r"^moshaf_page_\d+_\d+_\d+$"),
        CallbackQueryHandler(surah_audio, pattern=r"^surah_audio_\d+_\d+_\d+$"),

        # --- Tafsir ---
        CallbackQueryHandler(tafsir_menu, pattern=r"^menu_tafsir$"),
        CallbackQueryHandler(tafsir_surah_list, pattern=r"^tafsir_surah_list_\d+$"),
        CallbackQueryHandler(tafsir_surah_verses, pattern=r"^tafsir_surah_\d+_\d+$"),
        CallbackQueryHandler(tafsir_select, pattern=r"^tafsir_select_\d+_\d+$"),
        CallbackQueryHandler(tafsir_view, pattern=r"^tview_\w+_\d+_\d+$"),

        # --- Prayer Times ---
        CallbackQueryHandler(prayer_times, pattern=r"^menu_prayer$"),
        CallbackQueryHandler(country_list_callback, pattern=r"^loc_country_list_\d+$"),
        CallbackQueryHandler(country_select_callback, pattern=r"^loc_sel_.+$"),
        CallbackQueryHandler(city_select_callback, pattern=r"^loc_city_.+$"),
        # 🧭 طريقة حساب مواقيت الصلاة
        CallbackQueryHandler(calc_method_menu, pattern=r"^calc_method_menu$"),
        CallbackQueryHandler(calc_method_menu, pattern=r"^calc_method_from_settings$"),
        CallbackQueryHandler(calc_method_set, pattern=r"^calc_method_set_(\d+|auto)$"),

        # --- Adhkar ---
        CallbackQueryHandler(adhkar_menu, pattern=r"^menu_adhkar$"),
        CallbackQueryHandler(adhkar_morning, pattern=r"^adhkar_morning_\d+$"),
        CallbackQueryHandler(adhkar_evening, pattern=r"^adhkar_evening_\d+$"),
        CallbackQueryHandler(adhkar_categories, pattern=r"^adhkar_categories_\d+$"),
        CallbackQueryHandler(adhkar_category_detail, pattern=r"^adhkar_cat_\d+_\d+$"),

        # --- Khatma ---
        CallbackQueryHandler(khatma_menu, pattern=r"^menu_khatma$"),
        CallbackQueryHandler(khatma_choose_days, pattern=r"^khatma_choose_days$"),
        CallbackQueryHandler(khatma_set_days, pattern=r"^khatma_setdays_\d+$"),
        CallbackQueryHandler(khatma_set_time, pattern=r"^khatma_settime_\d+$"),
        CallbackQueryHandler(khatma_read, pattern=r"^khatma_read_\d+$"),
        CallbackQueryHandler(khatma_log, pattern=r"^khatma_log_\d+$"),
        CallbackQueryHandler(khatma_reset_confirm, pattern=r"^khatma_reset_confirm$"),
        CallbackQueryHandler(khatma_reset_yes, pattern=r"^khatma_reset_yes$"),
        CallbackQueryHandler(khatma_continue, pattern=r"^khatma_continue_\d+$"),

        # --- 👥 الختمة الجماعية ---
        CallbackQueryHandler(khatma_group_menu,           pattern=r"^khatma_group_menu$"),
        CallbackQueryHandler(khatma_group_view,           pattern=r"^khg_view_[A-Za-z0-9]+$"),
        CallbackQueryHandler(khatma_group_take,           pattern=r"^khg_take_[A-Za-z0-9]+_\d+$"),
        CallbackQueryHandler(khatma_group_my_pages,       pattern=r"^khg_my_[A-Za-z0-9]+_\d+$"),
        CallbackQueryHandler(khatma_group_read_page,      pattern=r"^khg_read_[A-Za-z0-9]+_\d+$"),
        CallbackQueryHandler(khatma_group_done_page,      pattern=r"^khg_done_[A-Za-z0-9]+_\d+$"),
        CallbackQueryHandler(khatma_group_noop,           pattern=r"^khg_noop$"),
        CallbackQueryHandler(khatma_group_leave_confirm,  pattern=r"^khg_leaveconf_[A-Za-z0-9]+$"),
        CallbackQueryHandler(khatma_group_leave_yes,      pattern=r"^khg_leaveyes_[A-Za-z0-9]+$"),
        CallbackQueryHandler(khatma_group_delete_confirm, pattern=r"^khg_delconf_[A-Za-z0-9]+$"),
        CallbackQueryHandler(khatma_group_delete_yes,     pattern=r"^khg_delyes_[A-Za-z0-9]+$"),

        # --- 🔢 شاشات قفز سريع للصفحات (jump-to-page) ---
        CallbackQueryHandler(adhkar_categories_jump, pattern=r"^adhkar_cats_jump_\d+$"),
        CallbackQueryHandler(quiz_cats_jump,         pattern=r"^quiz_cats_jump_\d+$"),
        CallbackQueryHandler(tafsir_sl_jump,         pattern=r"^tafsir_sl_jump_\d+$"),
        CallbackQueryHandler(tafsir_sv_jump,         pattern=r"^tafsir_sv_jump_\d+_\d+$"),
        CallbackQueryHandler(rt_sl_jump,             pattern=r"^rt_sl_jump_\d+$"),
        CallbackQueryHandler(rt_sv_jump,             pattern=r"^rt_sv_jump_\d+_\d+$"),
        CallbackQueryHandler(chist_jump,             pattern=r"^chist_jump_(daily|weekly)_\d+$"),

        # --- Quiz ---
        CallbackQueryHandler(quiz_menu, pattern=r"^menu_quiz$"),
        CallbackQueryHandler(quiz_categories_page, pattern=r"^quiz_cats_\d+$"),
        CallbackQueryHandler(quiz_select_category, pattern=r"^quiz_cat_.+$"),
        CallbackQueryHandler(quiz_set_count, pattern=r"^quiz_count_\d+$"),
        CallbackQueryHandler(quiz_answer, pattern=r"^quiz_ans_\d+_\d+$"),
        CallbackQueryHandler(quiz_next, pattern=r"^quiz_next$"),
        CallbackQueryHandler(quiz_get_result, pattern=r"^quiz_get_result$"),
        CallbackQueryHandler(quiz_reset, pattern=r"^quiz_reset$"),
        CallbackQueryHandler(quiz_restart, pattern=r"^quiz_restart_.+$"),

        # --- Challenges ---
        CallbackQueryHandler(challenges_menu, pattern=r"^menu_challenges$"),
        CallbackQueryHandler(challenge_daily, pattern=r"^challenge_daily$"),
        CallbackQueryHandler(challenge_weekly, pattern=r"^challenge_weekly$"),
        CallbackQueryHandler(challenge_complete, pattern=r"^challenge_complete_\w+_.+$"),
        CallbackQueryHandler(challenge_undo, pattern=r"^challenge_undo_\w+_.+$"),
        CallbackQueryHandler(challenge_stats, pattern=r"^challenge_stats$"),
        CallbackQueryHandler(challenge_history, pattern=r"^challenge_history_\w+_\d+$"),

        # --- Extras ---
        CallbackQueryHandler(names_of_allah, pattern=r"^names_allah$"),
        CallbackQueryHandler(names_callback, pattern=r"^names_page_\d+$"),
        CallbackQueryHandler(name_view, pattern=r"^name_view_\d+_[01]$"),
        CallbackQueryHandler(name_image, pattern=r"^name_img_\d+_[01]$"),
        CallbackQueryHandler(names_pdf, pattern=r"^names_pdf$"),
        CallbackQueryHandler(names_noop, pattern=r"^noop$"),
        CallbackQueryHandler(hadith_menu, pattern=r"^hadith_menu$"),
        CallbackQueryHandler(hadith_bukhari, pattern=r"^hadith_books_\d+$"),
        CallbackQueryHandler(hadith_book_detail, pattern=r"^hadith_book_\d+_\d+$"),
        CallbackQueryHandler(hadith_view, pattern=r"^hadith_view_\d+_\d+$"),
        CallbackQueryHandler(hadith_muslim, pattern=r"^hadith_muslim$"),
        CallbackQueryHandler(ruqyah_list, pattern=r"^ruqyah_list_\d+$"),
        CallbackQueryHandler(prophets_list, pattern=r"^prophets_list_\d+$"),
        CallbackQueryHandler(prophet_detail, pattern=r"^prophet_\d+_\d+$"),
        # --- Radio (نظام احترافي: مباشرة + أرشيف + مفضلة) ---
        CallbackQueryHandler(radio_menu, pattern=r"^radio_menu$"),
        CallbackQueryHandler(radio_live, pattern=r"^radio_live_\d+$"),
        CallbackQueryHandler(radio_live_jump, pattern=r"^radio_live_jump$"),
        CallbackQueryHandler(radio_archive, pattern=r"^radio_archive_\d+$"),
        CallbackQueryHandler(radio_favs, pattern=r"^radio_favs$"),
        CallbackQueryHandler(radio_fav_add, pattern=r"^radio_fav_(live|archive)_.+_\d+$"),
        CallbackQueryHandler(radio_fav_remove, pattern=r"^radio_unfav_(live|archive)_.+_\d+$"),
        CallbackQueryHandler(radio_play, pattern=r"^radio_play_(live|archive)_.+_\d+$"),
        CallbackQueryHandler(radio_noop, pattern=r"^radio_noop$"),
        # رابط قديم — يحوّل تلقائيًا للقائمة الجديدة
        CallbackQueryHandler(radio_menu, pattern=r"^radio_list_\d+$"),
        CallbackQueryHandler(tajweed_list, pattern=r"^tajweed_list_\d+$"),

        # --- Wudu ---
        CallbackQueryHandler(wudu_menu, pattern=r"^menu_wudu$"),
        CallbackQueryHandler(wudu_quick, pattern=r"^wudu_quick$"),
        CallbackQueryHandler(wudu_step, pattern=r"^wudu_steps_\d+$"),

        # --- Settings - Main ---
        CallbackQueryHandler(settings_menu, pattern=r"^settings_menu$"),
        CallbackQueryHandler(my_stats, pattern=r"^my_stats$"),

        # --- Settings - Reciter (New) ---
        CallbackQueryHandler(settings_reciter, pattern=r"^settings_reciter$"),
        CallbackQueryHandler(settings_reciter, pattern=r"^settings_reciter_page_\d+$"),
        CallbackQueryHandler(sreciter_select, pattern=r"^sreciter_select_\d+$"),
        CallbackQueryHandler(sreciter_confirm, pattern=r"^sreciter_confirm_\d+$"),
        CallbackQueryHandler(sreciter_set_moshaf, pattern=r"^sreciter_moshaf_\d+_\d+$"),

        # --- Settings - Tafsir ---
        CallbackQueryHandler(settings_tafsir, pattern=r"^settings_tafsir$"),
        CallbackQueryHandler(set_default_tafsir, pattern=r"^set_tafsir_\w+$"),

        # --- Settings - Language ---
        CallbackQueryHandler(settings_language, pattern=r"^settings_language$"),
        CallbackQueryHandler(set_language, pattern=r"^set_lang_\w+$"),

        # --- Settings - Location (New) ---
        CallbackQueryHandler(settings_location, pattern=r"^settings_location$"),
        CallbackQueryHandler(settings_request_gps, pattern=r"^settings_request_gps$"),

        # --- Settings - Notifications ---
        CallbackQueryHandler(settings_notifications, pattern=r"^settings_notifications$"),
        CallbackQueryHandler(toggle_notification, pattern=r"^notif_toggle_\w+$"),
        CallbackQueryHandler(change_alert_minutes, pattern=r"^notif_change_alert_mins$"),
        CallbackQueryHandler(set_alert_minutes, pattern=r"^set_alert_mins_\d+$"),

        # --- Salawat ala-Nabi ---
        CallbackQueryHandler(settings_salawat, pattern=r"^settings_salawat$"),
        CallbackQueryHandler(salawat_toggle, pattern=r"^salawat_toggle$"),
        CallbackQueryHandler(salawat_set_times, pattern=r"^salawat_set_times$"),
        CallbackQueryHandler(salawat_set_times_value, pattern=r"^salawat_times_\d+$"),
        CallbackQueryHandler(salawat_set_quiet, pattern=r"^salawat_set_quiet$"),
        CallbackQueryHandler(salawat_set_quiet_value, pattern=r"^salawat_quiet_\d+_\d+$"),

        # --- Test Notifications (Admin) ---
        CallbackQueryHandler(notif_test_menu, pattern=r"^notif_test_menu$"),
        CallbackQueryHandler(notif_test_send, pattern=r"^notif_test_\w+$"),

        # --- Read Tafsir ---
        CallbackQueryHandler(read_tafsir_surah_list, pattern=r"^read_tafsir_surah_list_\d+$"),
        CallbackQueryHandler(read_tafsir_verse_list, pattern=r"^rt_surah_\d+_\d+$"),
        CallbackQueryHandler(read_tafsir_view, pattern=r"^rt_view_\d+_\d+$"),
        CallbackQueryHandler(read_tafsir_change, pattern=r"^rt_change_\d+_\d+$"),
        CallbackQueryHandler(read_tafsir_set_and_view, pattern=r"^rt_set_\w+_\d+_\d+$"),

        # --- Referrals (دعوة الأصدقاء) ---
        CallbackQueryHandler(referral_menu, pattern=r"^menu_referral$"),
        CallbackQueryHandler(referral_top, pattern=r"^referral_top$"),

        # --- Top Users ---
        CallbackQueryHandler(top_users_menu, pattern=r"^top_users$"),
        CallbackQueryHandler(top_last_week, pattern=r"^top_last_week$"),
        CallbackQueryHandler(top_weekly, pattern=r"^top_weekly$"),
        CallbackQueryHandler(top_monthly, pattern=r"^top_monthly$"),

        # --- Admin (v2) ---
        CallbackQueryHandler(admin_menu_callback, pattern=r"^admin_menu$"),
        # Stats
        CallbackQueryHandler(admin_stats, pattern=r"^admin_stats$"),
        CallbackQueryHandler(admin_stats_countries, pattern=r"^admin_stats_countries$"),
        CallbackQueryHandler(admin_stats_actions, pattern=r"^admin_stats_actions$"),
        # Broadcast
        CallbackQueryHandler(admin_broadcast_log_view, pattern=r"^admin_broadcast_log_\d+$"),
        CallbackQueryHandler(admin_broadcast_view, pattern=r"^admin_bcast_view_.+$"),
        # حذف الإذاعة بنوعين مع تأكيد (مسارات منفصلة لتجنّب التعارض)
        CallbackQueryHandler(admin_broadcast_delete_menu, pattern=r"^admin_bcast_dlm_.+$"),
        CallbackQueryHandler(admin_broadcast_delete_log_confirm, pattern=r"^admin_bcast_dlog_.+$"),
        CallbackQueryHandler(admin_broadcast_delete_log_yes, pattern=r"^admin_bcast_xlog_.+$"),
        # نسخة "حذف لدى المستخدمين" الجديدة (لا تلمس السجل)
        CallbackQueryHandler(admin_broadcast_delete_users_confirm, pattern=r"^admin_bcast_dusr_.+$"),
        CallbackQueryHandler(admin_broadcast_delete_users_yes, pattern=r"^admin_bcast_xusr_.+$"),
        # توافق قديم
        CallbackQueryHandler(admin_broadcast_delete_all_confirm, pattern=r"^admin_bcast_dall_.+$"),
        CallbackQueryHandler(admin_broadcast_delete_all_yes, pattern=r"^admin_bcast_xall_.+$"),
        # Users
        CallbackQueryHandler(admin_users_list, pattern=r"^admin_users_\d+$"),
        CallbackQueryHandler(admin_inspect_user, pattern=r"^admin_inspect_\d+$"),
        CallbackQueryHandler(admin_toggle_block, pattern=r"^admin_toggle_block_\d+$"),
        CallbackQueryHandler(admin_unblock_user, pattern=r"^admin_unblock_\d+$"),
        CallbackQueryHandler(admin_ban_log, pattern=r"^admin_banlog_\d+$"),
        CallbackQueryHandler(admin_user_messages, pattern=r"^admin_user_msgs_\d+$"),
        CallbackQueryHandler(admin_user_notifs, pattern=r"^admin_user_notifs_\d+$"),
        CallbackQueryHandler(admin_user_activity, pattern=r"^admin_user_activity_\d+$"),
        CallbackQueryHandler(admin_reset_points, pattern=r"^admin_reset_points_\d+$"),
        CallbackQueryHandler(admin_lb_remove, pattern=r"^admin_lb_remove_\d+$"),
        CallbackQueryHandler(admin_user_delete, pattern=r"^admin_user_delete_\d+$"),
        CallbackQueryHandler(admin_user_delete_confirm, pattern=r"^admin_user_delconf_\d+$"),
        CallbackQueryHandler(admin_promote_user, pattern=r"^admin_promote_\d+$"),
        CallbackQueryHandler(admin_demote_user, pattern=r"^admin_demote_\d+$"),
        CallbackQueryHandler(admin_toggle_rank, pattern=r"^admin_toggle_rank_\d+$"),
        # Admins
        CallbackQueryHandler(admin_list_admins, pattern=r"^admin_list_admins$"),
        CallbackQueryHandler(admin_view_admin, pattern=r"^admin_view_admin_\d+$"),
        CallbackQueryHandler(admin_chrole, pattern=r"^admin_chrole_\d+$"),
        CallbackQueryHandler(admin_setrole, pattern=r"^admin_setrole_\d+_\w+$"),
        CallbackQueryHandler(admin_perms, pattern=r"^admin_perms_\d+$"),
        CallbackQueryHandler(admin_perm_toggle, pattern=r"^admin_perm_toggle_\d+_\w+$"),
        CallbackQueryHandler(admin_admin_block, pattern=r"^admin_admin_block_\d+$"),
        CallbackQueryHandler(admin_admin_logs, pattern=r"^admin_admin_logs_\d+$"),
        CallbackQueryHandler(admin_activity, pattern=r"^admin_activity$"),
        CallbackQueryHandler(admin_recent_actions, pattern=r"^admin_recent_actions$"),
        # Settings
        CallbackQueryHandler(admin_settings, pattern=r"^admin_settings$"),
        CallbackQueryHandler(admin_bot_operation, pattern=r"^admin_bot_operation$"),
        CallbackQueryHandler(admin_forced_sub, pattern=r"^admin_forced_sub$"),
        CallbackQueryHandler(admin_content_protection, pattern=r"^admin_content_protection$"),
        CallbackQueryHandler(admin_manage_sections, pattern=r"^admin_manage_sections$"),
        CallbackQueryHandler(admin_manage_sections, pattern=r"^admin_sect_page_\d+$"),
        CallbackQueryHandler(admin_sect_pages_picker, pattern=r"^admin_sect_pages_picker$"),
        CallbackQueryHandler(admin_sect_view, pattern=r"^admin_sect_view_\w+$"),
        CallbackQueryHandler(admin_sect_toggle, pattern=r"^admin_sect_toggle_\w+$"),
        CallbackQueryHandler(admin_sect_up, pattern=r"^admin_sect_up_\w+$"),
        CallbackQueryHandler(admin_sect_down, pattern=r"^admin_sect_down_\w+$"),
        CallbackQueryHandler(admin_sect_seed, pattern=r"^admin_sect_seed$"),
        CallbackQueryHandler(admin_sect_del, pattern=r"^admin_sect_del_\w+$"),
        CallbackQueryHandler(admin_sect_delyes, pattern=r"^admin_sect_delyes_\w+$"),
        CallbackQueryHandler(admin_set_toggle, pattern=r"^admin_toggle_setting_\w+$"),
        CallbackQueryHandler(admin_edit_start_view, pattern=r"^admin_edit_start$"),
        CallbackQueryHandler(admin_edit_start_preview, pattern=r"^admin_edit_start_preview$"),
        CallbackQueryHandler(admin_edit_start_reset, pattern=r"^admin_edit_start_reset$"),
        # 🛡️ Protection panel
        CallbackQueryHandler(admin_prot_action, pattern=r"^admin_prot_action$"),
        CallbackQueryHandler(admin_prot_setact, pattern=r"^admin_prot_setact_\w+$"),
        CallbackQueryHandler(admin_prot_words, pattern=r"^admin_prot_words$"),
        CallbackQueryHandler(admin_prot_log, pattern=r"^admin_prot_log_\d+$"),
        # Forced sub
        CallbackQueryHandler(admin_channels_list, pattern=r"^admin_channels_list$"),
        CallbackQueryHandler(admin_ch_view, pattern=r"^admin_ch_view_.+$"),
        CallbackQueryHandler(admin_ch_remove, pattern=r"^admin_ch_remove_.+$"),
        CallbackQueryHandler(admin_test_sub, pattern=r"^admin_test_sub$"),
        # Leaderboard
        CallbackQueryHandler(admin_leaderboard, pattern=r"^admin_leaderboard$"),
        CallbackQueryHandler(admin_lb_reset_weekly, pattern=r"^admin_lb_reset_weekly$"),
        CallbackQueryHandler(admin_lb_reset_weekly_yes, pattern=r"^admin_lb_reset_weekly_yes$"),
        CallbackQueryHandler(admin_lb_reset_monthly, pattern=r"^admin_lb_reset_monthly$"),
        CallbackQueryHandler(admin_lb_reset_monthly_yes, pattern=r"^admin_lb_reset_monthly_yes$"),
    ]
    app.add_handlers(callback_handlers)

    # ===================== Message Handlers =====================
    # 🛡️ مرشّح حماية المحتوى — يعمل قبل أي معالج آخر (group=-1)
    from handlers.protection_handler import content_protection_filter
    app.add_handler(MessageHandler(
        (filters.TEXT | filters.PHOTO | filters.VIDEO | filters.VOICE
         | filters.AUDIO | filters.Document.ALL | filters.ANIMATION
         | filters.Sticker.ALL) & ~filters.COMMAND,
        content_protection_filter,
    ), group=-1)

    # موقع GPS من المستخدم
    app.add_handler(MessageHandler(filters.LOCATION, handle_location_message))
    # رسائل المستخدمين (نص/وسائط) — تُحفظ وتُحوّل للمالك
    app.add_handler(MessageHandler(
        (filters.TEXT | filters.PHOTO | filters.VIDEO | filters.VOICE
         | filters.Document.ALL) & ~filters.COMMAND,
        handle_user_message))

    # ===================== Job Queue - تنبيهات الصلاة =====================
    job_queue = app.job_queue
    job_queue.run_repeating(check_prayer_alerts, interval=60, first=10)

    logger.info("✅ بوت نور الفرقان يعمل الآن...")
    # ⚡ polling سريع: poll_interval=0 و timeout=30 لاستجابة فورية
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        poll_interval=0.0,
        timeout=30,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
