# بوت نور الفرقان - سجل التغييرات والإصلاحات

## المراجعة الشاملة - مارس 2026

### 1. الملفات الأساسية

#### config.py
- تحديث بيانات الاتصال (Telegram Token, Supabase URL, Supabase Key, Admin ID)
- إضافة `DATA_DIR` للإشارة لمجلد البيانات

#### db_supabase.py
- تحويل كل الدوال إلى **async** باستخدام `httpx.AsyncClient` مع connection pooling
- إضافة caching ذكي باستخدام `TTLCache` لتقليل طلبات قاعدة البيانات
- إضافة دوال مفقودة: `get_all_users_count`, `get_active_users_count`, `get_detailed_stats`, `update_user_setting`, `is_favorite_reciter`
- إصلاح دالة `get_all_users` لدعم `limit` و `offset` للتصفح
- إضافة error handling شامل لكل الدوال

#### requirements.txt
- تحديث المكتبات المطلوبة بإصدارات متوافقة
- إزالة مكتبات غير مستخدمة (Pillow, python-dotenv)

### 2. utils/

#### api_clients.py
- إضافة `get_session()` لإدارة جلسة `aiohttp` واحدة مشتركة (connection pooling)
- إضافة caching للـ API responses (القراء، التفاسير)
- تحسين `MP3QuranAPI` و `QuranAPI` لاستخدام الجلسة المشتركة
- إضافة `close_session()` للتنظيف عند إيقاف البوت

#### keyboards.py
- إصلاح كل دوال لوحات المفاتيح
- توحيد نظام التنقل بين الصفحات عبر `_nav_buttons()`
- إضافة `surah_page_selector_keyboard` و `reciters_page_selector_keyboard`
- إضافة `notification_settings_keyboard` و `challenges_keyboard`
- التأكد من أن كل `callback_data` فريد وغير متكرر

#### data_loader.py
- إضافة فهارس ذكية (`SURAHS_BY_ID`, `SURAHS_BY_NAME`) لبحث O(1)
- إضافة `_strip_tashkeel()` لبحث أدق في القرآن
- إضافة بيانات مضمنة: أحكام التجويد (6 قواعد)، الرقية الشرعية (15 نص)، قصص الأنبياء (25 نبي)
- إصلاح تحميل الأحاديث (دعم dict و list)

### 3. الـ Handlers

#### start_handler.py
- إصلاح نص المصادر (إزالة الأسطر الفارغة الزائدة)
- إضافة `sources_info` callback
- التأكد من أن كل الأزرار inline

#### quran_handler.py
- إزالة الاستيرادات غير المستخدمة
- إضافة صفحات القرآن (604 صفحة) عبر Quran.com API
- إصلاح `verse_list_page` و `verse_list_select_page`
- كل شيء يعمل بـ `edit_message_text`

#### reciter_handler.py
- إزالة الاستيرادات غير المستخدمة
- إصلاح نظام المفضلة (إضافة/إزالة)
- إصلاح تصفح صفحات القراء

#### tafsir_handler.py
- 7 تفاسير عربية صحيحة من Quran.com API v4
- إصلاح parsing لـ callback_data
- إرسال التفسير بدون parse_mode لتجنب أخطاء Markdown مع نصوص التفسير الطويلة

#### adhkar_handler.py
- إصلاح تصفح الأذكار (صباح/مساء/حصن المسلم)
- إصلاح عداد التكرار

#### prayer_handler.py
- إزالة الاستيرادات غير المستخدمة
- إصلاح نظام اختيار الموقع (دول + مدن)
- إصلاح عرض مواقيت الصلاة

#### khatma_handler.py
- إصلاح نظام الختمة (بدء/استكمال/إعادة)

#### quiz_handler.py
- إصلاح `quiz_menu` callback
- حفظ الإجابة الصحيحة في `context.user_data` لعرضها عند الخطأ
- إضافة 20 سؤال مضمن كاحتياطي

#### extras_handler.py
- أسماء الله الحسنى مع التصفح
- الأحاديث (البخاري + مسلم)
- الرقية الشرعية مع التصفح
- قصص الأنبياء (25 نبي) مع التصفح
- إذاعة القرآن الكريم
- أحكام التجويد

#### challenges_handler.py
- التحدي اليومي والأسبوعي
- نظام النقاط

#### notifications_handler.py
- إعدادات التنبيهات (صلاة/ختمة/أذكار/كهف)
- إعداد القارئ والتفسير الافتراضي
- إعداد دقائق التنبيه قبل الصلاة

#### admin_handler.py
- إزالة الاستيرادات غير المستخدمة
- لوحة تحكم المشرف (إحصائيات/رسالة جماعية/حظر/إلغاء حظر/قائمة المستخدمين)

#### quran_image_handler.py
- تحويل لاستخدام `edit_message_text` فقط (بدلاً من إرسال صور)
- عرض آية عشوائية

#### prayer_reminder.py
- نظام تنبيهات الصلاة التلقائي

### 4. main.py
- إزالة كل مراجع `fiqh_assistant_handler` (المساعد الشفهي)
- إزالة الاستيرادات غير المستخدمة
- تسجيل كل الـ handlers بشكل صحيح مع patterns دقيقة
- إضافة error handler شامل
- إضافة `post_init` لتنظيف الجلسات عند الإيقاف

### 5. ما تم إلغاؤه
- **fiqh_assistant_handler.py** - تم إلغاء قسم المساعد الشفهي بالكامل وإزالة كل مراجعه من main.py
