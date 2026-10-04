# Big Data Hybrid Pipeline & Analytics Engine
### Phase 1 (Midterm) + Phase 2 (Final Project)
**Python Batch | Apache Spark | MongoDB | FastAPI | Materialized Views | APScheduler**

مشروع خط البيانات الضخمة المتكامل لمعالجة، تنظيف، استعلام، وتجميع بيانات الطلبات الضخمة وفق معمارية **ELT** مع واجهة تشغيل موحدة ومهام مجدولة.

---

## 1. نظرة عامة على المشروع (Project Overview)

المشروع مبني على مرحلتين متكاملتين داخل نفس المستودع:

### المرحلة الأولى: خط البيانات الهجين (Midterm Pipeline)
1. **File Router الذكي:** توجيه تلقائي حسب حجم الملف:
   - **Python Batch Loader:** للملفات الصغيرة ($\le 200\text{ MB}$) عبر معالجة تدفقية خفيفة وسريعة.
   - **PySpark Loader:** للملفات الكبيرة ($> 200\text{ MB}$) عبر معالجة موزعة متوازية بكافة الأنوية (تم بنجاح تحميل 30,000,000 سجل بحجم 12.65GB بسرعة 20,593 صف/ث).
2. **Raw Layer (ELT Pattern):** تحميل كافة البيانات الأصلية أولاً في `orders_raw` مع توثيق المصدر ورقم الصف ووقت التحميل.
3. **Data Quality & Audit Trail:** تطبيق أكثر من 8 قواعد تنظيف آلية مع توثيق التعديلات السابقة والجديدة ورمز القاعدة.
4. **Quarantine Isolation:** عزل السجلات غير القابلة للإصلاح في `orders_quarantine` مع أكواد وتفاصيل الأخطاء.
5. **Idempotency & Upsert:** اعتماد `order_id` كمفتاح فريد `Unique Index` واستخدام `Upsert` لمنع أي تكرار عند إعادة التشغيل.
6. **Strict Consistency Formula:** التحقق الرياضي الصارم من اتساق العدادات:
   $$\text{Raw Loaded} = \text{Valid} + \text{Corrected} + \text{Quarantine}$$

### المرحلة الثانية: التحليلات، الفهارس، والواجهة الموحدة (Final Phase 2)
1. **5 استعلامات عملية (Queries):** استعلامات واقعية مخصصة للبيانات.
2. **فهارس الأداء (Indexes & Compound Index):** إنشاء 4 فهارس متخصصة منها فهرس مركب `idx_city_status` وفهرس مركب `idx_customer_date`.
3. **مقارنة Explain ExecutionStats:** مقارنة أداء الاستعلامات قبل وبعد الفهارس وتوثيق انخفاض فحص الوثائق بنسبة تتجاوز 98% إلى 100%.
4. **5 تقارير تجميعية متقدمة (Aggregation Reports):**
   - المبيعات حسب المدينة (`sales_by_city`)
   - أفضل المنتجات مبيعاً وإيراداً (`top_products`)
   - أفضل العملاء إنفاقاً (`top_customers`)
   - المبيعات والاتجاه اليومي (`sales_by_period`)
   - توزيع الطلبات حسب حالة العمل (`orders_by_status`)
5. **العروض المادية التزايدية (Materialized Views):**
   - `daily_sales_summary`
   - `top_products_summary`
   - **آلية التحديث التزايدي:** تعتمد على العلامة المائية `last_synced_at` في `mv_sync_metadata` لتحديث الشرائح الجديدة فقط دون إعادة بناء البيانات من البداية.
6. **المهام المجدولة (Scheduled Jobs):**
   - مهمة التحديث التزايدي للعروض المادية (`refresh_materialized_views`)
   - مهمة التقرير الإداري الدوري المجمع (`generate_executive_report`)
   - تسجيل سجلات التشغيل في `job_logs` وإمكانية التشغيل اليدوي للاختبار.
7. **واجهة API موحدة عبر FastAPI:**
   - واجهة تشغيل واختبار بسيطة مع توثيق Swagger تفاعلي عبر `/docs`.

---

## 2. متطلبات التشغيل (Prerequisites)
- **Python 3.10+** (تم اختباره على Python 3.11)
- **MongoDB Server** (يعمل محلياً على المنفذ `27017`)
- **Java JDK 17+** (لمحرك Apache Spark)

---

## 3. التثبيت والإعداد (Setup)

```bash
# 1. استنساخ المستودع والدخول للمجلد
cd midterm-data-pipeline0

# 2. إنشاء بيئة افتراضية وتفعيلها
python -m venv venv
.\venv\Scripts\activate   # Windows
# source venv/bin/activate # Linux/Mac

# 3. تثبيت المتطلبات
pip install -r requirements.txt

# 4. نسخ ملف الإعدادات
cp .env.example .env
```

---

## 4. بنية المشروع (Project Structure)

```text
midterm-data-pipeline0/
|-- README.md                         # دليل التشغيل والتوثيق الشامل
|-- requirements.txt                  # متطلبات المشروع
|-- .env.example                      # نموذج متغيرات البيئة
|-- config/
|   `-- settings.py                   # إعدادات المسارات، Spark، و MongoDB
|-- data/
|   |-- orders_huge_mixed_quality.csv # الملف الضخم (30M صف ~ 12.65GB)
|   `-- orders_sample_100000.csv      # العينة التدفقية (100k صف)
|-- src/
|   |-- api.py                        # واجهة FastAPI الموحدة للتقييم والتشغيل
|   |-- queries_and_indexes.py        # 5 استعلامات، فهارس مركبة، ومقارنة explain
|   |-- aggregations.py               # 5 تقارير تجميعية مستقلة
|   |-- materialized_views.py         # العروض المادية وآلية التحديث التزايدي
|   |-- scheduler.py                  # المهام المجدولة وسجلات التنفيذ job_logs
|   |-- final_demo.py                 # سكربت العرض الحي للمشروع النهائي (Phase 2)
|   |-- demo_verification.py          # سكربت العرض الحي للمشروع النصفي (Midterm)
|   |-- main.py                       # منسق خط البيانات الهجين
|   |-- file_router.py                # موجّه المحرك التلقائي
|   |-- batch_loader.py               # محرك التحميل الدفعي السريع
|   |-- spark_loader.py               # محرك PySpark المتوازي (30M صف)
|   |-- quality_rules.py              # قواعد التنظيف وسجل التدقيق
|   |-- elt_pipeline.py               # تحويل وتصنيف وUpsert البيانات
|   |-- mongo_setup.py                # تهيئة فهارس المجموعات
|   `-- metrics.py                    # توليد المقاييس وتقارير Markdown
|-- reports/
|   |-- results.json                  # مقاييس تشغيل خط البيانات
|   |-- results.md                    # تقرير ملخص التشغيل
|   |-- explain_results.json          # نتائج فحص explain قبل وبعد الفهارس
|   |-- explain_results.md            # جدول مقارنة الأداء وتبرير الفهارس
|   `-- executive_summary.json        # التقرير الدوري الناتج عن المهام المجدولة
`-- tests/
    |-- test_cleaning_rules.py        # اختبارات قواعد الجودة والتنظيف
    |-- test_classification.py        # اختبارات التصنيف والعزل
    `-- test_final_project.py         # اختبارات شاملة للمرحلة النهائية (25/25 ناجحة)
```

---

## 5. أوامر التشغيل والاستعراض (Execution & Evaluation)

### أولاً: العرض الحي الشامل (Demo)
```bash
# تشغيل العرض الحي المباشر لجميع متطلبات المشروع النهائي (Phase 2):
python src/final_demo.py

# تشغيل العرض الحي للمشروع النصفي (Midterm):
python src/demo_verification.py
```

### ثانياً: تشغيل اختبارات الوحدة الآلية (Unit Tests)
```bash
# تشغيل جميع الاختبارات الـ 25:
python -m pytest tests/
```

### ثالثاً: تشغيل واجهة API الموحدة (FastAPI + Swagger)
```bash
# تشغيل خادم الـ API:
uvicorn src.api:app --host 127.0.0.1 --port 8000
```
- تصفح التوثيق التفاعلي الكامل عبر Swagger UI: **http://127.0.0.1:8000/docs**

#### نقاط الـ API الرئيسية المتوفرة:
| الطريقة | المسار | الوظيفة |
| :---: | :--- | :--- |
| `GET` | `/health` | فحص صحة النظام وحالة اتصال MongoDB وأعداد الوثائق |
| `POST` | `/ingest` | تشغيل خط إدخال البيانات النصفي (يدعم batch و spark و auto) |
| `POST` | `/indexes` | إنشاء فهارس الاستعلامات بما فيها الفهرس المركب |
| `GET` | `/queries` | عرض قائمة الـ 5 استعلامات المتاحة |
| `GET` | `/queries/{name}` | تنفيذ استعلام محدد مع معاملات ترشيح اختيارية |
| `GET` | `/queries/{name}/explain` | استخراج إحصائيات `executionStats` للاستعلام |
| `GET` | `/aggregations` | عرض قائمة الـ 5 تقارير التجميعية |
| `GET` | `/aggregations/{name}` | تنفيذ تقرير تجميعي محدد |
| `POST` | `/refresh-mv` | تشغيل التحديث التزايدي للعروض المادية |
| `GET` | `/materialized-views/{name}` | استعراض السجلات المحسوبة مسبقاً في العرض المادي |
| `GET` | `/jobs` | عرض قائمة المهام المجدولة وسجلات التنفيذ السابقة |
| `POST` | `/jobs/{name}/run` | تشغيل أي مهمة مجدولة يدوياً وفورياً للاختبار |

---

## 6. ملخص مقارنة أداء الفهارس (Explain ExecutionStats)

تم إجراء اختبار مقارنة تفصيلي قبل وبعد إنشاء الفهارس على بيانات `orders_validated`:

| الاستعلام | الفهرس المستهدف | نوع الفهرس | المرحلة قبل | المرحلة بعد | فحص الوثائق قبل | فحص الوثائق بعد | نسبة خفض الفحص |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Orders by City and Status** | `idx_city_status` | Compound | `COLLSCAN` | `IXSCAN` | 6,199 | 100 | **98.39%** |
| **Customer Order History** | `idx_customer_date` | Compound | `COLLSCAN` | `IXSCAN` | 89,937 | 1 | **100.0%** |
| **High Value Orders** | `idx_total_amount` | Single | `SORT -> COLLSCAN` | `IXSCAN` | 89,937 | 100 | **99.89%** |

*التفاصيل والتبريرات الفنية موثقة في [reports/explain_results.md](reports/explain_results.md).*

---

## 7. آلية التحديث التزايدي للعروض المادية (Incremental MV Refresh)

1. العرضان الماديان:
   - `daily_sales_summary` في مجموعة `mv_daily_sales_summary`.
   - `top_products_summary` في مجموعة `mv_top_products_summary`.
2. تحتفظ مجموعة `mv_sync_metadata` بأحدث تاريخ مزامنة `last_synced_at`.
3. عند استدعاء التحديث التزايدي، يستعلم النظام فقط عن السجلات التي أُضيفت أو عُدلت بعد تاريخ العلامة المائية:
   `{"validated_at": {"$gt": last_synced_at}}`
4. يتم دمج المقادير الجديدة تراكمياً باستخدام عمليات `$inc` الذرية في MongoDB، مما يلغي تماماً الحاجة لإعادة حساب الـ 30 مليون صف من البداية.

---

## 8. إنجاز تشغيل الـ 30 مليون صف (Scale Execution Milestone)
- **الملف:** `orders_huge_mixed_quality.csv` (12.65 GB).
- **السجلات المدخلة:** 30,000,000 سجل في `orders_raw`.
- **المحرك:** PySpark عبر 99 Partition وجميع الأنوية الـ 8.
- **الوقت المستغرق:** 1,456.82 ثانية (~24.2 دقيقة).
- **معدل السرعة:** **20,592.81 صف/ثانية**.
