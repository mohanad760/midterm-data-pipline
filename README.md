# Hybrid Big Data Pipeline (ELT)
### Python Batch | Apache Spark | MongoDB | Data Quality | Idempotency

مشروع خط البيانات الضخمة الهجين لمعالجة وتصنيف بيانات الطلبات الضخمة وفق نمط **ELT**.

---

## 1. نظرة عامة على المشروع (Project Overview)
يهدف المشروع إلى بناء خط بيانات ذكي يتعامل مع ملفات البيانات الضخمة وغير النظيفة:
1. **File Router:** يحدد المحرك تلقائياً حسب حجم الملف:
   - **Python Batch Loader:** للملفات الصغيرة ($\le 200\text{ MB}$) عبر معالجة تدفقية خفيفة وسريعة.
   - **PySpark Loader:** للملفات الكبيرة ($> 200\text{ MB}$) عبر معالجة موزعة ومتوازية.
2. **Raw Layer (ELT Pattern):** تحميل كافة البيانات كما هي أولاً في `orders_raw` دون أي فلترة أو تنظيف، مع توثيق المصدر ورقم الصف ووقت التحميل.
3. **Automated Cleaning & Audit Trail:** تطبيق أكثر من 8 قواعد تنظيف آلية مع توثيق التعديلات السابقة والجديدة ورمز القاعدة.
4. **Quarantine Isolation:** عزل السجلات غير القابلة للتصحيح في `orders_quarantine` وتوثيق أكواد الخطأ وتفاصيلها دون فقدان أي سجل.
5. **Idempotency & Upsert:** اعتماد `order_id` كمفتاح عمل أساسي، وإنشاء فهرس فريد `Unique Index` في `orders_validated`، واستخدام `Upsert` لضمان عدم تكرار السجلات عند إعادة التشغيل.
6. **Strict Consistency Formula:** التحقق الرياضي الصارم من اتساق العدادات:
   $$\text{run\_raw\_count} = \text{run\_valid\_count} + \text{run\_corrected\_count} + \text{run\_quarantine\_count}$$

---

## 2. متطلبات التشغيل (Prerequisites)
- **Python 3.10+** (تم اختباره على Python 3.11)
- **MongoDB Server** (يعمل محلياً على المنفذ `27017`)
- **Java JDK 17+** (لتشغيل محرك Apache Spark)

---

## 3. التثبيت والإعداد (Setup)

```bash
# تثبيت الحزم المطلوبة
pip install -r requirements.txt
```

---

## 4. بنية المجلدات (Project Structure)
```text
midterm-data-pipeline0/
|-- README.md                         # دليل التشغيل والتوثيق
|-- requirements.txt                  # متطلبات المشروع
|-- config/
|   `-- settings.py                   # إعدادات المسارات وقاعدة البيانات
|-- data/
|   |-- orders_huge_mixed_quality.csv # الملف الضخم الأساسي (~13 GB)
|   `-- orders_sample_100000.csv      # العينة الصغيرة المولدة (100k صف)
|-- src/
|   |-- main.py                       # نقطة التشغيل الرئيسية للمشروع
|   |-- file_router.py                # موجّه المحرك التلقائي حسب الحجم
|   |-- create_small_sample.py        # سكربت توليد عينة تدفقية
|   |-- batch_loader.py               # محرك التحميل الدفعي السريع
|   |-- spark_loader.py               # محرك المعالجة المتوازية PySpark
|   |-- quality_rules.py              # قواعد الجودة والتنظيف وسجل التدقيق
|   |-- elt_pipeline.py               # خط التحويل والتصنيف وUpsert
|   |-- mongo_setup.py                # تهيئة الفهارس والمجموعات
|   |-- metrics.py                    # تسجيل المقاييس والتقارير
|   `-- demo_verification.py          # سكربت إثبات خطوات العرض العملي
|-- tests/
|   |-- test_cleaning_rules.py        # اختبارات قواعد التنظيف
|   `-- test_classification.py        # اختبارات التصنيف والعزل
|-- reports/
|   |-- results.json                  # ملف نتائج ومقاييس كل تشغيل
|   `-- results.md                    # تقرير المقاييس المحدث
`-- docs/
    `-- architecture.md               # المعمارية والتبرير التقني
```

---

## 5. أوامر التشغيل (Execution Commands)

### أ. إنشاء العينة الصغيرة (Small Sample Extraction)
```bash
python src/create_small_sample.py --input data/orders_huge_mixed_quality.csv --rows 100000
```

### ب. تشغيل خط البيانات عبر الموجه التلقائي (Unified Pipeline Runner)
```bash
# تشغيل العينة الصغيرة (يختار تلقائياً python_batch):
python src/main.py --input data/orders_sample_100000.csv

# تشغيل الملف الكبير (يختار تلقائياً pyspark):
python src/main.py --input data/orders_huge_mixed_quality.csv
```

### ج. تشغيل الاختبارات الآلية (Unit Tests)
```bash
python -m pytest tests/ -v
```

### د. سيناريو العرض العملي والمناقشة (Live Oral Demo)
قم بتشغيل السكربت الشامل لعرض خطوات التحقق الثمانية المطلوبة أمام المحاضر:
```bash
python src/demo_verification.py
```

---

## 6. قواعد التنظيف وأكواد العزل المطبقة

### قواعد التنظيف الآلي (Cleaning Rules):
1. `ARABIC_DIGITS_CONVERTED`: تحويل الأرقام العربية والفواصل العشرية (`٠-٩`, `٫`) إلى أرقام لاتينية قياسية.
2. `WORD_NUMBER_CONVERTED`: تحويل الأسعار بالكلمات (`ألفان`, `خمسة آلاف`) إلى قيم عددية.
3. `CURRENCY_STANDARDIZED`: إزالة نصوص العملات وتوحيدها إلى `YER`.
4. `THOUSAND_SEPARATORS_REMOVED`: إزالة فواصل الآلاف من القيم النقدية.
5. `PHONE_FORMAT_NORMALIZED`: تنظيف المسافات والرموز وتوحيد صيغة أرقام الهواتف.
6. `EMAIL_REPEATED_SYMBOLS`: تصحيح الرموز المكررة مثل `@@` و `..` في البريد الإلكتروني.
7. `DATE_FORMAT_STANDARDIZED`: توحيد التواريخ بمختلف صيغها إلى صيغة ISO القياسية.
8. `WHITESPACE_TRIMMED` & `STATUS_SYNONYM_MAPPED`: إزالة المسافات وتوحيد مرادفات حالات الطلب والدفع.
9. `TOTAL_RECALCULATED`: إعادة احتساب إجمالي الطلب ومطابقته بدقة.

### أكواد العزل في `orders_quarantine`:
- `MISSING_ORDER_ID`: معرف الطلب مفقود.
- `MISSING_CUSTOMER_ID`: معرف العميل مفقود.
- `INVALID_IMPOSSIBLE_DATE`: تاريخ غير منطقي أو تالف.
- `CORRUPTED_ITEMS_JSON`: نص JSON غير قابل للتحليل.
- `EMPTY_ITEMS`: مصفوفة عناصر فارغة.
- `AMBIGUOUS_NEGATIVE_VALUE`: كميات أو أسعار سالبة غير صالحة.
- `UNKNOWN_PRICE`: تعذر تحديد السعر أو إجمالي الطلب.
- `MULTIPLE_CONFLICTING_ERRORS`: وجود أكثر من خطأ جوهري في نفس السجل.

---

## 7. إثبات الـ Idempotency و Upsert
- عند إعادة تشغيل نفس البيانات أو معالجة سجلات موجودة مسبقاً:
  - لا تزيد أعداد `orders_validated`.
  - لا يحدث خطأ تكرار (Duplicate Key Error).
  - يتم تحديث السجل المعدل (`updated_count`) مع بقاء إجمالي السجلات ثابتاً تماماً.
