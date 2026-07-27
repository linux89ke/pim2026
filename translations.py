# translations.py

LANGUAGES = {
    "English": "en",
    "Français": "fr",
    "العربية": "ar"
}

TRANSLATIONS = {
    "en": {
        # --- Sidebar ---
        "system_status":      "System Status",
        "clear_cache":        "Clear Cache & Reload Data",
        "display_settings":   "Display Settings",

        # --- Sections ---
        "upload_files":       "Upload Files",
        "val_results":        "Validation Results",
        "dashboard":          "Dashboard",
        "flags_breakdown":    "Flags Breakdown",
        "manual_review":      "Manual Image & Category Review",
        "download_reports":   "Download Reports",

        # --- Stats ---
        "total_prod":         "Total Products",
        "approved":           "Approved",
        "rejected":           "Rejected",
        "rej_rate":           "Rejection Rate",
        "common_skus":        "Common SKUs",
        "multi_skus":         "Multi-Country SKUs",

        # --- Batch toolbar ---
        "batch_reject":       "Batch Reject Selected",
        "select_all":         "Select All",
        "deselect_all":       "Deselect All",
        "items_pending":      "items pending",
        "approve_btn":        "Approve Selected",
        "undo":               "Undo Reject",
        "clear_sel":          "Clear Selection",
        "go_bottom":          "Go to Bottom",
        "go_top":             "Go to Top",

        # --- Rejection reasons ---
        "poor_img":           "Poor Image",
        "img_stretched":      "Image Stretched",
        "img_blurry":         "Image Blurry",
        "img_mismatch":       "Image Mismatch",
        "img_infringing":     "Image Infringing",
        "img_too_many":       "Too Many Things in Image",
        "wrong_cat":          "Wrong Category",
        "fake_prod":          "Suspected Fake",
        "restr_brand":        "Restricted Brand",
        "wrong_brand":        "Wrong Brand",
        "prohibited":         "Prohibited Product",
        "missing_color":      "Missing Color",
        "other_custom":       "Other Reason (Custom)",

        # --- Sort / Filter ---
        "sort_by_issue":      "Sort by issue",
        "most_flagged":       "Most Flagged First",
        "no_issue_first":     "No Issues First",
        "filter_by_flag":     "Filter by flag",
        "all_rejected":       "All Rejected",
        "clean_no_flags":     "Clean (no flags)",
        "grp_image":          "Image",
        "grp_qc_flags":       "QC Flags",
        "grp_prefetch":       "Prefetch Flags",
        "sort_low_res":       "Low Resolution",
        "sort_tall":          "Tall (Screenshot?)",
        "sort_wide":          "Wide Aspect",
        "sort_broken":        "Broken Image",
        "sort_wrong_cat":     "Wrong Category",
        "sort_restr_brand":   "Restricted Brands",
        "sort_fake":          "Suspected Fake",
        "sort_missing_color": "Missing Color",
        "sort_warranty":      "Warranty Issues",
        "sort_duplicates":    "Duplicates",
        "filter_brand_ocr":   "Brand Image OCR",
        "filter_duplicates":  "Duplicates",
        "filter_manual":      "Manual Review",
        "filter_color_mis":   "Color Mismatch",
        "filter_brand_name":  "Brand in Name",
        "filter_unneeded":    "Unnecessary Words",
        "filter_prohibited":  "Prohibited Words",

        # --- Custom reason panel ---
        "custom_reason_title": "Enter custom rejection reason",
        "custom_reason_ph":    "Type your reason here...",
        "custom_apply":        "Apply",
        "custom_cancel":       "Cancel",

        # --- Search & misc ---
        "search_grid":        "Search by name, brand, SID or category...",
        "products_label":     "products",
        "dark_mode":          "Dark",
        "reject_as":          "Reject As...",
        "more_options":       "More...",
    },

    "fr": {
        # --- Sidebar ---
        "system_status":      "Etat du Systeme",
        "clear_cache":        "Vider le cache et recharger",
        "display_settings":   "Parametres d'affichage",

        # --- Sections ---
        "upload_files":       "Importer des Fichiers",
        "val_results":        "Resultats de Validation",
        "dashboard":          "Tableau de Bord",
        "flags_breakdown":    "Detail des Rejets",
        "manual_review":      "Revision Manuelle des Images",
        "download_reports":   "Telecharger les Rapports",

        # --- Stats ---
        "total_prod":         "Total Produits",
        "approved":           "Approuve",
        "rejected":           "Rejete",
        "rej_rate":           "Taux de Rejet",
        "common_skus":        "SKU Communs",
        "multi_skus":         "SKU Multi-Pays",

        # --- Batch toolbar ---
        "batch_reject":       "Rejeter la Selection",
        "select_all":         "Tout Selectionner",
        "deselect_all":       "Tout Deselectionner",
        "items_pending":      "en attente",
        "approve_btn":        "Approuver la Selection",
        "undo":               "Annuler le Rejet",
        "clear_sel":          "Effacer la Selection",
        "go_bottom":          "Aller en Bas",
        "go_top":             "Aller en Haut",

        # --- Rejection reasons ---
        "poor_img":           "Image de Mauvaise Qualite",
        "img_stretched":      "Image Etiree",
        "img_blurry":         "Image Floue",
        "img_mismatch":       "Image Non Conforme",
        "img_infringing":     "Image Contrefaite",
        "img_too_many":       "Trop d'Elements dans l'Image",
        "wrong_cat":          "Mauvaise Categorie",
        "fake_prod":          "Produit Suspect / Contrefacon",
        "restr_brand":        "Marque Restreinte",
        "wrong_brand":        "Marque Incorrecte",
        "prohibited":         "Produit Interdit",
        "missing_color":      "Couleur Manquante",
        "other_custom":       "Autre Raison (Personnalise)",

        # --- Sort / Filter ---
        "sort_by_issue":      "Trier par probleme",
        "most_flagged":       "Plus de Problemes en Premier",
        "no_issue_first":     "Sans Problemes en Premier",
        "filter_by_flag":     "Filtrer par alerte",
        "all_rejected":       "Tous les Rejetes",
        "clean_no_flags":     "Propre (aucune alerte)",
        "grp_image":          "Image",
        "grp_qc_flags":       "Alertes QC",
        "grp_prefetch":       "Alertes Pre-chargees",
        "sort_low_res":       "Basse Resolution",
        "sort_tall":          "Haute (Capture d'ecran?)",
        "sort_wide":          "Format Large",
        "sort_broken":        "Image Cassee",
        "sort_wrong_cat":     "Mauvaise Categorie",
        "sort_restr_brand":   "Marques Restreintes",
        "sort_fake":          "Produit Suspect",
        "sort_missing_color": "Couleur Manquante",
        "sort_warranty":      "Problemes de Garantie",
        "sort_duplicates":    "Doublons",
        "filter_brand_ocr":   "OCR Image de Marque",
        "filter_duplicates":  "Doublons",
        "filter_manual":      "Revision Manuelle",
        "filter_color_mis":   "Couleur Incoherente",
        "filter_brand_name":  "Marque dans le Nom",
        "filter_unneeded":    "Mots Inutiles",
        "filter_prohibited":  "Mots Interdits",

        # --- Custom reason panel ---
        "custom_reason_title": "Saisir une raison de rejet personnalisee",
        "custom_reason_ph":    "Tapez votre raison ici...",
        "custom_apply":        "Appliquer",
        "custom_cancel":       "Annuler",

        # --- Search & misc ---
        "search_grid":        "Rechercher par nom, marque, SID ou categorie...",
        "products_label":     "produits",
        "dark_mode":          "Sombre",
        "reject_as":          "Rejeter Comme...",
        "more_options":       "Plus...",
    },

    "ar": {
        # --- Sidebar ---
        "system_status":      "حالة النظام",
        "clear_cache":        "مسح الذاكرة المؤقتة وإعادة التحميل",
        "display_settings":   "إعدادات العرض",

        # --- Sections ---
        "upload_files":       "رفع الملفات",
        "val_results":        "نتائج التحقق",
        "dashboard":          "لوحة القيادة",
        "flags_breakdown":    "تفصيل الرفض",
        "manual_review":      "المراجعة اليدوية للصور والفئات",
        "download_reports":   "تحميل التقارير",

        # --- Stats ---
        "total_prod":         "إجمالي المنتجات",
        "approved":           "مقبول",
        "rejected":           "مرفوض",
        "rej_rate":           "معدل الرفض",
        "common_skus":        "SKU المشتركة",
        "multi_skus":         "SKU متعددة الدول",

        # --- Batch toolbar ---
        "batch_reject":       "رفض المحدد دفعة واحدة",
        "select_all":         "تحديد الكل",
        "deselect_all":       "إلغاء تحديد الكل",
        "items_pending":      "عنصر قيد الانتظار",
        "approve_btn":        "قبول المحدد",
        "undo":               "التراجع عن الرفض",
        "clear_sel":          "إلغاء التحديد",
        "go_bottom":          "الذهاب للأسفل",
        "go_top":             "الذهاب للأعلى",

        # --- Rejection reasons ---
        "poor_img":           "صورة رديئة الجودة",
        "img_stretched":      "صورة مشوهة",
        "img_blurry":         "صورة ضبابية",
        "img_mismatch":       "صورة غير مطابقة",
        "img_infringing":     "صورة منتهكة للحقوق",
        "img_too_many":       "عناصر كثيرة في الصورة",
        "wrong_cat":          "فئة خاطئة",
        "fake_prod":          "منتج مشبوه / مزيف",
        "restr_brand":        "ماركة مقيدة",
        "wrong_brand":        "ماركة خاطئة",
        "prohibited":         "منتج محظور",
        "missing_color":      "لون مفقود",
        "other_custom":       "سبب آخر (مخصص)",

        # --- Sort / Filter ---
        "sort_by_issue":      "ترتيب حسب المشكلة",
        "most_flagged":       "الأكثر مشاكلاً أولاً",
        "no_issue_first":     "بدون مشاكل أولاً",
        "filter_by_flag":     "تصفية حسب التنبيه",
        "all_rejected":       "كل المرفوضات",
        "clean_no_flags":     "نظيف (بدون تنبيهات)",
        "grp_image":          "الصورة",
        "grp_qc_flags":       "تنبيهات مراقبة الجودة",
        "grp_prefetch":       "تنبيهات مسبقة",
        "sort_low_res":       "دقة منخفضة",
        "sort_tall":          "طويل (لقطة شاشة؟)",
        "sort_wide":          "عرض واسع",
        "sort_broken":        "صورة معطوبة",
        "sort_wrong_cat":     "فئة خاطئة",
        "sort_restr_brand":   "ماركات مقيدة",
        "sort_fake":          "منتج مشبوه",
        "sort_missing_color": "لون مفقود",
        "sort_warranty":      "مشاكل الضمان",
        "sort_duplicates":    "منتجات مكررة",
        "filter_brand_ocr":   "OCR صورة الماركة",
        "filter_duplicates":  "منتجات مكررة",
        "filter_manual":      "مراجعة يدوية",
        "filter_color_mis":   "تعارض الألوان",
        "filter_brand_name":  "الماركة في الاسم",
        "filter_unneeded":    "كلمات غير ضرورية",
        "filter_prohibited":  "كلمات محظورة",

        # --- Custom reason panel ---
        "custom_reason_title": "أدخل سبب الرفض المخصص",
        "custom_reason_ph":    "اكتب سببك هنا...",
        "custom_apply":        "تطبيق",
        "custom_cancel":       "إلغاء",

        # --- Search & misc ---
        "search_grid":        "البحث بالاسم أو الماركة أو SID أو الفئة...",
        "products_label":     "منتجات",
        "dark_mode":          "وضع داكن",
        "reject_as":          "رفض كـ...",
        "more_options":       "المزيد...",
    },
}


def get_translation(lang_code: str, key: str) -> str:
    """Return the translation for *key* in *lang_code*, falling back to English."""
    lang_data = TRANSLATIONS.get(lang_code) or TRANSLATIONS["en"]
    return lang_data.get(key) or TRANSLATIONS["en"].get(key, key)
