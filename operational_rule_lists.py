"""Persistent admin-managed additions and removals for operational rule lists.

The checked-in workbooks remain the baseline. Managed overrides are stored as a
small JSON object locally and, when configured, in R2 so the same edits survive
Streamlit Cloud restarts and can be shared with the local app.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from io import BytesIO
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

APP_ROOT = Path(__file__).resolve().parent
LOCAL_PATH = APP_ROOT / ".validation_automation" / "operational_rule_lists.json"
R2_KEY = "settings/operational_rule_lists.json"


@st.cache_data(ttl=60, max_entries=1, show_spinner=False)
def load_rule_list_overrides() -> dict:
    """Return the small managed override document, preferring shared R2."""
    candidates = []
    try:
        import r2_storage

        if r2_storage.enabled():
            raw = r2_storage.download_bytes(R2_KEY)
            if raw:
                payload = json.loads(raw.decode("utf-8"))
                if isinstance(payload, dict):
                    candidates.append(payload)
    except Exception:
        pass
    try:
        payload = json.loads(LOCAL_PATH.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            candidates.append(payload)
    except (OSError, ValueError, UnicodeDecodeError):
        pass
    if not candidates:
        return {}
    # Preserve a newer local edit when R2 was temporarily unavailable, while
    # naturally preferring the shared copy on Cloud when it has the newest save.
    def _updated(payload):
        try:
            return datetime.fromisoformat(str(payload.get("updated_at", "")).replace("Z", "+00:00")).timestamp()
        except (TypeError, ValueError, OverflowError):
            return 0.0
    return max(candidates, key=_updated)


def get_rule_list_override(list_name: str, country_code: str):
    """Return a managed country list or ``None`` when workbook baseline applies."""
    payload = load_rule_list_overrides()
    section = payload.get(str(list_name), {})
    if not isinstance(section, dict):
        return None
    code = str(country_code or "").strip().upper()
    if code not in section:
        return None
    return section[code]


def save_rule_list_overrides(payload: dict) -> tuple[bool, str, str]:
    """Atomically save locally and sync to R2 when configured.

    Returns ``(local_saved, cloud_status, backup_path)``. Cloud status is one
    of ``saved``, ``failed``, or ``not_configured``.
    """
    import r2_storage

    payload = dict(payload or {})
    payload["schema_version"] = 1
    payload["updated_at"] = datetime.now(timezone.utc).isoformat()
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")

    backup_path = ""
    try:
        LOCAL_PATH.parent.mkdir(parents=True, exist_ok=True)
        if LOCAL_PATH.is_file():
            backup_dir = LOCAL_PATH.parent / "backups"
            backup_dir.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            backup = backup_dir / f"operational_rule_lists_{stamp}.json"
            backup.write_bytes(LOCAL_PATH.read_bytes())
            backup_path = str(backup)
        fd, temp_name = tempfile.mkstemp(prefix="operational_rule_lists_", suffix=".tmp", dir=str(LOCAL_PATH.parent))
        os.close(fd)
        temp = Path(temp_name)
        temp.write_bytes(encoded)
        os.replace(temp, LOCAL_PATH)
        local_saved = True
    except OSError:
        return False, "failed", backup_path

    cloud_status = "not_configured"
    if r2_storage.enabled():
        previous = r2_storage.download_bytes(R2_KEY)
        if previous:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            backup_key = f"settings/backups/operational_rule_lists_{stamp}.json"
            if not r2_storage.upload_bytes(previous, backup_key, "application/json"):
                load_rule_list_overrides.clear()
                return True, "failed", backup_path
            backup_path = backup_key
        cloud_status = "saved" if r2_storage.upload_bytes(encoded, R2_KEY, "application/json") else "failed"
    load_rule_list_overrides.clear()
    return local_saved, cloud_status, backup_path


def clear_rule_list_caches() -> None:
    """Invalidate cached overlays and validation support bundles after edits."""
    load_rule_list_overrides.clear()
    try:
        import loaders

        for name in (
            "load_restricted_brands_from_local",
            "load_refurb_data_from_local",
            "load_alcohol_data_from_local",
            "load_books_data_from_local",
            "load_perfume_data_from_local",
            "load_support_files_lazy",
        ):
            cached = getattr(loaders, name, None)
            clear = getattr(cached, "clear", None)
            if callable(clear):
                clear()
    except Exception:
        pass


def _split_values(value, separators=r"[,;\n]") -> list[str]:
    return list(dict.fromkeys(
        part.strip() for part in re.split(separators, str(value or "")) if part.strip()
    ))


def render_rule_lists_admin_module() -> None:
    """Render the admin UI for managed brand and seller lists."""
    from data_utils import normalize_text
    import loaders

    st.markdown("## :material/rule_settings: Rule lists")
    st.caption(
        "Add or remove restricted brands and approved sellers used by validation. "
        "The manager saves a small persistent overlay; the source Excel workbooks stay intact."
    )


def normalize_seller_name(value) -> str:
    """Match seller names the same way the seller validators do."""
    return str(value or "").strip().lower()


def _refurb_types(value) -> tuple[str, ...]:
    normalized = str(value or "").strip().casefold()
    if normalized in {"both", "all", "phones and laptops", "phones & laptops"}:
        return ("Phones", "Laptops")
    if "phone" in normalized:
        return ("Phones",)
    return ("Laptops",)
    notice = st.session_state.pop("_operational_rule_notice", None)
    if notice:
        st.success(notice, icon=":material/check_circle:")

    choices = {
        "Restricted brands": ("restricted_brands", "brand"),
        "Refurbished approved sellers": ("refurb_sellers", "refurb"),
        "Alcohol approved sellers": ("alcohol_sellers", "seller"),
        "Books approved sellers": ("books_sellers", "seller"),
        "Perfume approved sellers": ("perfume_sellers", "seller"),
    }
    label = st.selectbox("List", list(choices), key="operational_rule_list_type")
    list_key, list_kind = choices[label]
    country_options = ["KE", "UG", "NG", "GH", "MA"]
    country = "KE" if list_key == "alcohol_sellers" else st.selectbox(
        "Country", country_options, key=f"operational_rule_country_{list_key}"
    )
    country_name = next((name for name, code in loaders.COUNTRY_NAME_TO_TAB.items() if code == country), "Kenya")

    try:
        if list_key == "restricted_brands":
            source = loaders.load_restricted_brands_from_local(country_name).get(country_name, [])
            rows = [{
                "Brand": str(item.get("brand_raw", item.get("brand", "")) or "").strip(),
                "Approved sellers": ", ".join(sorted(item.get("sellers", set()))),
                "Categories": " | ".join(sorted(item.get("categories", set()))),
                "Variations": ", ".join(sorted(item.get("variations", []))),
            } for item in source]
        elif list_key == "refurb_sellers":
            source = loaders.load_refurb_data_from_local(country).get("sellers", {}).get(country, {})
            rows = [{"Seller name": name, "Product type": kind}
                    for kind in ("Phones", "Laptops") for name in sorted(source.get(kind, set()))]
        elif list_key == "alcohol_sellers":
            rows = [{"Seller name": name} for name in sorted(loaders.load_alcohol_data_from_local().get("sellers", {}).get("KE", set()))]
        elif list_key == "books_sellers":
            rows = [{"Seller name": name} for name in sorted(loaders.load_books_data_from_local(country).get("sellers", {}).get(country, set()))]
        else:
            rows = [{"Seller name": name} for name in sorted(loaders.load_perfume_data_from_local(country).get("sellers", {}).get(country, set()))]
    except Exception as exc:
        st.error(f"Could not load {label}: {exc}", icon=":material/error:")
        return

    saved_country = load_rule_list_overrides().get(list_key, {})
    managed = isinstance(saved_country, dict) and country in saved_country
    st.caption(f"{len(rows):,} entries · {country} · {'Managed overlay' if managed else 'Excel baseline'}")
    search = st.text_input("Search", key=f"operational_rule_search_{list_key}_{country}").strip().casefold()
    displayed = rows if not search else [row for row in rows if search in " ".join(map(str, row.values())).casefold()]
    st.dataframe(pd.DataFrame(displayed[:300]), hide_index=True, width="stretch")
    if len(displayed) > 300:
        st.caption(f"Showing 300 of {len(displayed):,} matches. Narrow your search to see more.")

    def persist(next_rows: list[dict], message: str) -> None:
        payload = load_rule_list_overrides()
        section = payload.setdefault(list_key, {})
        if list_kind == "brand":
            section[country] = [{
                "brand": normalize_text(row.get("Brand", "")),
                "brand_raw": str(row.get("Brand", "")).strip(),
                "sellers": [normalize_text(value) for value in _split_values(row.get("Approved sellers", ""))],
                "categories": _split_values(row.get("Categories", ""), r"[|;\n]"),
                "variations": [normalize_text(value) for value in _split_values(row.get("Variations", ""))],
            } for row in next_rows if str(row.get("Brand", "")).strip()]
        elif list_kind == "refurb":
            section[country] = {
                kind: sorted({normalize_seller_name(row.get("Seller name", "")) for row in next_rows
                              if str(row.get("Product type", "")).casefold() == kind.casefold()
                              and str(row.get("Seller name", "")).strip()})
                for kind in ("Phones", "Laptops")
            }
        else:
            section[country] = sorted({normalize_seller_name(row.get("Seller name", "")) for row in next_rows
                                        if str(row.get("Seller name", "")).strip()})
        local_saved, cloud_status, backup = save_rule_list_overrides(payload)
        if not local_saved:
            st.error("Could not save locally. No list change was applied.", icon=":material/error:")
            return
        clear_rule_list_caches()
        if cloud_status == "saved":
            status = "Local and shared cloud copies saved."
        elif cloud_status == "failed":
            status = "Saved locally, but cloud sync failed. Check R2 configuration before relying on this online."
        else:
            status = "Saved locally; shared cloud storage is not configured in this environment."
        st.session_state["_operational_rule_notice"] = f"{message} {status}" + (f" Backup: {backup}" if backup else "")
        st.rerun()

    st.markdown("### Add manually")
    with st.form(f"operational_rule_add_{list_key}_{country}", clear_on_submit=True):
        if list_kind == "brand":
            a, b = st.columns(2)
            name = a.text_input("Brand name")
            sellers = b.text_input("Approved sellers, comma-separated")
            categories = st.text_input("Category codes, separated with |; blank means all categories")
            variations = st.text_input("Allowed variations, comma-separated")
            product_type = ""
        elif list_kind == "refurb":
            a, b = st.columns(2)
            name = a.text_input("Seller name")
            product_type = b.selectbox("Eligible product type", ["Phones", "Laptops", "Phones and Laptops"])
            sellers = categories = variations = ""
        else:
            name = st.text_input("Seller name")
            product_type = sellers = categories = variations = ""
        submit_add = st.form_submit_button("Add entry", icon=":material/add:", type="primary")
    if submit_add:
        name = str(name or "").strip()
        if not name:
            st.warning("Enter a brand or seller name first.")
        elif list_kind == "brand":
            next_rows = list(rows)
            brand_key = normalize_text(name)
            existing = next((row for row in next_rows if normalize_text(row.get("Brand", "")) == brand_key), None)
            if existing:
                for column, value in (("Approved sellers", sellers), ("Categories", categories), ("Variations", variations)):
                    separator = " | " if column == "Categories" else ", "
                    split_re = r"[|;,\n]" if column != "Categories" else r"[|;\n]"
                    old_values = _split_values(existing.get(column, ""), split_re)
                    existing[column] = separator.join(dict.fromkeys(old_values + _split_values(value, split_re)))
            else:
                next_rows.append({"Brand": name, "Approved sellers": sellers, "Categories": categories, "Variations": variations})
            persist(next_rows, f"Added or updated brand {name}.")
        else:
            types = _refurb_types(product_type) if list_kind == "refurb" else ("",)
            next_rows = list(rows)
            added = 0
            for kind in types:
                if any(normalize_seller_name(row.get("Seller name", "")) == normalize_seller_name(name) and row.get("Product type", "") == kind for row in next_rows):
                    continue
                entry = {"Seller name": name}
                if list_kind == "refurb":
                    entry["Product type"] = kind
                next_rows.append(entry)
                added += 1
            if not added:
                st.info(f"{name} is already in this list.")
            else:
                persist(next_rows, f"Added seller {name}.")

    st.markdown("### Remove entries")
    label_to_row = {}
    for row in rows:
        entry_label = str(row.get("Brand" if list_kind == "brand" else "Seller name", "")).strip()
        if list_kind == "refurb":
            entry_label = f"{entry_label} · {row.get('Product type', '')}"
        label_to_row[entry_label] = row
    selected = st.multiselect("Select entries", list(label_to_row), key=f"operational_rule_remove_{list_key}_{country}")
    if st.button("Remove selected", icon=":material/delete:", disabled=not selected, key=f"operational_rule_remove_btn_{list_key}_{country}"):
        remove_keys = set()
        for entry in selected:
            row = label_to_row[entry]
            key = normalize_text(row.get("Brand", "")) if list_kind == "brand" else normalize_seller_name(row.get("Seller name", ""))
            if list_kind == "refurb":
                key += "|" + str(row.get("Product type", ""))
            remove_keys.add(key)
        next_rows = []
        for row in rows:
            key = normalize_text(row.get("Brand", "")) if list_kind == "brand" else normalize_seller_name(row.get("Seller name", ""))
            if list_kind == "refurb":
                key += "|" + str(row.get("Product type", ""))
            if key not in remove_keys:
                next_rows.append(row)
        persist(next_rows, f"Removed {len(selected):,} selected entry/entries.")

    st.markdown("### Import from file")
    st.caption("Upload CSV or Excel. Brands use BRAND; seller lists use SELLER_NAME. Optional ACTION values can be Add or Remove.")
    if list_kind == "brand":
        template = "BRAND,APPROVED_SELLERS,CATEGORIES,VARIATIONS,ACTION\nExample Brand,approved seller,12345|67890,brand variation,Add\n"
        template_name = "restricted_brands_template.csv"
    elif list_kind == "refurb":
        template = "SELLER_NAME,PRODUCT_TYPE,ACTION\nExample Seller,Phones,Add\nExample Seller,Laptops,Add\n"
        template_name = "refurb_sellers_template.csv"
    else:
        template = "SELLER_NAME,ACTION\nExample Seller,Add\n"
        template_name = f"{list_key}_template.csv"
    st.download_button("Download CSV template", template.encode("utf-8-sig"), file_name=template_name, mime="text/csv", key=f"operational_rule_template_{list_key}")
    default_action = st.radio("File rows without ACTION", ["Add", "Remove"], horizontal=True, key=f"operational_rule_action_{list_key}_{country}")
    uploads = st.file_uploader("Rule list files", type=["csv", "xlsx", "xls"], accept_multiple_files=True, key=f"operational_rule_upload_{list_key}_{country}")
    imported_rows = []
    if uploads:
        errors = []
        for upload in uploads:
            try:
                raw = upload.getvalue()
                frame = pd.read_csv(BytesIO(raw), dtype=str, keep_default_na=False) if upload.name.casefold().endswith(".csv") else pd.read_excel(BytesIO(raw), dtype=str).fillna("")
                col_lookup = {re.sub(r"[^a-z0-9]", "", str(col).casefold()): col for col in frame.columns}
                def column(*names):
                    return next((col_lookup[key] for name in names if (key := re.sub(r"[^a-z0-9]", "", name.casefold())) in col_lookup), None)
                name_col = column("BRAND", "BRAND_NAME") if list_kind == "brand" else column("SELLER_NAME", "SELLER", "SELLER NAME")
                if not name_col:
                    errors.append(f"{upload.name}: required column {'BRAND' if list_kind == 'brand' else 'SELLER_NAME'} is missing")
                    continue
                action_col = column("ACTION")
                for _, record in frame.iterrows():
                    entry_name = str(record.get(name_col, "") or "").strip()
                    if not entry_name:
                        continue
                    action = str(record.get(action_col, default_action) if action_col else default_action).strip().casefold() or default_action.casefold()
                    if action not in {"add", "remove", "delete"}:
                        errors.append(f"{upload.name}: unsupported ACTION '{action}' for {entry_name}; use Add or Remove")
                        continue
                    action = "Remove" if action in {"remove", "delete"} else "Add"
                    item = {"name": entry_name, "action": action}
                    if list_kind == "brand":
                        seller_col, category_col, variation_col = column("APPROVED_SELLERS", "APPROVED SELLERS", "SELLERS"), column("CATEGORIES", "CATEGORY_CODES"), column("VARIATIONS")
                        item.update({"sellers": str(record.get(seller_col, "") or "") if seller_col else "", "categories": str(record.get(category_col, "") or "") if category_col else "", "variations": str(record.get(variation_col, "") or "") if variation_col else ""})
                    elif list_kind == "refurb":
                        type_col = column("PRODUCT_TYPE", "TYPE")
                        item["product_type"] = str(record.get(type_col, "Phones and Laptops") or "Phones and Laptops") if type_col else "Phones and Laptops"
                    imported_rows.append(item)
            except Exception as exc:
                errors.append(f"{upload.name}: {exc}")
        for error in errors:
            st.warning(error)
        if imported_rows:
            st.caption(f"{len(imported_rows):,} usable rows · {sum(x['action'] == 'Add' for x in imported_rows):,} additions · {sum(x['action'] == 'Remove' for x in imported_rows):,} removals")
            st.dataframe(pd.DataFrame(imported_rows).head(100), hide_index=True, width="stretch")
            if st.button("Apply file changes", icon=":material/upload_file:", type="primary", key=f"operational_rule_apply_{list_key}_{country}"):
                next_rows = list(rows)
                added = removed = duplicates = 0
                for item in imported_rows:
                    norm = normalize_text(item["name"]) if list_kind == "brand" else normalize_seller_name(item["name"])
                    if list_kind == "brand":
                        found = next((row for row in next_rows if normalize_text(row.get("Brand", "")) == norm), None)
                        if item["action"] == "Remove":
                            before = len(next_rows)
                            next_rows = [row for row in next_rows if normalize_text(row.get("Brand", "")) != norm]
                            removed += before - len(next_rows)
                        elif found:
                            for column_name, field in (("Approved sellers", "sellers"), ("Categories", "categories"), ("Variations", "variations")):
                                split_re = r"[|;,\n]" if column_name != "Categories" else r"[|;\n]"
                                separator = " | " if column_name == "Categories" else ", "
                                found[column_name] = separator.join(dict.fromkeys(_split_values(found.get(column_name, ""), split_re) + _split_values(item.get(field, ""), split_re)))
                            duplicates += 1
                        else:
                            next_rows.append({"Brand": item["name"], "Approved sellers": item["sellers"], "Categories": item["categories"], "Variations": item["variations"]})
                            added += 1
                    else:
                        kind_list = _refurb_types(item.get("product_type", "Phones and Laptops")) if list_kind == "refurb" else [""]
                        for kind in kind_list:
                            match = lambda row: normalize_seller_name(row.get("Seller name", "")) == norm and row.get("Product type", "") == kind
                            if item["action"] == "Remove":
                                before = len(next_rows)
                                next_rows = [row for row in next_rows if not match(row)]
                                removed += before - len(next_rows)
                            elif any(match(row) for row in next_rows):
                                duplicates += 1
                            else:
                                entry = {"Seller name": item["name"]}
                                if list_kind == "refurb":
                                    entry["Product type"] = kind
                                next_rows.append(entry)
                                added += 1
                persist(next_rows, f"File applied: {added:,} added, {removed:,} removed, {duplicates:,} duplicate(s) skipped or merged.")
