"""Build the five-page PBIR report from the Stage 7 mart contract.

The semantic model is authored separately through Microsoft Power BI MCP.
This file only writes report pages and visual bindings; it never loads data.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent / "EcommerceCDC.Report" / "definition" / "pages"
PAGE_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.1.0/schema.json"
VISUAL_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.9.0/schema.json"
PAGES_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.1.0/schema.json"
INK = "#243247"
MUTED = "#66758B"


def ident(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:20]


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def field(table: str, name: str, kind: str = "Column") -> dict:
    return {
        "field": {kind: {"Expression": {"SourceRef": {"Entity": table}}, "Property": name}},
        "queryRef": f"{table}.{name}",
        "nativeQueryRef": name,
    }


def visual(page: str, key: str, kind: str, x: int, y: int, w: int, h: int, *,
           query: dict | None = None, objects: dict | None = None,
           container: dict | None = None, sort: tuple[str, str] | None = None) -> None:
    name = ident(f"{page}:{key}")
    content = {
        "$schema": VISUAL_SCHEMA,
        "name": name,
        "position": {"x": x, "y": y, "z": 1000, "height": h, "width": w,
                     "tabOrder": 1000},
        "visual": {"visualType": kind},
    }
    target = content["visual"]
    if query:
        target["query"] = {"queryState": query}
        if sort:
            target["query"]["sortDefinition"] = {
                "sort": [{"field": field(*sort, "Measure")["field"],
                          "direction": "Descending"}],
                "isDefaultSort": True,
            }
    if objects:
        target["objects"] = objects
    if container:
        target["visualContainerObjects"] = container
    write_json(ROOT / page / "visuals" / name / "visual.json", content)


def heading(page: str, key: str, value: str, x: int, y: int, w: int, h: int,
            size: int = 18, color: str = INK) -> None:
    from math import ceil
    h = max(h, ceil(size * 25 / 16))
    visual(page, key, "textbox", x, y, w, h, objects={
        "general": [{"properties": {"paragraphs": [{
            "textRuns": [{"value": value, "textStyle": {
                "fontFamily": "Segoe UI Semibold", "fontSize": f"{size}px", "color": color,
            }}], "horizontalTextAlignment": "left",
        }]}}],
    }, container={"padding": [{"properties": {
        side: {"expr": {"Literal": {"Value": "0D"}}}
        for side in ("top", "bottom", "left", "right")
    }}]})


def cards(page: str, key: str, measures: list[tuple[str, str]],
          x: int, y: int, w: int, canvas_h: int) -> None:
    # Card sizing formula from the Power BI authoring skill (canvas 720/900).
    value_size = min(28, max(12, int(canvas_h * 0.028)))
    label_size = min(14, max(8, int(value_size * 0.55)))
    from math import ceil
    h = 16 + 16 + 2 + ceil(value_size * 1.5) + ceil(max(label_size, 12) * 1.5)
    literal = lambda v: {"expr": {"Literal": {"Value": f"{v}D"}}}
    visual(page, key, "cardVisual", x, y, w, h,
           query={"Data": {"projections": [field(t, m, "Measure") for t, m in measures]}},
           objects={
               "value": [{"properties": {"fontSize": literal(value_size)}, "selector": {"id": "default"}}],
               "label": [{"properties": {"fontSize": literal(label_size)}, "selector": {"id": "default"}}],
               "padding": [{"properties": {"paddingUniform": literal(8)}, "selector": {"id": "default"}}],
               "layout": [{"properties": {"paddingUniform": literal(0)}, "selector": {"id": "default"}}],
           }, container={"padding": [{"properties": {
               "top": literal(8), "bottom": literal(8), "left": literal(8), "right": literal(8),
           }, "selector": {"id": "default"}}]})


def chart(page: str, key: str, kind: str, category: tuple[str, str],
          measure: tuple[str, str], x: int, y: int, w: int, h: int) -> None:
    visual(page, key, kind, x, y, w, h, query={
        "Category": {"projections": [field(*category)]},
        "Y": {"projections": [field(*measure, "Measure")]},
    }, container={"title": [{"properties": {
        "show": {"expr": {"Literal": {"Value": "false"}}}
    }}]}, sort=measure if kind == "barChart" else None)


def table(page: str, key: str, columns: list[tuple[str, str]],
          measures: list[tuple[str, str]], x: int, y: int, w: int, h: int,
          sort: tuple[str, str] | None = None) -> None:
    literal = lambda value: {"expr": {"Literal": {"Value": value}}}
    visual(page, key, "tableEx", x, y, w, h,
           query={"Values": {"projections": [field(*c) for c in columns] +
                                       [field(*m, "Measure") for m in measures]}},
           objects={"columnHeaders": [{"properties": {
               "columnAdjustment": literal("'growToFit'"),
               "autoSizeColumnWidth": literal("true"),
           }}]}, sort=sort)


def slicer(page: str, key: str, column: tuple[str, str], label: str,
           x: int, y: int, w: int) -> None:
    literal = lambda value: {"expr": {"Literal": {"Value": value}}}
    visual(page, key, "slicer", x, y, w, 80,
           query={"Values": {"projections": [field(*column)]}},
           objects={
               "data": [{"properties": {"mode": literal("'Dropdown'")}}],
               "header": [{"properties": {"show": literal("true"), "text": literal(f"'{label}'")}}],
           }, container={"padding": [{"properties": {
               side: literal("8D") for side in ("top", "bottom", "left", "right")
           }}]})


def add_page(name: str, display: str, height: int = 720) -> str:
    page = "c1dcfcfe5c3840b7ba32" if name == "executive" else ident(name)
    write_json(ROOT / page / "page.json", {
        "$schema": PAGE_SCHEMA, "name": page, "displayName": display,
        "displayOption": "FitToPage", "height": height, "width": 1280,
    })
    return page


def build() -> None:
    sales = "mart_daily_sales"
    product = "mart_product_performance"
    customer = "mart_customer_rfm"
    payment = "mart_payment_mix_daily"
    experience = "mart_order_experience_daily"
    funnel = "mart_funnel_daily"
    ops = "ops_pipeline_runs"

    p1 = add_page("executive", "Executive / Sales")
    heading(p1, "title", "Sales overview", 24, 20, 850, 44, 28)
    heading(p1, "subtitle", "Olist historical orders · revenue excludes freight · BRL", 24, 66, 900, 30, 14, MUTED)
    slicer(p1, "purchase-date", (sales, "purchase_date"), "Purchase date", 24, 99, 255)
    slicer(p1, "customer-state", (sales, "customer_state"), "Customer state", 295, 99, 230)
    cards(p1, "headline", [(sales, m) for m in ("Delivered item revenue", "Orders", "Delivered orders", "AOV delivered item", "Cancellation rate")], 24, 188, 1232, 720)
    heading(p1, "daily-label", "Delivered item revenue by purchase date", 24, 297, 750, 28)
    chart(p1, "daily-revenue", "lineChart", (sales, "purchase_date"), (sales, "Delivered item revenue"), 24, 331, 780, 323)
    heading(p1, "state-label", "Revenue by customer state", 830, 297, 425, 28)
    chart(p1, "state-revenue", "barChart", (sales, "customer_state"), (sales, "Delivered item revenue"), 830, 331, 425, 323)

    p2 = add_page("product", "Product")
    heading(p2, "title", "Product performance", 24, 20, 850, 44, 28)
    heading(p2, "subtitle", "All-time delivered items · seller geography · no purchase-date filter", 24, 66, 1100, 30, 14, MUTED)
    slicer(p2, "category", (product, "category_name_english"), "Category", 24, 99, 270)
    slicer(p2, "seller", (product, "seller_id"), "Seller", 310, 99, 255)
    cards(p2, "headline", [(product, "Product revenue"), (product, "Items sold")], 24, 188, 600, 720)
    heading(p2, "category-label", "Revenue by category", 24, 297, 390, 28)
    chart(p2, "category-revenue", "barChart", (product, "category_name_english"), (product, "Product revenue"), 24, 331, 390, 323)
    heading(p2, "seller-state-label", "Revenue by seller state", 434, 297, 390, 28)
    chart(p2, "seller-state-revenue", "barChart", (product, "seller_state"), (product, "Product revenue"), 434, 331, 390, 323)
    heading(p2, "top-product-label", "Products by revenue", 844, 297, 410, 28)
    table(p2, "top-products", [(product, "product_id")],
          [(product, "Product revenue"), (product, "Items sold")],
          844, 331, 410, 323, sort=(product, "Product revenue"))
    heading(p2, "note", "Order counts across products are not distinct orders.", 650, 205, 580, 34, 13, MUTED)

    p3 = add_page("customer", "Customer / RFM")
    heading(p3, "title", "Customer segments", 24, 20, 850, 44, 28)
    heading(p3, "subtitle", "One current RFM snapshot, anchored to the data as-of date", 24, 66, 1000, 30, 14, MUTED)
    slicer(p3, "segment", (customer, "segment"), "RFM segment", 24, 99, 260)
    cards(p3, "headline", [(customer, "Customers"), (customer, "Customer monetary"), (customer, "RFM as of")], 24, 188, 880, 720)
    heading(p3, "segment-count-label", "Customers by segment", 24, 297, 390, 28)
    chart(p3, "segment-count", "barChart", (customer, "segment"), (customer, "Customers"), 24, 331, 390, 323)
    heading(p3, "segment-value-label", "Monetary by segment", 434, 297, 390, 28)
    chart(p3, "segment-value", "barChart", (customer, "segment"), (customer, "Customer monetary"), 434, 331, 390, 323)
    heading(p3, "high-value-label", "High-value customers", 844, 297, 410, 28)
    table(p3, "high-value", [(customer, "customer_unique_id")],
          [(customer, "Customer monetary")], 844, 331, 410, 323,
          sort=(customer, "Customer monetary"))

    p4 = add_page("journey", "Journey / Experience")
    heading(p4, "title", "Payments, delivery & journey", 24, 20, 1100, 44, 28)
    heading(p4, "subtitle", "Each section has its own date meaning; funnel events are synthetic", 24, 66, 1200, 30, 14, MUTED)
    heading(p4, "payment-title", "Payment mix", 24, 111, 380, 32, 20)
    slicer(p4, "payment-date", (payment, "order_purchase_date"), "Order purchase date", 24, 150, 380)
    cards(p4, "payment-kpi", [(payment, "Payment value")], 24, 240, 380, 720)
    chart(p4, "payment-type", "barChart", (payment, "payment_type"), (payment, "Payment value"), 24, 348, 380, 255)
    heading(p4, "payment-note", "Payment records can share one order.", 24, 631, 380, 30, 13, MUTED)
    heading(p4, "experience-title", "Delivery & review", 444, 111, 380, 32, 20)
    slicer(p4, "experience-date", (experience, "purchase_date"), "Order purchase date", 444, 150, 380)
    cards(p4, "experience-kpi", [(experience, "Late rate"), (experience, "Delivery days"), (experience, "Review score")], 444, 240, 380, 720)
    chart(p4, "experience-state", "barChart", (experience, "customer_state"), (experience, "Late rate"), 444, 348, 380, 255)
    heading(p4, "funnel-title", "Synthetic clickstream", 864, 111, 390, 32, 20)
    slicer(p4, "session-date", (funnel, "session_date"), "Session date", 864, 150, 390)
    cards(p4, "funnel-kpi", [(funnel, "Sessions"), (funnel, "Purchase rate")], 864, 240, 390, 720)
    chart(p4, "funnel-device", "barChart", (funnel, "device"), (funnel, "Purchase sessions"), 864, 348, 390, 255)
    heading(p4, "funnel-note", "Generated from orders; not real user conversion.", 864, 631, 390, 40, 13, MUTED)

    p5 = add_page("ops", "Data Quality / Ops")
    heading(p5, "title", "Pipeline quality & operations", 24, 20, 1100, 44, 28)
    heading(p5, "subtitle", "Per DAG run · inspect status, row counts and quarantine · CDC lag not available", 24, 66, 1200, 30, 14, MUTED)
    slicer(p5, "run-date", (ops, "started_at"), "Run time", 24, 99, 270)
    slicer(p5, "namespace", (ops, "namespace"), "Namespace", 310, 99, 280)
    heading(p5, "run-label", "Recorded pipeline runs", 24, 199, 1200, 28)
    table(p5, "runs", [(ops, n) for n in ("started_at", "run_id", "namespace", "status", "failed_task", "bronze_rows", "silver_rows", "gold_rows", "mart_rows", "quarantine_rows", "volume_warning")], [], 24, 235, 1232, 390)
    heading(p5, "footer", "Row counts belong to each run. A successful Airflow run still needs run_id reconciliation.", 24, 655, 1210, 35, 13, MUTED)

    write_json(ROOT / "pages.json", {"$schema": PAGES_SCHEMA,
        "pageOrder": [p1, p2, p3, p4, p5], "activePageName": p1})


if __name__ == "__main__":
    build()
