from __future__ import annotations

import os
import re
import textwrap
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from app.core.config import LOCAL_REPORT_EXPORT_DIR


PAGE_WIDTH = 595
PAGE_HEIGHT = 842
TOP_MARGIN = 56
BOTTOM_MARGIN = 48
LEFT_MARGIN = 52
RIGHT_MARGIN = 52
BODY_FONT_SIZE = 10.5
BODY_LINE_HEIGHT = 14
SECTION_FONT_SIZE = 12.5
SECTION_LINE_HEIGHT = 18
TITLE_FONT_SIZE = 18
TITLE_LINE_HEIGHT = 24
MAX_WRAP = 92
MAX_WRAP_BULLET = 86
MAX_WRAP_SECTION = 80


def _report_export_dir() -> Path:
    directory = Path(LOCAL_REPORT_EXPORT_DIR)
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _safe_filename_component(value: str, fallback: str = "report") -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "").strip())
    cleaned = cleaned.strip("._-")
    return cleaned[:80] or fallback


def _ascii_clean(value: str) -> str:
    text = str(value or "")
    replacements = {
        "\u2013": "-",
        "\u2014": "-",
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u2022": "-",
        "\u20b9": "Rs.",
        "\u00a0": " ",
    }
    for source, target in replacements.items():
        text = text.replace(source, target)
    return text.encode("latin-1", errors="replace").decode("latin-1")


def _pdf_escape(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace("(", "\\(")
        .replace(")", "\\)")
    )


def _line_style(text: str) -> tuple[str, float, int, int]:
    stripped = text.strip()
    if not stripped:
        return "F1", BODY_FONT_SIZE, BODY_LINE_HEIGHT, MAX_WRAP
    if stripped == "INVESTIGATION REPORT":
        return "F2", TITLE_FONT_SIZE, TITLE_LINE_HEIGHT, 56
    if stripped.endswith(":") and not stripped.startswith("- "):
        return "F2", SECTION_FONT_SIZE, SECTION_LINE_HEIGHT, MAX_WRAP_SECTION
    if stripped.startswith("- "):
        return "F1", BODY_FONT_SIZE, BODY_LINE_HEIGHT, MAX_WRAP_BULLET
    return "F1", BODY_FONT_SIZE, BODY_LINE_HEIGHT, MAX_WRAP


def _wrap_report_lines(report_text: str) -> list[dict[str, Any]]:
    wrapped_lines: list[dict[str, Any]] = []

    for raw_line in _ascii_clean(report_text).splitlines():
        stripped = raw_line.strip()
        font_name, font_size, line_height, wrap_width = _line_style(raw_line)

        if not stripped:
            wrapped_lines.append(
                {
                    "text": "",
                    "font_name": "F1",
                    "font_size": BODY_FONT_SIZE,
                    "line_height": BODY_LINE_HEIGHT,
                    "indent": 0,
                }
            )
            continue

        indent = 0
        bullet_prefix = ""
        content = stripped

        if stripped.startswith("- "):
            indent = 10
            bullet_prefix = "- "
            content = stripped[2:].strip()

        chunks = textwrap.wrap(
            content,
            width=wrap_width,
            break_long_words=False,
            break_on_hyphens=False,
        ) or [content]

        for index, chunk in enumerate(chunks):
            prefix = bullet_prefix if index == 0 and bullet_prefix else ("  " if bullet_prefix else "")
            wrapped_lines.append(
                {
                    "text": f"{prefix}{chunk}".rstrip(),
                    "font_name": font_name,
                    "font_size": font_size,
                    "line_height": line_height,
                    "indent": indent,
                }
            )

    return wrapped_lines


def _paginate_report_lines(lines: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    pages: list[list[dict[str, Any]]] = []
    current_page: list[dict[str, Any]] = []
    current_y = PAGE_HEIGHT - TOP_MARGIN

    for line in lines:
        line_height = int(line.get("line_height") or BODY_LINE_HEIGHT)
        if current_page and current_y - line_height < BOTTOM_MARGIN:
            pages.append(current_page)
            current_page = []
            current_y = PAGE_HEIGHT - TOP_MARGIN

        current_page.append(line)
        current_y -= line_height

    if current_page or not pages:
        pages.append(current_page)

    return pages


def _build_pdf_content_stream(lines: list[dict[str, Any]]) -> bytes:
    commands: list[str] = []
    current_y = PAGE_HEIGHT - TOP_MARGIN

    for line in lines:
        text = str(line.get("text") or "")
        font_name = str(line.get("font_name") or "F1")
        font_size = float(line.get("font_size") or BODY_FONT_SIZE)
        line_height = int(line.get("line_height") or BODY_LINE_HEIGHT)
        indent = int(line.get("indent") or 0)

        if text:
            x = LEFT_MARGIN + indent
            y = current_y
            commands.append(
                f"BT /{font_name} {font_size:.2f} Tf 1 0 0 1 {x} {y} Tm ({_pdf_escape(text)}) Tj ET"
            )

        current_y -= line_height

    return "\n".join(commands).encode("latin-1", errors="replace")


def _append_pdf_object(objects: list[bytes], payload: bytes | str) -> int:
    if isinstance(payload, str):
        payload = payload.encode("latin-1", errors="replace")
    objects.append(payload)
    return len(objects)


def _build_pdf_document(pages: list[list[dict[str, Any]]]) -> bytes:
    objects: list[bytes] = []

    catalog_id = _append_pdf_object(objects, b"<< /Type /Catalog /Pages 2 0 R >>")

    page_object_ids: list[int] = []
    content_object_ids: list[int] = []

    pages_id = _append_pdf_object(objects, b"<< /Type /Pages /Count 0 /Kids [] >>")
    font_regular_id = _append_pdf_object(objects, b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    font_bold_id = _append_pdf_object(objects, b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>")

    for page_lines in pages:
        content_stream = _build_pdf_content_stream(page_lines)
        content_object_ids.append(
            _append_pdf_object(
                objects,
                b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content_stream), content_stream),
            )
        )
        page_object_ids.append(
            _append_pdf_object(
                objects,
                (
                    f"<< /Type /Page /Parent {pages_id} 0 R /MediaBox [0 0 {PAGE_WIDTH} {PAGE_HEIGHT}] "
                    f"/Resources << /Font << /F1 {font_regular_id} 0 R /F2 {font_bold_id} 0 R >> >> "
                    f"/Contents {content_object_ids[-1]} 0 R >>"
                ),
            )
        )

    kids = " ".join(f"{page_id} 0 R" for page_id in page_object_ids)
    objects[pages_id - 1] = (
        f"<< /Type /Pages /Count {len(page_object_ids)} /Kids [{kids}] >>".encode("latin-1", errors="replace")
    )

    pdf = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]

    for object_id, payload in enumerate(objects, start=1):
        offsets.append(len(pdf))
        pdf.extend(f"{object_id} 0 obj\n".encode("latin-1"))
        pdf.extend(payload)
        pdf.extend(b"\nendobj\n")

    xref_offset = len(pdf)
    pdf.extend(f"xref\n0 {len(objects) + 1}\n".encode("latin-1"))
    pdf.extend(b"0000000000 65535 f \n")

    for offset in offsets[1:]:
        pdf.extend(f"{offset:010d} 00000 n \n".encode("latin-1"))

    pdf.extend(
        (
            f"trailer\n<< /Size {len(objects) + 1} /Root {catalog_id} 0 R >>\n"
            f"startxref\n{xref_offset}\n%%EOF"
        ).encode("latin-1")
    )
    return bytes(pdf)


def _resolve_report_filename(report_title: str) -> tuple[str, str]:
    report_id = uuid.uuid4().hex
    sanitized_title = _safe_filename_component(report_title, fallback="AXIS_Investigation_Report")
    filename = f"{report_id}__{sanitized_title}.pdf"
    return report_id, filename


def export_investigation_report_pdf(report_text: str, report_title: str | None = None) -> dict[str, str]:
    normalized_text = str(report_text or "").strip()
    if not normalized_text:
        raise ValueError("Investigation report text is required for export.")

    base_title = report_title or f"AXIS_Investigation_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    safe_title = _safe_filename_component(base_title, fallback="AXIS_Investigation_Report")
    report_id, filename = _resolve_report_filename(safe_title)
    lines = _wrap_report_lines(normalized_text)
    pages = _paginate_report_lines(lines)
    pdf_bytes = _build_pdf_document(pages)

    output_path = _report_export_dir() / filename
    output_path.write_bytes(pdf_bytes)

    return {
        "reportId": report_id,
        "fileName": filename.split("__", 1)[1],
        "filePath": str(output_path),
        "downloadUrl": f"/reports/{report_id}",
    }


def resolve_exported_report(report_id: str) -> dict[str, str] | None:
    safe_report_id = _safe_filename_component(report_id, fallback="")
    if not safe_report_id:
        return None

    export_dir = _report_export_dir()
    matches = sorted(export_dir.glob(f"{safe_report_id}__*.pdf"))
    if not matches:
        return None

    file_path = matches[0]
    download_name = file_path.name.split("__", 1)[1] if "__" in file_path.name else file_path.name
    return {
        "reportId": safe_report_id,
        "fileName": download_name,
        "filePath": str(file_path),
    }
