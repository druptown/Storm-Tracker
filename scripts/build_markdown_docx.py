"""Build a readable DOCX from the Storm Tracker Markdown audit.

The project intentionally has no document-generation runtime dependency.  This
small converter therefore writes the required OOXML parts with Python's
standard library.  It supports the structures used by the roadmap: headings,
paragraphs, lists, quotes, fenced code and simple pipe tables.
"""
from __future__ import annotations

import argparse
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path
import re
from xml.sax.saxutils import escape
import zipfile


_LINK = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
_NUMBERED = re.compile(r"^(\d+)\.\s+(.*)$")
_HEADING = re.compile(r"^(#{1,6})\s+(.*)$")
_TABLE_SEPARATOR = re.compile(
    r"^\|\s*:?-{3,}:?\s*(?:\|\s*:?-{3,}:?\s*)+\|?$"
)
_PAGE_BREAK_MARKER = "<!-- STV3_DOCX_PAGE_BREAK -->"


def _text(value: str) -> str:
    value = _LINK.sub(lambda match: f"{match.group(1)} ({match.group(2)})", value)
    value = value.replace("**", "").replace("__", "")
    value = value.replace("`", "").replace("*", "")
    return value.strip()


def _run(value: str, *, bold: bool = False, code: bool = False) -> str:
    properties = []
    if bold:
        properties.append("<w:b/>")
    if code:
        properties.extend((
            '<w:rFonts w:ascii="Courier New" w:hAnsi="Courier New"/>',
            '<w:sz w:val="16"/>',
        ))
    props = f"<w:rPr>{''.join(properties)}</w:rPr>" if properties else ""
    preserve = ' xml:space="preserve"' if value[:1].isspace() or value[-1:].isspace() else ""
    return f"<w:r>{props}<w:t{preserve}>{escape(value)}</w:t></w:r>"


def _paragraph(
    value: str,
    *,
    style: str = "Normal",
    before: int | None = None,
    after: int | None = None,
    left: int | None = None,
    bold: bool = False,
    code: bool = False,
) -> str:
    spacing = []
    if before is not None:
        spacing.append(f'w:before="{before}"')
    if after is not None:
        spacing.append(f'w:after="{after}"')
    paragraph_props = [f'<w:pStyle w:val="{style}"/>']
    if spacing:
        paragraph_props.append(f"<w:spacing {' '.join(spacing)}/>")
    if left is not None:
        paragraph_props.append(f'<w:ind w:left="{left}"/>')
    return (
        "<w:p><w:pPr>"
        + "".join(paragraph_props)
        + "</w:pPr>"
        + _run(value, bold=bold, code=code)
        + "</w:p>"
    )


def _page_break() -> str:
    return '<w:p><w:r><w:br w:type="page"/></w:r></w:p>'


def _table_cell(value: str, *, header: bool) -> str:
    shading = '<w:shd w:fill="D9EAF7"/>' if header else ""
    return (
        "<w:tc><w:tcPr>"
        + shading
        + '<w:tcMar><w:top w:w="70" w:type="dxa"/>'
        '<w:left w:w="90" w:type="dxa"/>'
        '<w:bottom w:w="70" w:type="dxa"/>'
        '<w:right w:w="90" w:type="dxa"/></w:tcMar>'
        "</w:tcPr>"
        + _paragraph(_text(value), style="TableText", bold=header)
        + "</w:tc>"
    )


def _table(rows: list[list[str]]) -> str:
    if not rows:
        return ""
    width = max(len(row) for row in rows)
    normalized = [row + [""] * (width - len(row)) for row in rows]
    body = []
    for index, row in enumerate(normalized):
        body.append(
            "<w:tr>"
            + "".join(_table_cell(cell, header=index == 0) for cell in row)
            + "</w:tr>"
        )
    return (
        "<w:tbl><w:tblPr>"
        '<w:tblStyle w:val="TableGrid"/>'
        '<w:tblW w:w="0" w:type="auto"/>'
        '<w:tblLayout w:type="autofit"/>'
        "</w:tblPr>"
        '<w:tblGrid>'
        + "".join('<w:gridCol w:w="2400"/>' for _ in range(width))
        + "</w:tblGrid>"
        + "".join(body)
        + "</w:tbl>"
        + _paragraph("", after=100)
    )


def _split_table_row(line: str) -> list[str]:
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def _document_body(markdown: str) -> str:
    lines = markdown.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    blocks: list[str] = []
    paragraph_lines: list[str] = []
    code_lines: list[str] = []
    in_code = False
    table_rows: list[list[str]] = []

    def flush_paragraph() -> None:
        if paragraph_lines:
            blocks.append(_paragraph(_text(" ".join(paragraph_lines))))
            paragraph_lines.clear()

    def flush_table() -> None:
        if table_rows:
            blocks.append(_table(table_rows))
            table_rows.clear()

    for raw in lines:
        line = raw.rstrip()
        if line.strip() == _PAGE_BREAK_MARKER:
            flush_paragraph()
            flush_table()
            if in_code:
                raise ValueError(
                    "DOCX-paginaovergang mag niet binnen een codeblok staan"
                )
            blocks.append(_page_break())
            continue
        if line.startswith("```"):
            flush_paragraph()
            flush_table()
            if in_code:
                blocks.append(
                    _paragraph(
                        "\n".join(code_lines),
                        style="CodeBlock",
                        code=True,
                    )
                )
                code_lines.clear()
                in_code = False
            else:
                in_code = True
            continue
        if in_code:
            code_lines.append(line)
            continue

        if line.strip().startswith("|") and "|" in line.strip()[1:]:
            flush_paragraph()
            if not _TABLE_SEPARATOR.match(line.strip()):
                table_rows.append(_split_table_row(line))
            continue
        flush_table()

        if not line.strip():
            flush_paragraph()
            continue
        heading = _HEADING.match(line)
        if heading:
            flush_paragraph()
            level = min(len(heading.group(1)), 4)
            blocks.append(
                _paragraph(
                    _text(heading.group(2)),
                    style=f"Heading{level}",
                )
            )
            continue
        if line.strip() in {"---", "***", "___"}:
            flush_paragraph()
            continue
        if line.startswith(">"):
            flush_paragraph()
            blocks.append(
                _paragraph(
                    _text(line.lstrip("> ")),
                    style="Quote",
                    left=360,
                )
            )
            continue
        stripped = line.lstrip()
        indent = len(line) - len(stripped)
        if stripped.startswith(("- ", "* ")):
            flush_paragraph()
            blocks.append(
                _paragraph(
                    "• " + _text(stripped[2:]),
                    style="ListText",
                    left=360 + min(indent, 8) * 90,
                )
            )
            continue
        numbered = _NUMBERED.match(stripped)
        if numbered:
            flush_paragraph()
            blocks.append(
                _paragraph(
                    f"{numbered.group(1)}. {_text(numbered.group(2))}",
                    style="ListText",
                    left=360 + min(indent, 8) * 90,
                )
            )
            continue
        paragraph_lines.append(line.strip())

    flush_paragraph()
    flush_table()
    if code_lines:
        blocks.append(_paragraph("\n".join(code_lines), style="CodeBlock", code=True))

    section = (
        "<w:sectPr>"
        '<w:pgSz w:w="11906" w:h="16838"/>'
        '<w:pgMar w:top="1134" w:right="1134" w:bottom="1134" '
        'w:left="1134" w:header="708" w:footer="708" w:gutter="0"/>'
        "</w:sectPr>"
    )
    return "".join(blocks) + section


def _styles() -> str:
    return """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:styles xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:docDefaults>
    <w:rPrDefault><w:rPr>
      <w:rFonts w:ascii="Aptos" w:hAnsi="Aptos"/>
      <w:sz w:val="21"/><w:szCs w:val="21"/>
      <w:lang w:val="nl-BE"/>
    </w:rPr></w:rPrDefault>
    <w:pPrDefault><w:pPr><w:spacing w:after="120" w:line="276" w:lineRule="auto"/></w:pPr></w:pPrDefault>
  </w:docDefaults>
  <w:style w:type="paragraph" w:default="1" w:styleId="Normal">
    <w:name w:val="Normal"/>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Heading1">
    <w:name w:val="heading 1"/><w:basedOn w:val="Normal"/>
    <w:next w:val="Normal"/><w:uiPriority w:val="9"/><w:qFormat/>
    <w:pPr><w:keepNext/><w:keepLines/><w:spacing w:before="360" w:after="140"/></w:pPr>
    <w:rPr><w:b/><w:color w:val="17365D"/><w:sz w:val="34"/></w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Heading2">
    <w:name w:val="heading 2"/><w:basedOn w:val="Normal"/>
    <w:next w:val="Normal"/><w:qFormat/>
    <w:pPr><w:keepNext/><w:spacing w:before="280" w:after="100"/></w:pPr>
    <w:rPr><w:b/><w:color w:val="245B78"/><w:sz w:val="29"/></w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Heading3">
    <w:name w:val="heading 3"/><w:basedOn w:val="Normal"/>
    <w:next w:val="Normal"/><w:qFormat/>
    <w:pPr><w:keepNext/><w:spacing w:before="220" w:after="80"/></w:pPr>
    <w:rPr><w:b/><w:color w:val="2C6E8F"/><w:sz w:val="25"/></w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Heading4">
    <w:name w:val="heading 4"/><w:basedOn w:val="Normal"/>
    <w:next w:val="Normal"/><w:qFormat/>
    <w:pPr><w:keepNext/><w:spacing w:before="180" w:after="60"/></w:pPr>
    <w:rPr><w:b/><w:color w:val="335F73"/><w:sz w:val="22"/></w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="ListText">
    <w:name w:val="List Text"/><w:basedOn w:val="Normal"/>
    <w:pPr><w:spacing w:after="50"/></w:pPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Quote">
    <w:name w:val="Quote"/><w:basedOn w:val="Normal"/>
    <w:pPr><w:spacing w:before="80" w:after="120"/></w:pPr>
    <w:rPr><w:i/><w:color w:val="555555"/></w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="CodeBlock">
    <w:name w:val="Code Block"/><w:basedOn w:val="Normal"/>
    <w:pPr><w:spacing w:before="80" w:after="120" w:line="220" w:lineRule="auto"/>
      <w:shd w:fill="F3F5F7"/><w:ind w:left="180" w:right="180"/></w:pPr>
    <w:rPr><w:rFonts w:ascii="Courier New" w:hAnsi="Courier New"/><w:sz w:val="16"/></w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="TableText">
    <w:name w:val="Table Text"/><w:basedOn w:val="Normal"/>
    <w:pPr><w:spacing w:after="20" w:line="210" w:lineRule="auto"/></w:pPr>
    <w:rPr><w:sz w:val="17"/></w:rPr>
  </w:style>
  <w:style w:type="table" w:styleId="TableGrid">
    <w:name w:val="Table Grid"/><w:uiPriority w:val="59"/><w:qFormat/>
    <w:tblPr><w:tblBorders>
      <w:top w:val="single" w:sz="4" w:color="B7C9D6"/>
      <w:left w:val="single" w:sz="4" w:color="B7C9D6"/>
      <w:bottom w:val="single" w:sz="4" w:color="B7C9D6"/>
      <w:right w:val="single" w:sz="4" w:color="B7C9D6"/>
      <w:insideH w:val="single" w:sz="4" w:color="D5E0E7"/>
      <w:insideV w:val="single" w:sz="4" w:color="D5E0E7"/>
    </w:tblBorders></w:tblPr>
  </w:style>
</w:styles>"""


def build(
    source: Path,
    destination: Path,
    *,
    appendices: Sequence[Path] = (),
) -> None:
    sources = (source, *appendices)
    resolved_sources = tuple(path.resolve() for path in sources)
    if len(set(resolved_sources)) != len(resolved_sources):
        raise ValueError("elk Markdown-bronbestand mag maar eenmaal voorkomen")
    markdown = (
        f"\n\n{_PAGE_BREAK_MARKER}\n\n".join(
            path.read_text(encoding="utf-8") for path in resolved_sources
        )
    )
    created = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    document = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        f"<w:body>{_document_body(markdown)}</w:body></w:document>"
    )
    parts = {
        "[Content_Types].xml": """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
  <Override PartName="/word/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml"/>
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
</Types>""",
        "_rels/.rels": """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>""",
        "word/document.xml": document,
        "word/styles.xml": _styles(),
        "word/_rels/document.xml.rels": """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"/>""",
        "docProps/core.xml": f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties"
 xmlns:dc="http://purl.org/dc/elements/1.1/"
 xmlns:dcterms="http://purl.org/dc/terms/"
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:title>{escape(source.stem)}</dc:title>
  <dc:subject>Storm Tracker V3 meteorologische en technische roadmap</dc:subject>
  <dc:creator>Storm Tracker V3 / Codex</dc:creator>
  <cp:lastModifiedBy>Storm Tracker V3 / Codex</cp:lastModifiedBy>
  <dcterms:created xsi:type="dcterms:W3CDTF">{created}</dcterms:created>
  <dcterms:modified xsi:type="dcterms:W3CDTF">{created}</dcterms:modified>
</cp:coreProperties>""",
        "docProps/app.xml": """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties"
 xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">
  <Application>Storm Tracker V3 OOXML builder</Application>
</Properties>""",
    }
    destination.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(
        destination,
        "w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=9,
    ) as archive:
        for name, payload in parts.items():
            archive.writestr(name, payload.encode("utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument(
        "--appendix",
        action="append",
        default=[],
        type=Path,
        help=(
            "extra Markdown-bron die na een paginaovergang aan het dossier "
            "wordt toegevoegd; mag meermaals worden opgegeven"
        ),
    )
    args = parser.parse_args()
    build(
        args.source.resolve(),
        args.destination.resolve(),
        appendices=tuple(path.resolve() for path in args.appendix),
    )


if __name__ == "__main__":
    main()
