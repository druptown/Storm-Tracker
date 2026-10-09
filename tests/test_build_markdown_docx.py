from __future__ import annotations

import importlib.util
from pathlib import Path
from xml.etree import ElementTree
import zipfile


ROOT = Path(__file__).resolve().parents[1]


def _module():
    path = ROOT / "scripts" / "build_markdown_docx.py"
    spec = importlib.util.spec_from_file_location("build_markdown_docx", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_builder_creates_valid_minimal_docx_with_table_and_code(tmp_path):
    source = tmp_path / "audit.md"
    destination = tmp_path / "audit.docx"
    source.write_text(
        "# Titel\n\n"
        "Een **paragraaf** met [bron](https://example.invalid).\n\n"
        "| Veld | Waarde |\n"
        "|---|---|\n"
        "| status | shadow |\n\n"
        "- opsomming\n\n"
        "```text\nexact bewijs\n```\n",
        encoding="utf-8",
    )

    _module().build(source, destination)

    assert destination.is_file()
    with zipfile.ZipFile(destination) as archive:
        names = set(archive.namelist())
        assert {
            "[Content_Types].xml",
            "_rels/.rels",
            "word/document.xml",
            "word/styles.xml",
            "docProps/core.xml",
        }.issubset(names)
        for name in names:
            if name.endswith((".xml", ".rels")):
                ElementTree.fromstring(archive.read(name))
        document = archive.read("word/document.xml").decode("utf-8")
    assert "Titel" in document
    assert "status" in document
    assert "exact bewijs" in document
    assert "\u2022 opsomming" in document


def test_builder_combines_appendices_with_real_page_breaks(tmp_path):
    source = tmp_path / "roadmap.md"
    appendix_a = tmp_path / "provider.md"
    appendix_b = tmp_path / "verification.md"
    destination = tmp_path / "dossier.docx"
    source.write_text("# Roadmap\n\nHoofdtekst.\n", encoding="utf-8")
    appendix_a.write_text("# Providerbijlage\n\nBroncontract.\n", encoding="utf-8")
    appendix_b.write_text(
        "# Verificatiebijlage\n\nUitkomstcontract.\n",
        encoding="utf-8",
    )

    _module().build(
        source,
        destination,
        appendices=(appendix_a, appendix_b),
    )

    with zipfile.ZipFile(destination) as archive:
        document = archive.read("word/document.xml").decode("utf-8")
    assert "Roadmap" in document
    assert "Providerbijlage" in document
    assert "Verificatiebijlage" in document
    assert document.count('<w:br w:type="page"/>') == 2


def test_builder_rejects_duplicate_sources(tmp_path):
    source = tmp_path / "roadmap.md"
    source.write_text("# Roadmap\n", encoding="utf-8")

    try:
        _module().build(
            source,
            tmp_path / "dossier.docx",
            appendices=(source,),
        )
    except ValueError as error:
        assert "maar eenmaal" in str(error)
    else:
        raise AssertionError("dubbele dossierbron werd niet geweigerd")
