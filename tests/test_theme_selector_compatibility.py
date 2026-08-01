from pathlib import Path


def test_pages_do_not_pass_compact_keyword_to_theme_selector():
    root = Path(__file__).resolve().parents[1]
    targets = [root / "login_app.py", root / "core" / "auth.py"]
    for path in targets:
        source = path.read_text(encoding="utf-8")
        assert "render_theme_selector" in source
        assert "compact=True" not in source


def test_theme_selector_accepts_legacy_compact_parameter():
    source = (Path(__file__).resolve().parents[1] / "core" / "styles.py").read_text(encoding="utf-8")
    assert "compact: bool | None = None" in source
    assert "**_: object" in source
