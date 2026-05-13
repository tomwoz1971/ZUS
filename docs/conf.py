"""Sphinx configuration for zus_db_utils documentation."""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

# ---------------------------------------------------------------------------
# Projekt
# ---------------------------------------------------------------------------
project = "zus_db_utils"
copyright = "2026, Zakład Ubezpieczeń Społecznych"
author = "Zakład Ubezpieczeń Społecznych"
release = "0.1.0"

# ---------------------------------------------------------------------------
# Rozszerzenia
# ---------------------------------------------------------------------------
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx.ext.autosummary",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
language = "pl"

# ---------------------------------------------------------------------------
# Autodoc
# ---------------------------------------------------------------------------
autodoc_default_options = {
    "members": True,
    "undoc-members": False,
    "show-inheritance": True,
    "member-order": "bysource",
    "special-members": "__init__",
}
autodoc_typehints = "description"
autodoc_typehints_description_target = "documented"
autosummary_generate = True

# ---------------------------------------------------------------------------
# Intersphinx
# ---------------------------------------------------------------------------
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "sqlalchemy": ("https://docs.sqlalchemy.org/en/20/", None),
    "pandas": ("https://pandas.pydata.org/docs/", None),
    "pydantic": ("https://docs.pydantic.dev/latest/", None),
}

# ---------------------------------------------------------------------------
# HTML — motyw Furo z brandingiem ZUS
# ---------------------------------------------------------------------------
html_theme = "furo"
html_title = "zus_db_utils"
html_static_path = ["_static"]
html_css_files = ["zus.css"]

html_theme_options = {
    "light_css_variables": {
        "color-brand-primary": "#003F87",
        "color-brand-content": "#003F87",
        "color-highlight-on-target": "#dce9f5",
    },
    "dark_css_variables": {
        "color-brand-primary": "#4A90D9",
        "color-brand-content": "#4A90D9",
        "color-highlight-on-target": "#1a2e4a",
    },
    "navigation_with_keys": True,
    "source_repository": "https://github.com/tomwoz1971/ZUS/",
    "source_branch": "main",
    "source_directory": "docs/",
}
