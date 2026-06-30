SPHINXOPTS    ?=
SPHINXBUILD   ?= sphinx-build
SOURCEDIR     = .
BUILDDIR      = _build

.PHONY: help html clean

help:
	@echo "Użycie: make [target]"
	@echo ""
	@echo "Dostępne targety (buildery Sphinx):"
	@echo "  html        - standalone HTML files"
	@echo "  dirhtml     - HTML files named index.html in directories"
	@echo "  singlehtml  - single large HTML file"
	@echo "  pickle      - pickle files"
	@echo "  json        - JSON files"
	@echo "  htmlhelp    - HTML files and HTML help project"
	@echo "  qthelp      - HTML files and qthelp project"
	@echo "  devhelp     - HTML files and Devhelp project"
	@echo "  epub        - epub ebook"
	@echo "  latex       - LaTeX files (PAPER=a4 lub PAPER=letter)"
	@echo "  latexpdf    - LaTeX and PDF files (via pdflatex)"
	@echo "  latexpdfja  - LaTeX files (via platex/dvipdfmx)"
	@echo "  text        - plain text files"
	@echo "  man         - manual pages"
	@echo "  texinfo     - Texinfo files"
	@echo "  info        - Texinfo files and run makeinfo"
	@echo "  gettext     - PO message catalogs"
	@echo "  changes     - overview of changes/added/deprecated"
	@echo "  xml         - Docutils-native XML files"
	@echo "  pseudoxml   - pseudoxml-XML files"
	@echo "  linkcheck   - check all external links"
	@echo "  doctest     - run doctests in documentation"
	@echo "  coverage    - check documentation coverage"
	@echo ""
	@echo "Specjalne targety:"
	@echo "  clean       - remove everything in build directory"
	@echo "  help        - show this message"

html:
	@$(SPHINXBUILD) -M html "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
	@echo
	@echo "Build gotowy: $(BUILDDIR)/html/index.html"

clean:
	@rm -rf "$(BUILDDIR)"
	@echo "Katalog $(BUILDDIR) usunięty."

# Uniwersalny target dla wszystkich innych builderów Sphinx
%:
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
