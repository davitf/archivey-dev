# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys
sys.path.insert(0, os.path.abspath('../src'))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'archivey'
copyright = '2024, davi davi'
author = 'davi davi'
release = '0.1.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.intersphinx',
    'sphinx.ext.viewcode',
    'sphinx_autodoc_typehints',
    'myst_parser',
    'sphinx_copybutton',
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'furo'
html_static_path = ['_static']

# -- Intersphinx configuration ---------------------------------------------
intersphinx_mapping = {'python': ('https://docs.python.org/3', None)}

# -- Napoleon settings -----------------------------------------------------
napoleon_google_docstring = True
napoleon_numpy_docstring = False

# -- Autodoc settings ------------------------------------------------------
autodoc_member_order = 'bysource'
# sphinx-autodoc-typehints settings
# sphinx_autodoc_typehints_format_rtype = 'sphinx' # Optional: to change return type format
# typehints_fully_qualified = False # Optional: to shorten type hints
# always_document_param_types = True # Optional: to always include param types
# typehints_document_rtype = True # Optional: to document return types by default
