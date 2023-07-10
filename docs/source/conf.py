# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys
import codecs

import sphinx_bootstrap_theme

sys.path.insert(0, os.path.abspath("../../"))

project = "snowflake-utilities"
copyright = "2023, James Twose"
author = "James Twose"


def read(rel_path):
    here = os.path.abspath(os.path.dirname("__file__"))
    with codecs.open(os.path.join(here, rel_path), "r") as fp:
        return fp.read()


def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith("__version__"):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError("Unable to find version string.")


release = get_version("../../snowflake_utilities/__init__.py")

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.duration",
    "sphinx.ext.doctest",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.mathjax",
    "sphinx.ext.viewcode",
    "numpydoc",
    "sphinx.ext.inheritance_diagram",
    "nbsphinx",
    "nbsphinx_link",
    'sphinx_copybutton'
]

templates_path = ["_templates"]
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "bootstrap"
html_theme_path = sphinx_bootstrap_theme.get_html_theme_path()
html_static_path = ["_static"]


# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
html_theme_options = {
    "source_link_position": "footer",
    "bootswatch_theme": "simplex",
    "navbar_title": "snowflake-utilities",
    "navbar_sidebarrel": False,
    "bootstrap_version": "4",
    "nosidebar": True,
    "body_max_width": "100%",
    "navbar_links": [
        # ("Gallery", "examples/index"),
        # ("Tutorial", "tutorial"),
        # ("Home", "index"),
        # ("API", "api"),
    ],
}


# Add the custom css we wrote to the build
def setup(app):
    app.add_css_file("custom.css")


# Generate the API documentation when building
autosummary_generate = True
numpydoc_show_class_members = False