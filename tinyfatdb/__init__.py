# -*- coding: utf-8 -*-

"""Top-level package for TinyFatDB."""

__author__ = """Carlos Velez"""
__email__ = 'velez.carlos.a@gmail.com'
__version__ = '0.1.0'

from .databases import TinyFatDB
from .tables import TinyFatTable
from .models import TinyFatModel
from .querysets import TinyFatQueryset

import os
MODELS_DIR = os.path.join(os.path.dirname(__file__), "dbs")
if not os.path.exists(MODELS_DIR):
    os.mkdir(MODELS_DIR)
