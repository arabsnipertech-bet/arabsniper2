"""Stable 3appdays entry point for the universal lateral observation engine."""
from lateral_matrix import MODEL_ID
from statistics_integration import install as _install


def install(ns):
    return _install(ns, matrix_mode=True)
