"""Shared pytest configuration."""

import pytest


def pytest_configure(config):
    config.addinivalue_line("markers", "live: tests that hit live external APIs (skip with -m 'not live')")
