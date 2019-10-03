#!/usr/bin/env bash
cd ${AIRTUNNEL_HOME}/test
pytest -s test_airtunnel/conftest.py
pytest -s . -s --doctest-modules --cov=airtunnel