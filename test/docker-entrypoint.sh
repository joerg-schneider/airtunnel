#!/usr/bin/env bash
cd ${AIRTUNNEL_HOME}
pytest -s test/test_airtunnel/conftest.py
pytest -s test/test_airtunnel