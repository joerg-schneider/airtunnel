#!/usr/bin/env bash
cd ${AIRTUNNEL_HOME}
pytest -s test/conftest.py
pytest -s test/