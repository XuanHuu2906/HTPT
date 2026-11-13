#!/usr/bin/env python3
"""Fail CI if real PostgreSQL D13 tests were missing, skipped, or unsuccessful."""
from pathlib import Path
import xml.etree.ElementTree as ET

reports = list(Path('backend-node/target/surefire-reports').glob(
    'TEST-*PersistenceRestartTest$GroupB_RealPostgreSqlRestartTests.xml'))
if len(reports) != 1:
    raise SystemExit('D13 PostgreSQL report is missing or ambiguous')
suite = ET.parse(reports[0]).getroot()
tests = int(suite.attrib['tests'])
if tests < 1 or any(int(suite.attrib.get(key, '0')) for key in ('failures', 'errors', 'skipped')):
    raise SystemExit(f'D13 PostgreSQL suite did not run completely: {suite.attrib}')
print(f'D13 PostgreSQL: {tests} tests, 0 failures, 0 errors, 0 skipped')
