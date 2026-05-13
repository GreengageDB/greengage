#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) Greenplum Inc 2008. All Rights Reserved. 
#
import logging

import gplog
from test.unit.gp_unittest import GpTestCase, run_tests


class GplogTestCase(GpTestCase):
    def test_basics(self):
        logger = gplog.get_default_logger()
        self.assertTrue(logger is not None)

    def test_set_loglevels(self):
        logger = gplog.get_default_logger()
        self.assertTrue(logger.getEffectiveLevel() == logging.INFO)
        gplog.enable_verbose_logging()
        self.assertTrue(logger.getEffectiveLevel() == logging.DEBUG)

    def test_log_to_file_only_is_ok_when_not_initialized(self):
        gplog._LOGGER = None
        gplog.log_to_file_only("should not crash")

    def test_log_to_file_only_is_ok_when_stdout_not_initialized(self):
        gplog._LOGGER = None
        gplog.get_unittest_logger()
        gplog.log_to_file_only("should not crash")

    def test_log_exception_with_unicode(self):
        logger = gplog.get_default_logger()
        logger.exception(Exception(u"тест".encode('utf-8')))
        logger.exception(Exception(u"тест"))

    def test_log_unicode(self):
        logger = gplog.get_default_logger()
        logger.log(logging.INFO, u"тест")

    def test_log_binary_string(self):
        logger = gplog.get_default_logger()
        logger.log(logging.INFO, b"test")

    def test_log_binary_string_with_unicode(self):
        logger = gplog.get_default_logger()
        logger.log(logging.INFO, u"тест".encode('utf-8'))

if __name__ == '__main__':
    run_tests()
