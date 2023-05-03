#!/bin/bash
coverage run -m pytest
rm -rf "*.pyc"
coverage report xml -i
coverage report