#!/bin/bash

# Install Playwright browsers
playwright install chromium

# Set environment variable for Chromium to run in sandboxed environment
export PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH=/usr/bin/chromium 