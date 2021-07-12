# Tomcat web server log parser
Python based parser for Tomcat webser log files

## Overview
There's probably much better ways to achieve this, but this was what worked for our use case.

## How to
 - Download log files from Tomcat eb server to the `log-files` directory
 - Add any site URLs that you specifically want to capture/count/record in the `url-dict.py` (see url-dict-template.py for format).