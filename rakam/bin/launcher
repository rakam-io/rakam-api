#!/bin/sh -eu
#
# Launcher for Airlift applications
#
# Java must be in PATH.
#
# This launcher script, launcher.py and launcher.properties must be
# located in 'bin'. The properties file must contain a 'main-class'
# entry that specifies the Java class to execute.
#
# The classpath will contain everything in the 'lib' directory.
#
# Config files must be located in 'etc':
#
#   jvm.config          -- required: Java command line options, one per line
#   config.properties   -- required: application configuration properties
#   node.properties     -- optional: application environment properties
#   log.properties      -- optional: log levels
#
# The 'etc' and 'plugin' directories will be symlinked into the data
# directory before the process is started, allowing the application to
# easily reference these at runtime.
#
# When run as a daemon, the application will log to the server log and
# stdout and stderr are redirected to the launcher log.
#
# The following commands are supported:
#
#   run     -- run the application in the foreground (for debugging)
#   start   -- run the application as a daemon
#   stop    -- request the application to terminate (SIGTERM)
#   kill    -- forcibly terminate the application (SIGKILL)
#   restart -- run the stop command, then run the start command
#   status  -- check if the application is running (0=true, 3=false)
#
# Run with --help to see options.
#

exec "$(dirname "$0")/launcher.py" "$@"
