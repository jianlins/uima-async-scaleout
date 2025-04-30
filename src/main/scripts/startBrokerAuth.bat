@echo off

REM   Licensed to the Apache Software Foundation (ASF) under one
REM   or more contributor license agreements.  See the NOTICE file
REM   distributed with this work for additional information
REM   regarding copyright ownership.  The ASF licenses this file
REM   to you under the Apache License, Version 2.0 (the
REM   "License"); you may not use this file except in compliance
REM   with the License.  You may obtain a copy of the License at
REM
REM    http://www.apache.org/licenses/LICENSE-2.0
REM
REM   Unless required by applicable law or agreed to in writing,
REM   software distributed under the License is distributed on an
REM   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
REM   KIND, either express or implied.  See the License for the
REM   specific language governing permissions and limitations
REM   under the License.

@if not defined UIMA_HOME goto USAGE_UIMA
@goto RUN

:USAGE_UIMA
@echo Setting UIMA_HOME based on current location...
@set "CURRENT_PATH=%~dp0"

@REM First check if we're in a packaged structure with bin directory
@if "%CURRENT_PATH:~-4%"=="bin\" (
    @for %%I in ("%~dp0..") do set "UIMA_HOME=%%~fI"
    @cd /d ..
    @goto UIMA_HOME_SET
)


@REM Default case - use current directory
@set "UIMA_HOME=%~dp0"
@cd /d %UIMA_HOME%

:UIMA_HOME_SET
@echo Current working directory is:
@cd

@echo UIMA_HOME set to: %UIMA_HOME%
@goto RUN

:RUN

@echo on

@REM  ActiveMQ needs a HOME
@setlocal
@if "%ACTIVEMQ_HOME%" == "" (
  set "ACTIVEMQ_HOME=%UIMA_HOME%\apache-activemq"
)

@echo ACTIVEMQ_HOME set to: %ACTIVEMQ_HOME%

@REM  ActiveMQ needs a writable directory for the log files and derbydb
@REM  watchout! it appears that ACTIVEMQ_BASE cannot contain backslashes!
@if "%ACTIVEMQ_BASE%" == "" (
  set ACTIVEMQ_BASE=amq_auth
)
@echo Set ACTIVEMQ_BASE to %ACTIVEMQ_BASE%

@REM If directory missing create it
@if not exist "%ACTIVEMQ_BASE%" (
  @echo Create ACTIVEMQ_BASE to %ACTIVEMQ_BASE%
  mkdir "%ACTIVEMQ_BASE%"
  mkdir "%ACTIVEMQ_BASE%\conf"
)

@REM Copy test configuration files
@echo Copying configuration files from test resources...

@REM Try different paths to find the config files based on project structure
@set "CONFIG_FOUND=false"

@REM First try the standard path in the source project
@if exist "%UIMA_HOME%\uimaj-as-activemq\src\test\resources\activemq-auth.xml" (
  @copy "%UIMA_HOME%\uimaj-as-activemq\src\test\resources\activemq-auth.xml" "%ACTIVEMQ_BASE%\conf\"
  @copy "%UIMA_HOME%\uimaj-as-activemq\src\test\resources\credentials.properties" "%ACTIVEMQ_BASE%\conf\"
  @echo Found config files in uimaj-as-activemq\src\test\resources
  @set "CONFIG_FOUND=true"
) else (
  @echo Could not find config files in uimaj-as-activemq\src\test\resources
)

@REM Try Maven target path
@if %CONFIG_FOUND%==false (
  @if exist "%UIMA_HOME%\uimaj-as-activemq\target\test-classes\activemq-auth.xml" (
    @copy "%UIMA_HOME%\uimaj-as-activemq\target\test-classes\activemq-auth.xml" "%ACTIVEMQ_BASE%\conf\"
    @copy "%UIMA_HOME%\uimaj-as-activemq\target\test-classes\credentials.properties" "%ACTIVEMQ_BASE%\conf\"
    @echo Found config files in uimaj-as-activemq\target\test-classes
    @set "CONFIG_FOUND=true"
  ) else (
    @echo Could not find config files in uimaj-as-activemq\target\test-classes
  )
)

@REM If still not found, try a few other common locations
@if %CONFIG_FOUND%==false (
  @if exist "%UIMA_HOME%\src\test\resources\activemq-auth.xml" (
    @copy "%UIMA_HOME%\src\test\resources\activemq-auth.xml" "%ACTIVEMQ_BASE%\conf\"
    @copy "%UIMA_HOME%\src\test\resources\credentials.properties" "%ACTIVEMQ_BASE%\conf\"
    @echo Found config files in src\test\resources
    @set "CONFIG_FOUND=true"
  ) else (
    @echo Could not find config files in src\test\resources
  )
)

@if %CONFIG_FOUND%==false (
  @echo ERROR: Could not find configuration files in any expected location.
  @echo Please make sure activemq-auth.xml and credentials.properties exist in one of the expected paths.
  @goto EXIT
)

@copy "%ACTIVEMQ_HOME%\conf\log4j.properties" "%ACTIVEMQ_BASE%\conf\"

@REM Pass authentication credentials as system properties
@set ACTIVEMQ_OPTS=-Dactivemq.username=uimauser -Dactivemq.password=uimapass

call "%ACTIVEMQ_HOME%\bin\activemq.bat" "start" "xbean:file:%ACTIVEMQ_BASE%/conf/activemq-auth.xml"
:EXIT
