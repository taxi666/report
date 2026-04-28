@echo off
setlocal

for %%I in ("%~dp0..") do set PROJECT_ROOT=%%~fI
set JAVA_EXE=%PROJECT_ROOT%\tools\java\jdk-17.0.18+8\bin\java.exe
set KAFKA_HOME=%PROJECT_ROOT%\tools\kafka_2.13-4.2.0
set KAFKA_CONFIG=%PROJECT_ROOT%\infra\kafka-local.properties
set KAFKA_STDOUT=%PROJECT_ROOT%\kafka.stdout.log
set KAFKA_STDERR=%PROJECT_ROOT%\kafka.stderr.log

"%JAVA_EXE%" -Xmx1G -Xms1G -Dlog4j2.configurationFile=file:%PROJECT_ROOT:\=/%/tools/kafka_2.13-4.2.0/config/log4j2.yaml -cp "%KAFKA_HOME%\libs\*" kafka.Kafka "%KAFKA_CONFIG%" 1>>"%KAFKA_STDOUT%" 2>>"%KAFKA_STDERR%"
