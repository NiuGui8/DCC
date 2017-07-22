@echo off
set Port=6666
set Interval=100

set JavaProgram=server.jar
set RunCmd=java -jar %JavaProgram% --port %Port% -interval %Interval%
start %RunCmd%
REM pause