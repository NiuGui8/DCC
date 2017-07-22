@echo off
set ClientNum=10
set TestCount=100000
set Server=localhost
REM set Server=192.168.2.141
set Port=6666
set InstrumentName=COD
set InstrumentType=PT62S
set Interval=10

set JavaProgram=luip.jar
set RunCmd=java -jar %JavaProgram% -id %%i -name %InstrumentName% CODdd -type %InstrumentType% -interval %Interval% -server %Server% -port %Port%

for /l %%i in (1,1,%ClientNum%) do (
    start %RunCmd%
)

REM pause