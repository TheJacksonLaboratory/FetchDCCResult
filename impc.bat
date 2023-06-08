@echo off

ECHO Populating experiments on lines table
set VAR_1="IMPC"
set VAR_2="-l"
python src\App.py %VAR_1% %VAR_2%
ECHO Job done

timeout 5 > NUL

ECHO Populating experiments on specimens table
set VAR_1="IMPC"
set VAR_2="-e"
python src\App.py %VAR_1% %VAR_2%
ECHO Job done

timeout 5 > NUL

ECHO Populating images table
set VAR_1="IMPC"
set VAR_2="-i"
python src\App.py %VAR_1% %VAR_2%
ECHO Job done

ECHO Populating xml tables
set VAR_1="IMPC"
set VAR_2="-xml"
python src\App.py %VAR_1% %VAR_2%
ECHO Job done

ECHO All processes finished
@pause