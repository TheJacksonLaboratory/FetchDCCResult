@echo off

ECHO Populating ebi procedure table
set VAR_1="ebi"
set VAR_2="-p"
python src\App.py %VAR_1% %VAR_2%
ECHO Job done

ECHO Populating ebi images table
set VAR_1="ebi"
set VAR_2="-i"
python src\App.py %VAR_1% %VAR_2%
ECHO Job done
