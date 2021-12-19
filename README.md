# Airflow-Refresh-Script-for-SQl-Server-Analysis-Service-Tables-SSAS-

## run command: 

###### python SQL_Jobs_Python.py <job_name> <start/status>


## example:

###### python SQL_Jobs_Python.py test start


## expected outputs:

#### in case start job:

###### true: if job starts successfully
###### false: otherwise 

#### in case get status:
#### 0: if all steps in the job fail
#### 1: if all steps in the job succeed 
#### 2: if some steps succeed and other fail
#### 3: if the job still running				
