import subprocess
import numpy as np
import json
import sys


def parse_sqlcmd_output_table(returned_text):
    
    """ parse sqlcmd output string into 2D array"""
    
    temp_array = np.array([[j for j in i.split('\t')] for i in returned_text.splitlines()])
    listToStr = ' '.join([str(elem) for elem in temp_array[1]])
    c = ' -'
    array_positions = [n for n in range(len(listToStr)) if listToStr.find(c, n) == n]
    array_positions_shift = [x + 1 for x in array_positions]
    output = []
    l = [0] + array_positions_shift + [len(listToStr)]
    for _list in temp_array:
        temp_str = str(_list[0])
        split_list = [temp_str[x:y] for x,y in zip(l, l[1:])]
        split_list = [x.lstrip() for x in split_list]
        split_list = [x.rstrip() for x in split_list]
        output.append(split_list)
    return output


def start_job(ip,port,username,password,job_name):
    
    """ 
    arguments:
              ip:Microsoft SQL server IP address (string type)
              port: Microsoft SQL server service port (string type)
              username: DB user
              password: DB password
              job_name: job name of an existing job on Microsoft SQL agent service
              
    returns: 
            true: if job starts successfully
            false: otherwise 
    """
    
    query='Sqlcmd -S \"'+ip+','+port+'\" -U \"'+username+'\" -P \"'+password+'\" -Q\"exec msdb.dbo.sp_start_job @job_name =\''+job_name+'\'"'
    returned_text=subprocess.check_output(query, shell=True, universal_newlines=True)
    return returned_text.__contains__("successfully")


def get_job_status(ip,port,username,password,job_name):
    
    """
    arguments:
              ip:Microsoft SQL server IP address (string type)
              port: Microsoft SQL server service port (string type)
              username: DB user
              password: DB password
              job_name: job name of an existing job on Microsoft SQL agent service
              
    returns: 
              0: if all steps in the job fail
              1: if all steps in the job succeed 
              2: if some steps succeed and other fail
              3: if the job still running
    """
    
    query='Sqlcmd -S \"'+ip+','+port+'\" -U \"'+username+'\" -P \"'+password+'\" -Q\"exec msdb.dbo.sp_help_jobactivity @job_name =\''+job_name+'\'"'
    returned_text=subprocess.check_output(query, shell=True, universal_newlines=True)
    returned_table=parse_sqlcmd_output_table(returned_text)
    
    if returned_table[2][13]=='NULL':
        return 3
    
    
    query='Sqlcmd -S \"'+ip+','+port+'\" -U \"'+username+'\" -P \"'+password+'\" -Q\"exec msdb.dbo.sp_help_jobhistory @job_name =\''+job_name+'\',@mode=\'FULL\'"'
    returned_text=subprocess.check_output(query, shell=True, universal_newlines=True)
    returned_table=parse_sqlcmd_output_table(returned_text)
    
    step_status_list=[]
    
    if(returned_table[2][3]=='0'):
        step_count=int(returned_table[3][3])
        j=3
        
    else:
        step_count=int(returned_table[2][3])
        j=2
    for i in range(step_count):
        step_status_list.append(int(returned_table[j+i][8]))
    
    if step_status_list.count(1) == len(step_status_list):
        return 1
    elif step_status_list.count(0) == len(step_status_list):
        return 0
    elif step_status_list.__contains__(1):
        return 2
    else:
        return 0


if __name__ == "__main__":
    
    # Opening JSON file
    f = open('config.json',)
    # returns JSON object as
    # a dictionarusernamey
    config = json.load(f)
    # Closing file
    f.close()
    
    ip=config['ip']
    port=config['port']
    username=config['username']
    password=config['password']
    
    if sys.argv[2]=="start":
        job_name=sys.argv[1]
        print(start_job(ip,port,username,password,job_name))
    elif sys.argv[2]=="status":
        job_name=sys.argv[1]
        print(get_job_status(ip,port,username,password,job_name))
    else: print("please enter the arguments (job_name  start/status)")
    