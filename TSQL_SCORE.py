
import re
import sys

filepath = ''
sf_line=[]
sf_final=[]
first_word_array =[]
stmts=['']
multistatement = 0
sql_text = 0
# Change Trackers
loop_cnt = -1
loop_array = [
'FOR', 'WHILE'
]

flow_cnt = 0
flow_arr=[
'BEGIN ',
#'END'
'RETURN',
'BREAK',
'THROW',
'CONTINUE',
'TRY',
'CATCH',
'GOTO',
'WAITFOR',
'IF ',
'ELSE',
'WHILE'
]

variables_cnt = 0
cursor_cnt = 0
trigger_cnt = 0
# TSQL Aggregate Functions
agg_cnt = 0
agg_arr=[
'AVG'
,'COUNT'
,'MAX'
,'MIN'
,'SUM'
]

# TSQL System Stored Procedures
ssp_cnt = 0
ssp_arr=[
'Sp_addextendedproperty'
,'Sp_autostats'
,'Sp_columns'
,'Sp_column_privileges'
,'Sp_special_columns'
,'Sp_configure'
,'Sp_databases'
,'Sp_execute'
,'Sp_executesql'
,'Sp_fkeys'
,'Sp_help'
,'Sp_helpconstraint'
,'Sp_helpdb'
,'Sp_helpindex'
,'Sp_lock'
,'Sp_monitor'
,'Sp_prepare'
,'Sp_pkeys'
,'Sp_rename'
,'Sp_renamedb'
,'Sp_tables'
,'Sp_helptrigger'
,'Sp_table_privileges'
,'Sp_server_info'
,'Sp_statistics'
,'Sp_stored_procedures'
,'Sp_unprepare'
,'Sp_updatestats'
,'Sp_who]']

# TSQL String Functions
str_cnt = 0
str_arr=[
'Charindex'
,'Concat'
,'Left'
,'Len'
,'Lower'
,'Ltrim'
,'Substring'
,'Patindex'
,'Replace'
,'Right'
,'Rtrim'
,'Upper']
# TSQL Date and Time Data Types and Functions
data_time_cnt = 0
data_time_arr=[
'@@DATEFIRST'
,'CURRENT_TIMESTAMP'
,'DATEADD'
,'DATEDIFF'
,'DATEFROMPARTS'
,'DATENAME'
,'DATEPART'
,'DATETIMEFROMPARTS'
,'DATETIME2FROMPARTS'
,'DAY'
,'EOMONTH'
,'GETDATE()'
,'GETUTCDATE()'
,'ISDATE'
,'MONTH'
,'SMALLDATETIMEFROMPARTS'
,'SWITCHOFFSET'
,'SYSDATETIME'
,'SYSDATETIMEOFFSET'
,'SYSUTCDATETIME'
,'TIMEFROMPARTS'
,'TODATETIMEOFFSET'
,'YEAR'
]
# TSQL System Functions
sys_fun_cnt = 0
sys_fun_arr=[
'@@CONNECTIONS',
'@@ERROR',
'@@IDENTITY',
'@@ROWCOUNT',
'COALESCE',
'ERROR_LINE',
'ERROR_MESSAGE',
'ERROR_NUMBER',
'ERROR_PROCEDURE',
'ERROR_SEVERITY',
'ERROR_STATE',
'HOST_ID',
'HOST_NAME',
'ISNULL',
'ISNUMERIC',
'NULLIF']
# TSQL Security Functions
secfun_cnt = 0
secfun_arr = [
'CURRENT_USER',
'ORIGINAL_LOGIN',
'SESSION_USER',
'SYSTEM_USER',
'USER_NAME']
# TSQL Metadata Functions
metadata_cnt = 0
metadata_arr =[
'APP_NAME',
'DB_ID',
'DB_NAME',
'OBJECT_DEFINITION',
'OBJECT_ID',
'OBJECT_NAME',
'OBJECT_SCHEMA_NAME',
'SCHEMA_ID',
'SCHEMA_NAME']
# TSQL Configuration Functions
Configuration_cnt = 0
Configuration_arr =[
'@@LOCK_TIMEOUT',
'@@MAX_CONNECTIONS',
'@@SERVERNAME',
'@@SERVICENAME',
'@@SPID']
# TSQL Conversion Functions
conversion_cnt = 0
conversion_arr =[
'CAST'
'CONVERT',
'PARSE',
'TRY_CAST',
'TRY_CONVERT',
'TRY_PARSE',
]
# TSQL DDL
DDL_CNT = 0
DDL_ARR = [
'CREATE',
'ALTER',
'DROP',
'RENAME',
'DISABLE TRIGGER',
'ENABLE TRIGGER',
'COLLATIONS',
'UPDATE STATISTICS']

#TSQL DML
DML_CNT = 0
DML_CONFLICTS = 0
DML_ARR =[
'INSERT',
'DELETE',
'MERGE',
'BULK INSERT',
'TRUNCATE TABLE',
'SELECT',
'SET'
]


row_index ={1:{
'old_line':'',
'new_line':'',
'anaylsis':['']
}}

def convert_vars(var_line):
    # fix database objects
    var_line=var_line.replace('dbo.','')
    #fix varariable callouts
    if 'SET @' in var_line.upper():
        var_line = re.sub("set @", "Set ", var_line, flags=re.I)
    if 'IF EXISTS(' in var_line.upper():
        var_line = "// FLAG -"+ var_line
    if "@@" in line:
        SPEC_VAR_CNT = SPEC_VAR_CNT+1
        var_line = var_line.replace("@@","/* flagged special variable")
        var_line = var_line+ "*/"
    if "DECLARE" in var_line.upper():
        var_line ='\n'
    var_line=var_line.replace('@','$')
    var_line=var_line.replace('[','')
    var_line=var_line.replace(']','')
    var_line=var_line.replace('#','TMP_TBL_')
    if "CREATE PROCEDURE" in line.upper():
        var_line =re.sub("Create", "CREATE or REPLACE", var_line, flags=re.I)
        var_line = re.sub("\n","()\n",var_line)
    elif var_line.strip() == 'AS':
        var_line = re.sub('AS','\nRETURNS VARCHAR\nLANGUAGE JAVASCRIPT\nEXECUTE AS CALLER\nAS\n',var_line, flags=re.I )

    elif "NOCOUNT" in var_line.upper():
        var_line = ''
    return var_line


with open(filepath) as fp:
   line = fp.readline()
   row_id = 1
   converted_line = convert_vars(line)
   while line:
       for word in DML_ARR:
           if word.upper() in line.upper():
               DML_CNT = DML_CNT+1
               row_index[row_id]['anaylsis'].append('DML')
               if word.upper() == 'BULK INSERT' or  word.upper() == 'TRUNCATE TABLE':
                   DML_CONFLICTS = DML_CONFLICTS + 1
       for word in DDL_ARR:
           if word.upper() in line.upper():
               DDL_CNT = DDL_CNT+1
               row_index[row_id]['anaylsis'].append('DDL')

       for word in Configuration_arr:
           if word.upper() in line.upper():
               Configuration_cnt = Configuration_cnt+1
               row_index[row_id]['anaylsis'].append('configuration')

       for word in metadata_arr:
           if word.upper() in line.upper():
               row_index[row_id]['anaylsis'].append('metadata')
               metadata_cnt = metadata_cnt+1

       for word in secfun_arr:
           if word.upper() in line.upper():
               secfun_cnt = secfun_cnt+1
               row_index[row_id]['anaylsis'].append('security_function')

       for word in sys_fun_arr:
           if word.upper() in line.upper():
               sys_fun_cnt = sys_fun_cnt+1
               row_index[row_id]['anaylsis'].append('sys_function')

       for word in str_arr:
           if word.upper() in line.upper():
               str_cnt = str_cnt+1
               row_index[row_id]['anaylsis'].append('string_function')

       for word in agg_arr:
           if word.upper() in line.upper():
               agg_cnt = agg_cnt+1
               row_index[row_id]['anaylsis'].append('agg_function')

       for word in conversion_arr:
           if word.upper() in line.upper():
               conversion_cnt = conversion_cnt+1
               row_index[row_id]['anaylsis'].append('conversion_function')

       for word in data_time_arr:
           if word.upper() in line.upper(): data_time_cnt = data_time_cnt+1

       for word in conversion_arr:
           if '@' in line: variables_cnt = variables_cnt+1

       for word in flow_arr:
           if word.upper() in line.upper(): flow_cnt = flow_cnt+1

       for word in ssp_arr:
           if word.upper() in line.upper(): ssp_cnt = ssp_cnt+1

       if 'CURSOR_PRODUCT' in line.upper(): cursor_cnt = cursor_cnt+1

       if 'TRIGGER' in line.upper(): trigger_cnt = trigger_cnt+1
       # Reformats the SP for Snowflake
       #print("Line {}: {}".format(cnt, convert(line.replace())))
       line = fp.readline()
       row_id = row_id+ 1
       converted_line = convert_vars(line)
       row_index[row_id] ={'old_line':line,'new_line':converted_line,'anaylsis':['']}


trncnt = 0


print('--------------------------------------------------------------')
print(' Toal lines scanned: '+ str(row_id))
print('--------------------------------------------------------------')
print('-------- COMMON SQL SYNTAX - REQUIRES MINIMAL REVIEW ---------')
print('--------------------------------------------------------------')
print()
print(' DML statements: '+ str(DML_CNT) + ' ['+ str(DML_CONFLICTS)+' conflicts]')
print(' Variables referenced: '+ str(variables_cnt))
print(' Security functions: '+ str(secfun_cnt))
print(' System functions: '+ str(sys_fun_cnt))
print(' String functions: '+str(str_cnt))
print(' Aggregate functions: '+str(agg_cnt))
print(' Date/time functions: '+str(data_time_cnt))
print(' Conversion functions: '+ str(conversion_cnt))
print()
print('--------------------------------------------------------------')
print('-------- TSQL SPECIFIC SYNTAX - REQUIRES MANUAL REVIEW -------')
print('--------------------------------------------------------------')
print()
print(' Flow control performed in procedure: ' + str(flow_cnt))
print(' Stystem Stored Procedures use in procedure: ' + str(ssp_cnt))
print(' DDL statements: '+ str(DDL_CNT))
print(' Metadata commands: '+ str(metadata_cnt))
print(' Configurations functions: '+ str(Configuration_cnt))
print(' Cursor functions: '+ str(cursor_cnt))
print(' Trigger calls: '+ str(trigger_cnt))
print()
print('--------------------------------------------------------------')
print('')



fp.close()
