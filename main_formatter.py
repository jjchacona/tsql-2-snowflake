import re
import sys
filepath = ''
#output lines
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
begin_cnt = 0
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
trncnt = 0
SPEC_VAR_CNT = 0




#basic formatting for @, [], etc around objects
def convert_vars(line):
    # fix database objects
    line=line.replace('dbo.','')
    #fix varariable callouts
    if 'SET @' in line.upper():
        line = re.sub("set @", "Set ", line, flags=re.I)
    if 'IF EXISTS(' in line.upper():
        line = "// FLAG -"+ line
    if "@@" in line:
        #SPEC_VAR_CNT = SPEC_VAR_CNT+1
        line = line.replace("@@","/* flagged special variable")
        line = line+ "*/"
    if "DECLARE" in line.upper():
        line ='\n'
    line=line.replace('@','$')
    line=line.replace('[','')
    line=line.replace(']','')
    line=line.replace('#','TMP_TBL_')
    if "CREATE PROCEDURE" in line.upper():
        line =re.sub("Create", "CREATE or REPLACE", line, flags=re.I)
        line = re.sub("\n","()\n",line)
    elif line.strip() == 'AS':
        line = re.sub('AS','\nRETURNS VARCHAR\nLANGUAGE JAVASCRIPT\nEXECUTE AS CALLER\nAS\n',line, flags=re.I )

    elif "NOCOUNT" in line.upper():
        line = ''

    return line

# Convert lines
with open(filepath) as fp:
   o_line = fp.readline()
   i=0
   while o_line:
       if o_line != '':
           new_line = convert_vars(o_line)
           sf_line.append(new_line)
           o_line = fp.readline()
           i+= 1
fp.close()

i=0

for ff in sf_line:
    fff= ff.strip()
    match_first = re.match(r"[a-zA-z]+", fff)

#Checks the first word of each line
    if match_first:
        first_word = match_first.group(0)

        #checks for transations
        if first_word.upper() == 'BEGIN':
            if begin_cnt == 0:
                begin_cnt =+1
                sf_final.append('$$')
                trncnt = 1


        #checks for develeoper comments
        if first_word == '--':
            re.sub('--','/*',fff)
            fff=fff+'*/'
            sf_final.append(fff)
        #Checks for end of a trans
        if first_word.upper() == 'END':
            sf_final.append('\t"});')
            break

        for dml in DML_ARR:
            if dml == first_word.upper():
                if trncnt ==1:
                    sf_final.append('\nvar stmt['+str(trncnt)+'] = snowflake.createStatement({sqlText:" \\\t')
                    sf_final.append("\t"+fff+"\\")
                if trncnt > 1:
                    sf_final.append('\t;"});\nstmt['+str(trncnt-1)+'].execute();')
                    sf_final.append('\nvar stmt['+str(trncnt)+'] = snowflake.createStatement({sqlText:" \\\t')
                trncnt = trncnt+ 1
        if fff != '' and trncnt > 2 and first_word.upper() != 'BEGIN':
            sf_final.append("\t"+fff+"\t\\")
        if first_word.upper() != 'BEGIN' and trncnt == 0:
            sf_final.append(fff)
for ff in sf_final:
    print(ff)

print('Return \'Success\'\n$$')
