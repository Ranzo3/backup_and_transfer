#!/bin/bash
#
#
# Copyright (c) 2020 by Delphix. All rights reserved.
#
# Original Author: Tad Martin
# Additional Authors: Ranzo Taylor
#
# This script will run an incremental backup (normally level 0 or 1) of an RDS
# database based on the parameters that are provided or defined below.
# If the backup should be run with a least-privileged model, an example
# of creating a user with the needed parameters is below:
#
## USER CREATION EXAMPLE ###
# create user dlpxbackup identified by dlpxbackup;
# grant connect to dlpxbackup;
# grant read,write on directory DB_BACKUP to dlpxbackup;
# grant read on directory BDUMP to dlpxbackup;
# grant execute on rdsadmin.rds_file_util to dlpxbackup;
# grant execute on RDSADMIN.RDSADMIN_RMAN_UTIL to dlpxbackup;
# grant execute on rdsadmin.rdsadmin_s3_tasks to dlpxbackup;
# grant execute on utl_file to dlpxbackup;
## END USER CREATION EXAMPLE ###
#
# Please note:
# DATA_PUMP_DIR above should match what BDIR is below
#
BDIR=DB_BACKUP
LEV=1
ODIR=/var/tmp
PURGE=0
TSTAMP=`date +%Y%m%d%H%M%S`
USER=dlpxbackup
PASS=dlpxbackup
BNAME=delphix-backup-bucket
#DNAME=MYSOURCE  #Enables possible use case of same bucket for multiple databases
SID=MYSOURCE
COMPRESS=TRUE
RDIR=/home/oracle/fusemount/mysource-backup-bucket
SSEC=2
SRCSCRIPT=~oracle/.profile.18

show_usage() {
        echo
        echo "${0} [ -b <s3 bucket> ] [ -c ] [ -d <db dir> ] [ -e <profile script> ] [ -l 0|1 ] [ -o <dir> ] [ -p <# of days> ] [ -r <s3fs dir> ] [ -s <RDS SID> ] [ -u <user> ] [ -w <password> ]";
        echo
        echo "Where: "
        echo "  -b <s3 bucket> is the S3 bucket where you want to transfer the backup files when complete"
        echo "  -c indicates compression should be used for the RMAN backup"
        echo "  -d <db dir> is the RDS directory where RMAN creates files"
        echo "  -e is the profile script which helps to find the right tnsnames"
        echo "  -l 0|1 indicates what incremental level should be used (default: ${LEV})"
        echo "  -o <dir> defines the directory used for the script output files"
        echo "  -p Number of days to keep when purging the database backup directory and s3fs directory prior to running RMAN."
        echo "     Set to 0 to disable purging."
        echo "  -r <s3fs dir> sets the directory where the s3 bucket has been mounted on this host"
        echo "  -s <sid> indicates the TNS entry or connection string for the RDS source instance"
        echo "  -u <user> is the RDS instance user leveraged for all database operations"
        echo "  -w <password> is the password for the RDS user.  If not defined, you will be prompted"
        echo "  -h will display this message"
        echo
        exit 1
}

while getopts b:cd:e:hl:o:p:r:s:u:w: opt
do
        case ${opt} in
                b)
                        BNAME=${OPTARG}
                        ;;
                c)
                        COMPRESS=TRUE
                        ;;
                d)
                        BDIR=${OPTARG}
                        ;;
                e)
                        SRCSCRIPT=${OPTARG}
                        ;;
                l)
                        LEV=${OPTARG}
                        ;;
                o)
                        ODIR=${OPTARG}
                        ;;
                p)
                        PURGE=${OPTARG}
                        ;;
                r)
                        RDIR=${OPTARG}
                        ;;
                u)
                        USER=${OPTARG}
                        ;;
                s)
                        SID="${OPTARG}"
                        ;;
                w)
                        PASS=${OPTARG}
                        ;;
                h|*)
                        show_usage
                        ;;
        esac
done

if [ -f ${SRCSCRIPT} ]
then
        . ${SRCSCRIPT}
fi

LFILE=${ODIR}/${TSTAMP}_rds_output.log
BFILE=${ODIR}/${TSTAMP}_rds_backup.log
#BFILE=/var/tmp/20190430212423_rds_backup.log
TFILE=${ODIR}/${TSTAMP}_s3_transfer.log

logstamp () {
        echo "`date +%Y%m%d-%H:%M:%S` ${@}" >> ${LFILE}
}

#Let's validate several things to make sure we're good to go
if [ ! -d ${ODIR} ]
then
        mkdir -p ${ODIR} >/dev/null 2>&1
        if [ $? -gt 0 ]
        then
                echo "${ODIR} doesn't exist, and I can't create it, exiting!"
                exit 255
        else
                logstamp "Output directory ${ODIR} created"
        fi
fi

if [ ! -d ${RDIR} ]
then
        logstamp "Receiving directory ${RDIR} doesn't exist!"
        logstamp "Exiting"
        exit 4
fi

if [ "${PASS}x" = "x" ]
then
        logstamp "Prompting for password"
fi
while [ "${PASS}x" = "x" ]
do
        read -sp "Password not provided, please enter it: " PASS
done


if [ "${PURGE}" -ne 0 ]
then
        logstamp "Beginning of RDS ${BDIR} file purge"
        sqlplus -s /nolog<<-EOF >/dev/null
        connect ${USER}/${PASS}@"${SID}"
        BEGIN
        for f in (select filename from table(rdsadmin.rds_file_util.listdir(p_directory => '${BDIR}')) where filename like 'BACKUP%'
                  and to_date(mtime,'YYYY-MM-DD HH24:MI:SS') < SYSDATE-${PURGE}
                 )
        loop
                utl_file.fremove('${BDIR}',f.filename);
        end loop;
        end;
        /
	EOF
        logstamp "End RDS ${BDIR} file purge"
        logstamp "Beginning of ${RDIR} purge"
        find ${RDIR} -type f -mtime +${PURGE} -exec rm {} \;
        logstamp "End of ${RDIR} purge"
fi


logstamp "Starting level ${LEV} backup"
sqlplus -s /nolog<<EOF >/dev/null
connect $USER/${PASS}@"${SID}"
set serveroutput on;
set linesize 32767 trimspool on trimout on wrap off termout off;
spool ${BFILE};
exec RDSADMIN.RDSADMIN_RMAN_UTIL.BACKUP_DATABASE_INCREMENTAL(P_OWNER => 'SYS', P_DIRECTORY_NAME => '${BDIR}', P_COMPRESS => ${COMPRESS}, P_OPTIMIZE => TRUE, P_RMAN_TO_DBMS_OUTPUT => TRUE, P_LEVEL => ${LEV}, P_INCLUDE_ARCHIVE_LOGS => TRUE, P_INCLUDE_CONTROLFILE => TRUE);
spool off;
EOF
logstamp "Backup Finished"
logstamp "Begining file analysis"
# Here we'll find the first portion of the filename, based on the standard naming format
# We'll leverage this to transfer only the new files related to this backup
psname=
sname=
ccount=0
xferpre=
for fname in `/bin/grep handle ${BFILE}|/bin/awk -F= '{print $2}'|awk '{print $1}'`
do
        sname=`basename ${fname}|awk -F- 'BEGIN {OFS="-";} {print $1,$2,$3,$4,$5,$6,$7}'`
        if [ "${psname}" != "${sname}" ]
        then
                psname=${sname}
                (( ccount++ ))
        fi
done
# Here we'll make sure that the filenames didn't vary for some reason, and extract the first portion
if [ ${ccount} -gt 1 ]
then
        logstamp "Filenames varied between output handles, which doesn't seem right"
        #logstamp "We'll continue, but only grab the first 5 fields from the name (down to hour)"
        #xferpre=`/bin/echo ${sname}| /bin/awk -F- 'BEGIN {OFS="-";} {print $1,$2,$3,$4,$5}'`
        xferpre=`/bin/echo ${sname}| /bin/awk -F- 'BEGIN {OFS="-";} {print $1,$2,$3}'`
        logstamp "We'll continue, but only grab the first 3 fields from the name (down to month [${xferpre}])"
else
        xferpre=`/bin/echo ${sname}| /bin/awk -F- 'BEGIN {OFS="-";} {print $1,$2,$3,$4,$5,$6,$7}'`
fi
if [ "${xferpre}" = "----" ] || [ ${xferpre} = "------" ]
then
        logstamp "We can't seem to find the file prefix, exiting!"
        exit 2
fi
logstamp "The file prefix for transfer is: ${xferpre}"
logstamp "File analysis complete"
logstamp "Beginning transfer to S3 bucket: ${BNAME}"
taskid=
if [ "${DNAME}x" = "x" ]
then
        taskid=`sqlplus -s /nolog<<-EOF | tail -1
        connect ${USER}/${PASS}@"${SID}"
        set head off echo off feedback off verify off serveroutput on
        SELECT rdsadmin.rdsadmin_s3_tasks.upload_to_s3(
              p_bucket_name    =>  '${BNAME}',
              p_prefix => '${xferpre}',
              p_s3_prefix => '',
              p_directory_name => '${BDIR}')
        as task_id from dual;
	EOF`
else
        taskid=`sqlplus -s /nolog<<-EOF |tail -1
        connect ${USER}/${PASS}@"${SID}"
        set head off echo off feedback off verify off serveroutput on
        SELECT rdsadmin.rdsadmin_s3_tasks.upload_to_s3(
              p_bucket_name    =>  '${BNAME}',
              p_prefix => '${xferpre}',
              p_s3_prefix => '${DNAME}/',
              p_directory_name => '${BDIR}')
        as task_id from dual;
	EOF`
fi

logstamp "Transfer upload started"
logstamp "Task ID: ${taskid}"
retval=1
lcount=0
while [ ${retval} -eq 1 ] && [ ${lcount} -lt 100 ]
do
        if [ ${lcount} -gt 4 ]
        then
                let lsleep=${SSEC}*10
        else
                lsleep=${SSEC}
        fi
        logstamp "Verifying files transferred"
        echo "Verifying files transferred"
        sqlplus -s /nolog<<-EOF >/dev/null
        connect ${USER}/${PASS}@"${SID}"
        set head off verify off feedback off echo off termout off
        spool ${TFILE};
        set serveroutput on
        DECLARE
          p_directory varchar2(256) := 'BDUMP';
          p_filename varchar2(256) := 'dbtask-${taskid}.log';
          v_file utl_file.file_type;
          v_buffer_size pls_integer := 32767;
          v_buffer varchar2(32767);
        begin
            v_file := utl_file.fopen(p_directory, p_filename, 'r', v_buffer_size);
            loop
              begin
                utl_file.get_line(v_file, v_buffer);
                dbms_output.put_line(v_buffer);
              exception when no_data_found then exit;
              end;
            end loop;
            utl_file.fclose(v_file);
        end;
        /
        spool off;
	EOF
        sleep ${lsleep}
        /bin/grep -q "The task finished successfully" ${TFILE}
        retval=$?
        (( lcount++ ))
done

if [ ${retval} -gt 0 ]
then
        logstamp "Unable to verify the files were transferred successfully!"
        logstamp "Review logfile ${TFILE}"
        logstamp "Exiting"
        exit 3
fi

logstamp "Files transferred successfully"
# Here we grab the file names again and verify they're in the S3 bucket that is mounted in RDIR.
# We'll check 10 times, and give up
lcount=0
lfound=1
ftot=0
ffound=0
while [ ${lcount} -lt 10 ] && [ ${lfound} -gt 0 ]
do
        logstamp "Verifying files received"
        lfound=0
        ffound=0
        ftot=0
        for fname in `/bin/grep handle ${BFILE}|/bin/awk -F= '{print $2}'|/bin/awk '{print $1}'`
        do
                (( ftot++ ))
                sname=`basename ${fname}`
                if [ "${DNAME}x" = "x" ]
                then
                  if [ ! -f ${RDIR}/${sname} ]
                  then
                    (( lfound++ ))
                  else
                    (( ffound++ ))
                  fi
                else
                  if [ ! -f ${RDIR}/${DNAME}/${sname} ]
                  then
                    (( lfound++ ))
                  else
                    (( ffound++ ))
                  fi
                fi
        done
        if [ ${lfound} -gt 0 ]
        then
                sleep ${SSEC}
        fi
        (( lcount++ ))
done

if [ ${lfound} -gt 0 ]
then
        logstamp "We only found ${ffound} of ${ftot} in ${RDIR}!"
        logstamp "Unable to continue!"
        logstamp "Exiting"
        exit 5
fi

logstamp "All ${ftot} files transferred successfully"
logstamp "Backup and Transfer is complete"
exit 0

