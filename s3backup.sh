#!/bin/sh

## Configuration
S3BACKUP_JAR=/home/schandran/checkout/site/s3backup/target/s3backup-1.2-jar-with-dependencies.jar

S3ACCESSKEY=XXX
S3SECRETKET=XXX
S3BUCKET=XXX

## Configuration ended. Do not modify.

if [ $# -ne 1 ]; then
    echo Give directory to backup as parameter.
    exit 1
fi

BACKUPDIR=$1

if [ ! -d "$BACKUPDIR" ]; then
    echo $BACKUPDIR is not directory.
fi

cd "$BACKUPDIR"

# Go one step above backup dir:
cd ..

DIRNAME=`basename $BACKUPDIR`
OUTFILE=$DIRNAME`date -I`.tbz2

if tar -cjf $OUTFILE $DIRNAME
then
    ## Call the java program to push to s3
    echo Pushing to s3: $OUTFILE
    ### TODO: CHANGE THE PARAMETER VALUES
    if $S3BACKUP_JAR -jar -a $S3ACCESSKEY -s $S3SECRETKET -b $S3BUCKET -f $OUTFILE ;then
        rm $OUTFILE
    else
        echo Push failed for some reason.
        echo Removing temporary file: $OUTFILE
        rm $OUTFILE
    fi
fi

