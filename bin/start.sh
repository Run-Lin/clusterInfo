#!/usr/bin/env bash
APP_HOME="$(cd "`dirname "$0"`/.."; pwd)"
DATE=`date --date='-1 day' +%Y-%m-%d`
while :
do
    case "$1" in
      -h | --help)
	 echo
 	 echo "Usage: $0 [option...] {mr|spark} {begin_date} {end_date}" >&2
         echo "date format: yyyy-MM-dd"
	 echo
         exit 0
          ;;
      *)  # No more options
          break
          ;;
    esac
done
if [ -z "$SPARK_HOME" ]; then
  echo "Error: SPARK_HOME is not set, cannot proceed."
  exit -1
fi
export HADOOP_USER_NAME=bigdata
export _APP_CMD_USAGE="Usage: ./bin/start.sh [mr/spark] [begin_date] [end_date]"
STATIS_TYPE=${1:-mr}
STATIS_BEG_DATE=${2:-$DATE}
STATIS_END_DATE=${3:-$DATE}
MAIN_CLASS="com.spark.job.statis.SparkMrLogStatis"
if [ ! $STATIS_TYPE = "mr" ]
then
  MAIN_CLASS="com.spark.job.statis.SparkEventLogStatis"
fi
cd "$APP_HOME"
$SPARK_HOME/bin/spark-submit \
--queue root.jichupingtaibu-dashujujiagoubu.offline \
--class ${MAIN_CLASS} \
$APP_HOME/build/spark-statis-1.0-SNAPSHOT.jar \
${STATIS_BEG_DATE} \
${STATIS_END_DATE}
