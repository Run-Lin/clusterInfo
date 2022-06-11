package com.spark.job.statis

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.regex.{Matcher, Pattern}

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer


object SparkEventLogStatis {

  def main(args: Array[String]) {
    if (args.length < 3) {
      println("Usage: SparkEventLogStatis <startDate> <endDate>  <logpath>")
      System.exit(1)
    }
    val startDateStr = args(0).split("-")
    val endDateStr = args(1).split("-")
    val logpath = args(2)
    if (startDateStr.length != 3 || endDateStr.length != 3 ) {
      println("Usage: SparkEventLogStatis <startDate> <endDate>, date format like: yyyy-MM-dd!")
      System.exit(1)
    }
    val spark = SparkSession.builder().appName("spark-eventlog-statis").getOrCreate()
    val startDate = new SimpleDateFormat("yyyy-MM-dd").parse(args(0))
    val endDate = new SimpleDateFormat("yyyy-MM-dd").parse(args(1))
    val startTime = if(startDate.before(endDate)) startDate else endDate
    val diffMils = Math.abs(endDate.getTime-startDate.getTime)
    val diffDays = diffMils/(1000 * 3600 * 24)
    val starCal = Calendar.getInstance()
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd")
//    val logPaths = new ListBuffer[Path]()
    val _LogPath =  new Path(logpath)
//    logPaths += new Path("hdfs://DClusterNmg/tmp/spark/staging/historylog")
//    logPaths += new Path("hdfs://DClusterNmg2/tmp/spark/staging/historylog")
//    logPaths += new Path("hdfs://DClusterNmg4/tmp/spark/staging/historylog")
//    logPaths += new Path("hdfs://DClusterNmg4/tmp/spark/staging/historylog")
    for(i <- 0 to diffDays.toInt ){
      starCal.setTime(startTime)
      starCal.add(Calendar.DAY_OF_MONTH,i)
      val fs = _LogPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
      val hdfsFiles:Array[FileStatus] = fs.listStatus(_LogPath).filter(f=>{
        val modTime = timeFormat.format(new java.util.Date(f.getModificationTime))
        starCal.setTime(startTime)
        starCal.add(Calendar.DAY_OF_MONTH,i)
        timeFormat.format(starCal.getTime).equals(modTime) && f.getPath.getName.startsWith("application_")
      })
      val filePaths = new ListBuffer[String]
      hdfsFiles.foreach(filePath =>{
        filePaths += filePath.getPath.toString
      })
      import spark.implicits._
      val mrJobs = spark.read.text(filePaths:_*)
      val readBytes = spark.sparkContext.longAccumulator("total write")
      val writeBytes = spark.sparkContext.longAccumulator("total read")
      mrJobs.filter(_.toString().contains("\"Event\":\"SparkListenerStageCompleted\"")).flatMap(_.toString().split("Accumulables")).foreach(line => {
        val readReg = Pattern.compile("(\"internal.metrics.input.bytesRead\")([^\\}]*)")
        val writeReg = Pattern.compile("(\"internal.metrics.output.bytesWritten\")([^\\}]*)")
        val readMatch: Matcher = readReg.matcher(line)
        val writeMatch: Matcher = writeReg.matcher(line)
        while (readMatch.find) {
          val readValues = readMatch.group.split(",")
          if(readValues.size>0){
            for(readValue <- readValues){
              if(readValue.contains("\"Value\":")){
                val values = readValue.split(":")
                readBytes.add(java.lang.Long.valueOf(values(values.size-1)))
              }
            }
          }
        }
        while (writeMatch.find()) {
          val writeValues = writeMatch.group.split(",")
          if(writeValues.size>0){
            for(writeValue <- writeValues){
              if(writeValue.contains("\"Value\":")){
                val values = writeValue.split(":")
                writeBytes.add(java.lang.Long.valueOf(values(values.size-1)))
              }
            }
          }
        }
       }
      )
      println(s"currentDate:${timeFormat.format(starCal.getTime)} readBytes:${readBytes.value} writeBytes:${writeBytes.value} appCount:${filePaths.size}")
    }
  }
}
