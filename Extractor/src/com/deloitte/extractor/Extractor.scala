package com.deloitte.extractor

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.Iterator
import java.io._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable

object Extractor {

  val sc = new SparkContext(new SparkConf().setMaster("spark://localhost.localdomain:7077").setAppName("WeblogExtractor"))

  def main(args: Array[String]): Unit = {

    val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss")

    val textFile = sc.textFile("input.txt").map(line => line.split(" ")).map(line => (line(0), (line(3), line(6)))).groupByKey

    var timeOnPageSec: Long = 0

    var noOfPagesPerSession = 0

    var totalTimeOnSite: Long = 0

    for ((ipAddress, seqData) <- textFile) {

      seqData.sortBy(seqData => seqData._1)

      val iterator: Iterator[(String, String)] = seqData.iterator

      while (iterator.hasNext) {

        val varTuple = iterator.next

        var varDateTime = varTuple._1

        varDateTime = varDateTime.replaceAll("\\[", "")

        noOfPagesPerSession += 1

        val varDate = dateFormat.parse(varDateTime)

        val pageURL = varTuple._2

        if (iterator.hasNext) {

          val varNextTuple = iterator.next()

          var varNextDateTime = varTuple._1

          varNextDateTime = varNextDateTime.replaceAll("\\[", "")

          val varNextDate = dateFormat.parse(varNextDateTime)

          val duration = Math.abs((varNextDate.getTime() - varDate.getTime()) / 1000)

          if (duration > 600 || duration == 0) {

            if (noOfPagesPerSession > 1) {

              timeOnPageSec = timeOnPageSec + totalTimeOnSite / noOfPagesPerSession;

            } else {

              if (duration > 600) {

                timeOnPageSec = timeOnPageSec + 600;

              }

            }

            totalTimeOnSite = 0;

            noOfPagesPerSession = 1;

            System.out.println("Helloduration")

          } else {

            timeOnPageSec = timeOnPageSec + duration

            totalTimeOnSite = totalTimeOnSite + duration

          }

        }
        //writer.write(ipAddress + "," + varDate.toString + "," + pageURL + "," + duration.toString)

      }

    }

  }

}