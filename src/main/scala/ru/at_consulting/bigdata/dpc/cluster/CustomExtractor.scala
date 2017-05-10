package ru.at_consulting.bigdata.dpc.cluster

import java.io.File
import java.util

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.LocalDateTime
import ru.at_consulting.bigdata.dpc.cluster.loader.CustomPathFilter
import ru.at_consulting.bigdata.dpc.cluster.loaders.{LoadTextFile, Loader}

/**
  * Created by NSkovpin on 04.05.2017.
  */
object CustomExtractor {

  def extractSeenFolders(sc: SparkContext, fs:FileSystem, logPath: String): java.util.Set[String] = {
    val array = LoadTextFile.loadDataSource(sc, logPath).collect()
    val javaSet: java.util.Set[String] = new java.util.HashSet[String]()
    array.filter(path => path.length > 0).foreach(path => javaSet.add(path))
    javaSet
  }

  def extractNewFolders(sc: SparkContext, fs: FileSystem, dimDpcFolder: String, logPath: String, dateTime: LocalDateTime): (String, java.util.Set[String], java.util.Set[String]) = {
    var seenSet: java.util.Set[String] = new util.HashSet[String]()
    if (fs.exists(new Path(logPath))) {
      seenSet = extractSeenFolders(sc, fs, logPath)
    }
    val customPathFilter = new CustomPathFilter(dateTime, seenSet)

    val days = fs.globStatus(new Path(dimDpcFolder + File.separator + "*" + File.separator + "*" + File.separator + "*" + File.separator + "*"), customPathFilter)
    val newPathsStr = days.map(file => file.getPath).mkString(",")
    val newDatesSet = customPathFilter.getNewDates
    (newPathsStr, newDatesSet, seenSet)
  }

  def getNewLines(sc: SparkContext, fs: FileSystem, loader: Loader, dimDpcFolder: String, logPath: String, dateTime: LocalDateTime): (Boolean, RDD[String], java.util.Set[String]) = {
    val pathAndSet = extractNewFolders(sc, fs, dimDpcFolder, logPath, dateTime)
    if (pathAndSet._1.length > 0 && pathAndSet._2 != null && pathAndSet._2.size() > 0) {

      pathAndSet._2.addAll(pathAndSet._3)
      println(">>>>Loading this files:" + pathAndSet._1)
      val jsonLines = loader.loadDataSource(sc, pathAndSet._1)
      (true, jsonLines, pathAndSet._2)
    } else {
      (false, null, null)
    }

  }

  def getClosedFolders(fs: FileSystem, closedValue: String, dimPath: String): Array[FileStatus] = {
    val folders = fs.globStatus(new Path(dimPath + File.separator + closedValue + File.separator + "*"), new PathFilter() {
      override def accept(path: Path): Boolean = {
        if (path != null) {
          if (path.getName.startsWith("_")) {
            return false
          }
          print(">>>>Proper path to copy:"+ path)
          return true
        }
        false
      }
    })
    folders
  }


}
