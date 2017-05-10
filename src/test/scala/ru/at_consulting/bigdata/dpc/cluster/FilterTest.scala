package ru.at_consulting.bigdata.dpc.cluster

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.scalatest.Matchers
import ru.at_consulting.bigdata.dpc.cluster.loaders.LoadTextFile
import ru.at_consulting.bigdata.dpc.cluster.system.ClusterProperties
import ru.at_consulting.bigdata.dpc.cluster.utils.SparkTestUtils

/**
  * Created by NSkovpin on 04.05.2017.
  */
class FilterTest extends SparkTestUtils with Matchers {

  sparkTest("Filter test") {
    println("Test 1")

    val date = 20170103
    val jsonPath = Paths.get("src/test/resources/filter/tech_dpc_bdg_ms/dpc").toString
    val outputPath = Paths.get("src/test/resources/filter/out")

    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val args: Array[String] = Array(
      "PROJECT_NAME=dpc_loader",
      "HDFS_JSON_PATH=" + jsonPath,
      "HDFS_OUTPUT_DIR=" + outputPath,
      "TIME_KEY=" + date
    )
    val clusterProperties = new ClusterProperties(args)

    FileUtils.deleteDirectory(outputPath.toFile)


    val timeKey = clusterProperties.getTimeKey
    val loggerPath = clusterProperties.getHdfsLoggerPath

    val files1 = fs.globStatus(new Path("src/test/resources/filter/out/*"))
    val files2 = fs.globStatus(new Path("src/test/resources/filter/tech_dpc_bdg_ms/dpc" + File.separator + "*" + File.separator + "*/*/*"))

    val result = ClusterExecutor.execute(sc, fs, LoadTextFile,
      clusterProperties.getHdfsJsonPath,
      clusterProperties.getHdfsOutputDir,
      loggerPath,
      timeKey)


  }

}
