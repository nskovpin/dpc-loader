package ru.at_consulting.bigdata.dpc.cluster

import java.nio.file.Paths

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.scalatest.Matchers
import ru.at_consulting.bigdata.dpc.cluster.loaders.LoadSequenceFile
import ru.at_consulting.bigdata.dpc.cluster.system.ClusterProperties
import ru.at_consulting.bigdata.dpc.cluster.utils.SparkTestUtils

/**
  * Created by NSkovpin on 07.03.2017.
  * If scala can't find some Lombok methods,
  * then you should set scala compile after java (idea > settings > scala compiler > compiler order)
  */
class ExecuteTask extends SparkTestUtils with Matchers {

  val jsonPath: String = Paths.get("src/test/resources/scala/executor/tech_dpc_bgd_ms/dpc/20150101/data1.sf").toString
  val outputPath: String = Paths.get("src/test/resources/scala/executor/out").toString
  val result = "src/test/resources/scala/external/out/result"


  sparkTest("ExternalDimTest") {
    println("Test 1")

    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val args: Array[String] = Array(
      "PROJECT_NAME=dpc_loader",
      "HDFS_JSON_PATH="+jsonPath,
      "HDFS_OUTPUT_DIR="+outputPath,
      "TIME_KEY=20150101"
    )
    val clusterProperties = new ClusterProperties(args)

    val timeKey = clusterProperties.getTimeKey.toString(ClusterProperties.TIME_KEY_PATTERN)

    ClusterExecutor.execute(sc, fs, LoadSequenceFile,
      clusterProperties.getHdfsJsonPath,
      clusterProperties.getHdfsOutputDir,
      timeKey, test = true)


  }

}
