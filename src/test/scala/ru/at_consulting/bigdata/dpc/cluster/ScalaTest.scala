package ru.at_consulting.bigdata.dpc.cluster

import java.nio.file.Paths
import java.util

import org.apache.commons.io.FileUtils
import org.scalatest.Matchers
import ru.at_consulting.bigdata.dpc.cluster.utils.SparkTestUtils

/**
  * Created by NSkovpin on 04.05.2017.
  */
class ScalaTest extends SparkTestUtils with Matchers {

  sparkTest("Filter test") {
    println("Test 1")
    val outputPath = Paths.get("src/test/resources/lol")

    FileUtils.deleteDirectory(outputPath.toFile)

    val javaSet:java.util.Set[String] = new util.HashSet()
    javaSet.add("lol1")
    javaSet.add("lol2")
    javaSet.add("lol3")

    val sq = ClusterExecutor.convertToScalaSet(javaSet)
    println(sq)

    val rdd = sc.parallelize(Seq(("k1", "v2"), ("k2", "v1"), ("k3", "v3")))
    rdd.saveAsTextFile(outputPath.toString)

  }

}
