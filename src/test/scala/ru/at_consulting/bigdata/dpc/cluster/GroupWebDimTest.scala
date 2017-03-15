package ru.at_consulting.bigdata.dpc.cluster

import java.nio.file.Paths

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.scalatest.Matchers
import ru.at_consulting.bigdata.dpc.cluster.loaders.LoadTextFile
import ru.at_consulting.bigdata.dpc.cluster.utils.SparkTestUtils
import ru.at_consulting.bigdata.dpc.dim.{DimEntity, ExternalRegionMappingDim, ProductDim, WebEntityDim}

/**
  * Created by NSkovpin on 07.03.2017.
  * If scala can't find some Lombok methods,
  * then you should set scala compile after java (idea > settings > scala compiler > compiler order)
  */
class GroupWebDimTest extends SparkTestUtils with Matchers {

  System.setProperty("hadoop.home.dir", "C:\\winutil\\")

  val newWebDimPath: String = Paths.get("src/test/resources/scala/in/new").toString
  val historyWebDimPath: String = Paths.get("src/test/resources/scala/in/history").toString
  val resultPath = "src/test/resources/scala/out/web/result"

  sparkTest("WebDimTest") {
    println("Test 1")
    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    val newRdd = ClusterExecutor.loadAggregate(sc, fs, LoadTextFile, newWebDimPath, classOf[WebEntityDim])
    assert(newRdd.count() > 0)
    val collectionNew = newRdd.collect()

    val historyRdd = ClusterExecutor.loadAggregate(sc, fs, LoadTextFile, historyWebDimPath, classOf[WebEntityDim])
    assert(historyRdd.count() > 0)
    val collectionHist = historyRdd.collect()

    val result = ClusterExecutor.executeGroups(newRdd, historyRdd, sc, classOf[WebEntityDim])
    assert(result.count() > 0)

    val result1 = result.filter(x => x._1.equals(DimEntity.EXPIRATION_DATE_INFINITY)).map(x => x._2)
    val result2 = result.filter(x => !x._1.equals(DimEntity.EXPIRATION_DATE_INFINITY)).map(x => x._2)

    val expected1 = sc.textFile(resultPath + "1")
    val expected2 = sc.textFile(resultPath + "2")
    writeToFile("src/test/resources/scala/web1", result1.collect().mkString("\n"))
    writeToFile("src/test/resources/scala/web2", result2.collect().mkString("\n"))

    result1.collect().sortWith((a, b) => a.compareTo(b) >= 0) should be(expected1.collect().sortWith((a, b) => a.compare(b) >= 0))
    result2.collect().sortWith((a, b) => a.compareTo(b) >= 0) should be(expected2.collect().sortWith((a, b) => a.compare(b) >= 0))
  }

}
