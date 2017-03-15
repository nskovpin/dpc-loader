package ru.at_consulting.bigdata.dpc.cluster

import java.nio.file.Paths

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.scalatest.Matchers
import ru.at_consulting.bigdata.dpc.cluster.groups.GroupTraitFactory
import ru.at_consulting.bigdata.dpc.cluster.loaders.LoadTextFile
import ru.at_consulting.bigdata.dpc.cluster.utils.SparkTestUtils
import ru.at_consulting.bigdata.dpc.dim.{DimEntity, MarketingProductDim, ProductDim, RegionDim}

/**
  * Created by NSkovpin on 07.03.2017.
  * If scala can't find some Lombok methods,
  * then you should set scala compile after java (idea > settings > scala compiler > compiler order)
  */
class GroupRegionDimTest extends SparkTestUtils with Matchers {

  val newRegionDimPath: String = Paths.get("src/test/resources/scala/in/new").toString
  val historyRegionDimPath: String = Paths.get("src/test/resources/scala/in/history").toString
  val resultPath = "src/test/resources/scala/out/region/result"


  sparkTest("RegionDimTest") {
    println("Test 1")
    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    val newRdd = ClusterExecutor.loadAggregate(sc, fs, LoadTextFile, newRegionDimPath, classOf[RegionDim])
    assert(newRdd.count() > 0)
    val collectionNew = newRdd.collect()

    val historyRdd = ClusterExecutor.loadAggregate(sc, fs, LoadTextFile, historyRegionDimPath, classOf[RegionDim])
    assert(historyRdd.count() > 0)
    val collectionHist = historyRdd.collect()

    val result = ClusterExecutor.executeGroups(newRdd, historyRdd, sc, classOf[RegionDim])
    assert(result.count() > 0)

    val result1 = result.filter(x => x._1.equals(DimEntity.EXPIRATION_DATE_INFINITY)).map(x => x._2)
    val result2 = result.filter(x => !x._1.equals(DimEntity.EXPIRATION_DATE_INFINITY)).map(x => x._2)

    val expected1 = sc.textFile(resultPath + "1")
    val expected2 = sc.textFile(resultPath + "2")
    writeToFile("src/test/resources/scala/region1", result1.collect().mkString("\n"))
    writeToFile("src/test/resources/scala/region2", result2.collect().mkString("\n"))

    result1.collect().sortWith((a, b) => a.compareTo(b) >= 0) should be(expected1.collect().sortWith((a, b) => a.compare(b) >= 0))
    result2.collect().sortWith((a, b) => a.compareTo(b) >= 0) should be(expected2.collect().sortWith((a, b) => a.compare(b) >= 0))
  }

}
