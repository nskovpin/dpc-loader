package ru.at_consulting.bigdata.dpc.cluster

import java.nio.file.Paths

import org.scalatest.Matchers
import ru.at_consulting.bigdata.dpc.cluster.groups.GroupTraitFactory
import ru.at_consulting.bigdata.dpc.cluster.loaders.LoadTextFile
import ru.at_consulting.bigdata.dpc.cluster.utils.SparkTestUtils
import ru.at_consulting.bigdata.dpc.dim.{MarketingProductDim, RegionDim}

/**
  * Created by NSkovpin on 07.03.2017.
  * If scala can't find some Lombok methods,
  * then you should set scala compile after java (idea > settings > scala compiler > compiler order)
  */
class GroupRegionDimTest extends SparkTestUtils with Matchers {

  System.setProperty("hadoop.home.dir", "C:\\winutil\\")

  val newRegionDimPath: String = Paths.get("src/test/resources/scala/region/in/newRegionDim").toString
  val historyRegionDimPath: String = Paths.get("src/test/resources/scala/region/in/historyRegionDim").toString
  val result = "src/test/resources/scala/region/out/result"


  sparkTest("RegionDimTest") {
    println("Test 1")

    val newRegionDimRdd = LoadTextFile.loadDataSource(sc, newRegionDimPath)
    assert(newRegionDimRdd.count() > 0)
    val historyRegionDimRdd = LoadTextFile.loadDataSource(sc, historyRegionDimPath)
    assert(historyRegionDimRdd.count() > 0)
    val groupTrait = GroupTraitFactory.createGroupTrait(classOf[RegionDim])
    val result1 = groupTrait.group(newRegionDimRdd, historyRegionDimRdd, sc, classOf[RegionDim])
    assert(result1.count() > 0)
    val expected = sc.textFile(result + "1")
    writeToFile("src/test/resources/scala/tesssst3", result1.collect().mkString("\n"))
    result1.collect().sortWith((a, b) => a.compareTo(b) >= 0) should be(expected.collect().sortWith((a, b) => a.compare(b) >= 0))

  }

}
