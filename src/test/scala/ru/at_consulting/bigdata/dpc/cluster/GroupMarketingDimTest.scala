package ru.at_consulting.bigdata.dpc.cluster

import java.nio.file.Paths

import org.scalatest.Matchers
import ru.at_consulting.bigdata.dpc.cluster.groups.GroupTraitFactory
import ru.at_consulting.bigdata.dpc.cluster.loaders.LoadTextFile
import ru.at_consulting.bigdata.dpc.cluster.utils.SparkTestUtils
import ru.at_consulting.bigdata.dpc.dim.MarketingProductDim

/**
  * Created by NSkovpin on 07.03.2017.
  * If scala can't find some Lombok methods,
  * then you should set scala compile after java (idea > settings > scala compiler > compiler order)
  */
class GroupMarketingDimTest extends SparkTestUtils with Matchers {

  System.setProperty("hadoop.home.dir", "C:\\winutil\\")

  val newMarketingDimPath: String = Paths.get("src/test/resources/scala/marketing/in/newMarketingDim").toString
  val historyMarketingDimPath: String = Paths.get("src/test/resources/scala/marketing/in/historyMarketingDim").toString
  val result = "src/test/resources/scala/marketing/out/result"


  sparkTest("MarketingDimTest") {
    println("Test 1")

    val newMarketingDimRdd = LoadTextFile.loadDataSource(sc, newMarketingDimPath)
    assert(newMarketingDimRdd.count() > 0)
    val historyMarketingDimRdd = LoadTextFile.loadDataSource(sc, historyMarketingDimPath)
    assert(historyMarketingDimRdd.count() > 0)
    val groupTrait = GroupTraitFactory.createGroupTrait(classOf[MarketingProductDim])
    val result1 = groupTrait.group(newMarketingDimRdd, historyMarketingDimRdd, sc, classOf[MarketingProductDim])
    assert(result1.count() > 0)
    val expected = sc.textFile(result + "1")
    writeToFile("src/test/resources/scala/tesssst2", result1.collect().mkString("\n"))
    result1.collect().sortWith((a, b) => a.compareTo(b) >= 0) should be(expected.collect().sortWith((a, b) => a.compare(b) >= 0))
  }

}
