package ru.at_consulting.bigdata.dpc.cluster

import java.nio.file.Paths

import org.scalatest.Matchers
import ru.at_consulting.bigdata.dpc.cluster.groups.GroupTraitFactory
import ru.at_consulting.bigdata.dpc.cluster.loaders.LoadTextFile
import ru.at_consulting.bigdata.dpc.cluster.utils.SparkTestUtils
import ru.at_consulting.bigdata.dpc.dim.ProductDim

/**
  * Created by NSkovpin on 07.03.2017.
  * If scala can't find some Lombok methods,
  * then you should set scala compile after java (idea > settings > scala compiler > compiler order)
  */
class GroupProductDimTest extends SparkTestUtils with Matchers {

  System.setProperty("hadoop.home.dir", "C:\\winutil\\")

  val newProductDimPath: String = Paths.get("src/test/resources/scala/product/in/newProductDim").toString
  val historyProductDimPath: String = Paths.get("src/test/resources/scala/product/in/historyProductDim").toString
  val result = "src/test/resources/scala/product/out/result"


  sparkTest("ProductDimTest") {
    println("Test 1")

    val newProductDimRdd = LoadTextFile.loadDataSource(sc, newProductDimPath)
    assert(newProductDimRdd.count() > 0)
    val historyProductDimRdd = LoadTextFile.loadDataSource(sc, historyProductDimPath)
    assert(historyProductDimRdd.count() > 0)
    val groupTrait = GroupTraitFactory.createGroupTrait(classOf[ProductDim])
    val result1 = groupTrait.group(newProductDimRdd, historyProductDimRdd, sc, classOf[ProductDim])
    assert(result1.count() > 0)
    val expected = sc.textFile(result + "1")
    writeToFile("src/test/resources/scala/tesssst", result1.collect().mkString("\n"))
    result1.collect().sortWith((a, b) => a.compareTo(b) >= 0) should be(expected.collect().sortWith((a, b) => a.compare(b) >= 0))

  }

  sparkTest("ProductDimTest: new is empty") {
    println("Test 2")
  }

  sparkTest("ProductDimTest: history is empty") {
    println("Test 3")
  }

}
