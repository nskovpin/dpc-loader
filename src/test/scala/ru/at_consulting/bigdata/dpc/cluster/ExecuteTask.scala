package ru.at_consulting.bigdata.dpc.cluster

import java.nio.file.{Files, Paths}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.rdd.RDD
import org.scalatest.Matchers
import ru.at_consulting.bigdata.dpc.cluster.loader.ParserJson
import ru.at_consulting.bigdata.dpc.cluster.loaders.{LoadSequenceFile, LoadTextFile}
import ru.at_consulting.bigdata.dpc.cluster.system.ClusterProperties
import ru.at_consulting.bigdata.dpc.cluster.utils.SparkTestUtils
import ru.at_consulting.bigdata.dpc.dim.{DimEntity, ProductDim}

/**
  * Created by NSkovpin on 07.03.2017.
  * If scala can't find some Lombok methods,
  * then you should set scala compile after java (idea > settings > scala compiler > compiler order)
  */
class ExecuteTask extends SparkTestUtils with Matchers {

  sparkTest("Analitic test") {
    println("Test 2")

    val resultActual = "src/test/resources/scala/analitic/out/actual"
    emptyFile(resultActual + "/product/29991231")
    emptyFile(resultActual + "/webEntity/29991231")
    emptyFile(resultActual + "/marketingProduct/29991231")
    emptyFile(resultActual + "/productMap/29991231")
    emptyFile(resultActual + "/externalRegionMapping/29991231")
    emptyFile(resultActual + "/productRegionLink/29991231")
    emptyFile(resultActual + "/region/29991231")

    var date = ""
    val last = 6
    for (a <- 4 to last) {
      date = "2017031" + a
      val expectedPath = "src/test/resources/scala/analitic/tech_dpc_bgd_ms/dpc/dpc"
      val expectedPathRes = "src/test/resources/scala/analitic/tech_dpc_bgd_ms/dpc/"

      val jsonPath: String = Paths.get(expectedPath + date + ".csv").toString
      val outputPath: String = Paths.get("src/test/resources/scala/analitic/out/actual").toString



      val conf = new Configuration()
      val fs = FileSystem.get(conf)
      val args: Array[String] = Array(
        "PROJECT_NAME=dpc_loader",
        "HDFS_JSON_PATH=" + jsonPath,
        "HDFS_OUTPUT_DIR=" + outputPath,
        "TIME_KEY=" + date
      )
      val clusterProperties = new ClusterProperties(args)

      val timeKey = clusterProperties.getTimeKey

      val result = ClusterExecutor.execute(sc, fs, LoadTextFile,
        clusterProperties.getHdfsJsonPath,
        clusterProperties.getHdfsOutputDir,
        timeKey, test = true)

      val resultProduct1 = result._1.filter(x => x._1.equals(DimEntity.EXPIRATION_DATE_INFINITY)).map(x => x._2)
      val resultProduct2 = result._1.filter(x => !x._1.equals(DimEntity.EXPIRATION_DATE_INFINITY)).map(x => x._2)
      val resultWeb1 = result._6.filter(x => x._1.equals(DimEntity.EXPIRATION_DATE_INFINITY)).map(x => x._2)
      val resultWeb2 = result._6.filter(x => !x._1.equals(DimEntity.EXPIRATION_DATE_INFINITY)).map(x => x._2)
      val resultMarketing1 = result._3.filter(x => x._1.equals(DimEntity.EXPIRATION_DATE_INFINITY)).map(x => x._2)
      val resultMarketing2 = result._3.filter(x => !x._1.equals(DimEntity.EXPIRATION_DATE_INFINITY)).map(x => x._2)
      val resultProductMap1 = result._7.filter(x => x._1.equals(DimEntity.EXPIRATION_DATE_INFINITY)).map(x => x._2)
      val resultProductMap2 = result._7.filter(x => !x._1.equals(DimEntity.EXPIRATION_DATE_INFINITY)).map(x => x._2)
      val resultExternal1 = result._2.filter(x => x._1.equals(DimEntity.EXPIRATION_DATE_INFINITY)).map(x => x._2)
      val resultExternal2 = result._2.filter(x => !x._1.equals(DimEntity.EXPIRATION_DATE_INFINITY)).map(x => x._2)
      val resultLink1 = result._4.filter(x => x._1.equals(DimEntity.EXPIRATION_DATE_INFINITY)).map(x => x._2)
      val resultLink2 = result._4.filter(x => !x._1.equals(DimEntity.EXPIRATION_DATE_INFINITY)).map(x => x._2)
      val resultRegion1 = result._5.filter(x => x._1.equals(DimEntity.EXPIRATION_DATE_INFINITY)).map(x => x._2)
      val resultRegion2 = result._5.filter(x => !x._1.equals(DimEntity.EXPIRATION_DATE_INFINITY)).map(x => x._2)

      writeToFile(resultActual + "/product/29991231", resultProduct1.collect().mkString("\n"))
      writeToFile(resultActual + "/product/" + date, resultProduct2.collect().mkString("\n"))
      writeToFile(resultActual + "/webEntity/29991231", resultWeb1.collect().mkString("\n"))
      writeToFile(resultActual + "/webEntity/" + date, resultWeb2.collect().mkString("\n"))
      writeToFile(resultActual + "/marketingProduct/29991231", resultMarketing1.collect().mkString("\n"))
      writeToFile(resultActual + "/marketingProduct/" + date, resultMarketing2.collect().mkString("\n"))
      writeToFile(resultActual + "/productMap/29991231", resultProductMap1.collect().mkString("\n"))
      writeToFile(resultActual + "/productMap/" + date, resultProductMap2.collect().mkString("\n"))
      writeToFile(resultActual + "/externalRegionMapping/29991231", resultExternal1.collect().mkString("\n"))
      writeToFile(resultActual + "/externalRegionMapping/" + date, resultExternal2.collect().mkString("\n"))
      writeToFile(resultActual + "/productRegionLink/29991231", resultLink1.collect().mkString("\n"))
      writeToFile(resultActual + "/productRegionLink/" + date, resultLink2.collect().mkString("\n"))
      writeToFile(resultActual + "/region/29991231", resultRegion1.collect().mkString("\n"))
      writeToFile(resultActual + "/region/" + date, resultRegion2.collect().mkString("\n"))

      val expectedProduct = sc.textFile(expectedPathRes + "digit/" + "digital.dim_product_"+date+".csv")
      val expectedWeb = sc.textFile(expectedPathRes + "digit/" + "digital.dim_web_entity_"+date+".csv")
      val expectedMarketing = sc.textFile(expectedPathRes + "digit/" + "digital.dim_marketing_product_"+date+".csv")
      val expectedProductMap = sc.textFile(expectedPathRes + "digit/" + "digital.dim_product_map_"+date+".csv")
      val expectedExternal = sc.textFile(expectedPathRes + "digit/" + "digital.dim_external_region_mapping_"+date+".csv")
      val expectedLink = sc.textFile(expectedPathRes + "digit/" + "digital.dim_product_region_link_"+date+".csv")
      val expectedRegion = sc.textFile(expectedPathRes + "digit/" + "digital.dim_region_"+date+".csv")

      resultProduct2.collect().sortWith((a, b) => a.compareTo(b) >= 0) should be(expectedProduct.collect().sortWith((a, b) => a.compare(b) >= 0))
      resultWeb2.collect().sortWith((a, b) => a.compareTo(b) >= 0) should be(expectedWeb.collect().sortWith((a, b) => a.compare(b) >= 0))
      resultMarketing2.collect().sortWith((a, b) => a.compareTo(b) >= 0) should be(expectedMarketing.collect().sortWith((a, b) => a.compare(b) >= 0))
      resultProductMap2.collect().sortWith((a, b) => a.compareTo(b) >= 0) should be(expectedProductMap.collect().sortWith((a, b) => a.compare(b) >= 0))
      resultExternal2.collect().sortWith((a, b) => a.compareTo(b) >= 0) should be(expectedExternal.collect().sortWith((a, b) => a.compare(b) >= 0))
      resultLink2.collect().sortWith((a, b) => a.compareTo(b) >= 0) should be(expectedLink.collect().sortWith((a, b) => a.compare(b) >= 0))
      resultRegion2.collect().sortWith((a, b) => a.compareTo(b) >= 0) should be(expectedRegion.collect().sortWith((a, b) => a.compare(b) >= 0))


      if(last == a){
        date = "29991231"
        val expectedProductLast = sc.textFile(expectedPathRes + "digit/" + "digital.dim_product_"+date+".csv")
        val expectedWebLast = sc.textFile(expectedPathRes + "digit/" + "digital.dim_web_entity_"+date+".csv")
        val expectedMarketingLast = sc.textFile(expectedPathRes + "digit/" + "digital.dim_marketing_product_"+date+".csv")
        val expectedProductMapLast = sc.textFile(expectedPathRes + "digit/" + "digital.dim_product_map_"+date+".csv")
        val expectedExternalLast = sc.textFile(expectedPathRes + "digit/" + "digital.dim_external_region_mapping_"+date+".csv")
        val expectedLinkLast = sc.textFile(expectedPathRes + "digit/" + "digital.dim_product_region_link_"+date+".csv")
        val expectedRegionLast = sc.textFile(expectedPathRes + "digit/" + "digital.dim_region_"+date+".csv")

        resultProduct1.collect().sortWith((a, b) => a.compareTo(b) >= 0) should be(expectedProductLast.collect().sortWith((a, b) => a.compare(b) >= 0))
        resultWeb1.collect().sortWith((a, b) => a.compareTo(b) >= 0) should be(expectedWebLast.collect().sortWith((a, b) => a.compare(b) >= 0))
        resultMarketing1.collect().sortWith((a, b) => a.compareTo(b) >= 0) should be(expectedMarketingLast.collect().sortWith((a, b) => a.compare(b) >= 0))
        resultProductMap1.collect().sortWith((a, b) => a.compareTo(b) >= 0) should be(expectedProductMapLast.collect().sortWith((a, b) => a.compare(b) >= 0))
        resultExternal1.collect().sortWith((a, b) => a.compareTo(b) >= 0) should be(expectedExternalLast.collect().sortWith((a, b) => a.compare(b) >= 0))
        resultLink1.collect().sortWith((a, b) => a.compareTo(b) >= 0) should be(expectedLinkLast.collect().sortWith((a, b) => a.compare(b) >= 0))
        resultRegion1.collect().sortWith((a, b) => a.compareTo(b) >= 0) should be(expectedRegionLast.collect().sortWith((a, b) => a.compare(b) >= 0))


      }

    }


  }

}
