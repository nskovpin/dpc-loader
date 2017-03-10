package ru.at_consulting.bigdata.dpc.cluster

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import ru.at_consulting.bigdata.dpc.cluster.groups.GroupTraitFactory
import ru.at_consulting.bigdata.dpc.cluster.loader.ParserJson
import ru.at_consulting.bigdata.dpc.cluster.loaders.{LoadTextFile, Loader}
import ru.at_consulting.bigdata.dpc.cluster.system.ClusterProperties
import ru.at_consulting.bigdata.dpc.dim._

/**
  * Created by NSkovpin on 07.03.2017.
  */
object GroupDims {

  def loadDataSources(sc: SparkContext, clusterProperties: ClusterProperties, loader: Loader, clazz: Class[_], historical: Boolean): RDD[String] = {
    var path = ""
    if (historical) {
      path = clusterProperties.getHdfsOutputDir + File.separator + ParserJson.getDimName(clazz)
    } else {
      path = clusterProperties.getHdfsDataDir + File.separator + ParserJson.getDimName(clazz)
    }
    loader.loadDataSource(sc, path)
  }

  def main(args: Array[String]) {

    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val clusterProperties = new ClusterProperties(args)
    val hdfs = FileSystem.get(conf)

    println("sparkContext start")
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    println("sparkContext end")

    println("Properties:")
    println(clusterProperties.toString)

    println("Reading JSON file:" + clusterProperties.getHdfsJsonPath)
    val path = new Path(clusterProperties.getHdfsJsonPath)
    val inputStream = hdfs.open(path)
    val parserJson = new ParserJson()
    parserJson.parseSingleObject(inputStream, conf, clusterProperties.getHdfsOutputDir)
    println("Saving results to:" + clusterProperties.getHdfsDataDir)

    //    val fake = new FakeFtpServer()
    //    fake.setSystemName("name")

    //product dim
    val newProductRdd = loadDataSources(sc, clusterProperties, LoadTextFile, classOf[ProductDim], historical = false)
    val historyProductRdd = loadDataSources(sc, clusterProperties, LoadTextFile, classOf[ProductDim], historical = true)
    var groupTrait = GroupTraitFactory.createGroupTrait(classOf[ProductDim])


    //region dim
    val newRegionRdd = loadDataSources(sc, clusterProperties, LoadTextFile, classOf[RegionDim], historical = false)
    val historyRegionRdd = loadDataSources(sc, clusterProperties, LoadTextFile, classOf[RegionDim], historical = true)
    groupTrait = GroupTraitFactory.createGroupTrait(classOf[RegionDim])

    //externalMapping dim
    val newExternalMappingRdd = loadDataSources(sc, clusterProperties, LoadTextFile, classOf[ExternalRegionMappingDim], historical = false)
    val historyExternalMappingRdd = loadDataSources(sc, clusterProperties, LoadTextFile, classOf[ExternalRegionMappingDim], historical = true)
    groupTrait = GroupTraitFactory.createGroupTrait(classOf[ExternalRegionMappingDim])

    //webEntity dim
    val newWebEntityRdd = loadDataSources(sc, clusterProperties, LoadTextFile, classOf[WebEntityDim], historical = false)
    val historyWebEntityRdd = loadDataSources(sc, clusterProperties, LoadTextFile, classOf[WebEntityDim], historical = true)
    groupTrait = GroupTraitFactory.createGroupTrait(classOf[WebEntityDim])

    //productRegionLink dim
    val newProductRegionLinkRdd = loadDataSources(sc, clusterProperties, LoadTextFile, classOf[WebEntityDim], historical = false)
    val historyProductRegionLinkRdd = loadDataSources(sc, clusterProperties, LoadTextFile, classOf[WebEntityDim], historical = true)


    //    sc.stop()
  }

}
