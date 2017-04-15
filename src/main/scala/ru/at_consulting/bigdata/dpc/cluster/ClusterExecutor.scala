package ru.at_consulting.bigdata.dpc.cluster

import java.io.File

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import ru.at_consulting.bigdata.dpc.cluster.groups.GroupTraitFactory
import ru.at_consulting.bigdata.dpc.cluster.loader.ParserJson
import ru.at_consulting.bigdata.dpc.cluster.loaders.Loader
import ru.at_consulting.bigdata.dpc.cluster.system.ClusterProperties
import ru.at_consulting.bigdata.dpc.dim._
import ru.at_consulting.bigdata.dpc.json.DpcRoot

/**
  * Created by NSkovpin on 10.03.2017.
  */
object ClusterExecutor {

  def execute(sc: SparkContext, hdfs: FileSystem, loader: Loader, jsonPath: String, aggregatePath: String,
              timeKeyDateTime: DateTime, test: Boolean = false, optional: Boolean = false, openDate: String = "29991231"): (RDD[(String, String)], RDD[(String, String)], RDD[(String, String)], RDD[(String, String)], RDD[(String, String)], RDD[(String, String)], RDD[(String, String)]) = {
    var jsonPathNew = ""
    val timeKey = timeKeyDateTime.toString(ClusterProperties.TIME_KEY_PATTERN)
    if (!test) {
      jsonPathNew = jsonPath + File.separator +
        timeKeyDateTime.toString("YYYY") + File.separator +
        timeKeyDateTime.toString("MM") + File.separator +
        timeKeyDateTime.toString("dd")
    } else {
      jsonPathNew = jsonPath
    }

    val jsonLines = loader.loadDataSource(sc, jsonPathNew)

    val parsedRDDS = jsonLines.map(jsonObject => {
      val mapper = new ObjectMapper().enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
      val dpcRoot = mapper.readValue(jsonObject, classOf[DpcRoot])
      val parserJson = new ParserJson
      val parsedDimsHolder = parserJson.parseDcpRoot(dpcRoot)

      val productDim = parsedDimsHolder.getProductDim
      val externalRegionDimList = parsedDimsHolder.getExternalRegionMappingDimList
      val marketingProductDim = parsedDimsHolder.getMarketingProductDim
      val productRegionLinkDimList = parsedDimsHolder.getProductRegionLinkDimList
      val regionDimList = parsedDimsHolder.getRegionDimList
      val webEntityDimList = parsedDimsHolder.getWebEntityDimList
      val productMapDimList = parsedDimsHolder.getProductMapDimList

      (productDim, externalRegionDimList, marketingProductDim, productRegionLinkDimList, regionDimList, webEntityDimList, productMapDimList)
    }).persist()

    var newProductRdd = parsedRDDS.filter(x => x != null).filter(x => x._1 != null).map(x => x._1.asInstanceOf[DimEntity])
    var newExternalRdd = parsedRDDS.filter(x => x != null).filter(x => x._2 != null).map(x => ("", x._2)).flatMapValues(x => convertToList(x)).map(x => x._2)
    var newMarketingRdd = parsedRDDS.filter(x => x != null).filter(x => x._3 != null).map(x => x._3.asInstanceOf[DimEntity])
    var newProductRegionLinkRdd = parsedRDDS.filter(x => x != null).filter(x => x._4 != null).map(x => ("", x._4)).flatMapValues(x => convertToList(x)).map(x => x._2)
    var newRegionRdd = parsedRDDS.filter(x => x != null).filter(x => x._5 != null).map(x => ("", x._5)).flatMapValues(x => convertToList(x)).map(x => x._2)
    var newWebEntityRdd = parsedRDDS.filter(x => x != null).filter(x => x._6 != null).map(x => ("", x._6)).flatMapValues(x => convertToList(x)).map(x => x._2)
    var newProductMapRdd = parsedRDDS.filter(x => x != null).filter(x => x._7 != null).map(x => ("", x._7)).flatMapValues(x => convertToList(x)).map(x => x._2)

    var product: RDD[(String, String)] = null
    var external: RDD[(String, String)] = null
    var market: RDD[(String, String)] = null
    var link: RDD[(String, String)] = null
    var region: RDD[(String, String)] = null
    var web: RDD[(String, String)] = null
    var productMap: RDD[(String, String)] = null

    var historyProductRdd = loadAggregate(sc, hdfs, loader, aggregatePath, classOf[ProductDim], openDate)
    var historyExternalRdd = loadAggregate(sc, hdfs, loader, aggregatePath, classOf[ExternalRegionMappingDim], openDate)
    var historyMarketingRdd = loadAggregate(sc, hdfs, loader, aggregatePath, classOf[MarketingProductDim], openDate)
    var historyProductRegionLinkRdd = loadAggregate(sc, hdfs, loader, aggregatePath, classOf[ProductRegionLinkDim], openDate)
    var historyRegionRdd = loadAggregate(sc, hdfs, loader, aggregatePath, classOf[RegionDim], openDate)
    var historyWebEntityRdd = loadAggregate(sc, hdfs, loader, aggregatePath, classOf[WebEntityDim], openDate)
    var historyProductMapRdd = loadAggregate(sc, hdfs, loader, aggregatePath, classOf[ProductMapDim], openDate)

    product = executeGroups(newProductRdd, historyProductRdd, sc, classOf[ProductDim])
    external = executeGroups(newExternalRdd, historyExternalRdd, sc, classOf[ExternalRegionMappingDim])
    market = executeGroups(newMarketingRdd, historyMarketingRdd, sc, classOf[MarketingProductDim])
    link = executeGroups(newProductRegionLinkRdd, historyProductRegionLinkRdd, sc, classOf[ProductRegionLinkDim])
    region = executeGroups(newRegionRdd, historyRegionRdd, sc, classOf[RegionDim])
    web = executeGroups(newWebEntityRdd, historyWebEntityRdd, sc, classOf[WebEntityDim])
    productMap = executeGroups(newProductMapRdd, historyProductMapRdd, sc, classOf[ProductMapDim])

    if (!test) {
      if(product != null){
        saveAggregate(product, hdfs, aggregatePath, classOf[ProductDim], timeKey, openDate)
      }
      if(external != null){
        saveAggregate(external, hdfs, aggregatePath, classOf[ExternalRegionMappingDim], timeKey, openDate)
      }
      if(market != null){
        saveAggregate(market, hdfs, aggregatePath, classOf[MarketingProductDim], timeKey, openDate)
      }
      if(link != null){
        saveAggregate(link, hdfs, aggregatePath, classOf[ProductRegionLinkDim], timeKey, openDate)
      }
      if(region != null){
        saveAggregate(region, hdfs, aggregatePath, classOf[RegionDim], timeKey, openDate)
      }
      if(web != null){
        saveAggregate(web, hdfs, aggregatePath, classOf[WebEntityDim], timeKey, openDate)
      }
      if(productMap != null){
        saveAggregate(productMap, hdfs, aggregatePath, classOf[ProductMapDim], timeKey, openDate)
      }
      (null, null, null, null, null, null,null)
    }else{
      (product, external, market, link, region, web, productMap)
    }
  }

  def executeGroups(newRdd: RDD[DimEntity], historyRdd: RDD[DimEntity], sc: SparkContext, clazz: Class[_ <: DimEntity]): RDD[(String, String)] = {
    var groupTrait = GroupTraitFactory.createGroupTrait(clazz)
    groupTrait.groupRdds(newRdd, historyRdd, sc, clazz)
  }

  def loadNew(sc: SparkContext, loader: Loader, path: String, dimClass: Class[_ <: DimEntity]): RDD[DimEntity] = {
    val pathToNew = path + File.separator + ParserJson.getDimName(dimClass)
    val newRdd = loader.loadDataSource(sc, pathToNew)
    parseDimEntities(newRdd, dimClass)
  }

  def loadAggregate(sc: SparkContext, hdfs: FileSystem, loader: Loader, path: String,
                    dimClass: Class[_ <: DimEntity], openDate: String = "29991231"): RDD[DimEntity] = {
    val pathToAgg = path + File.separator + ParserJson.getDimName(dimClass) + File.separator + openDate
    if (!hdfs.exists(new Path(pathToAgg))) {
      hdfs.mkdirs(new Path(pathToAgg))
    }
    val aggRdd = loader.loadDataSource(sc, pathToAgg)
    parseDimEntities(aggRdd, dimClass)
  }

  def saveAggregate(lines: RDD[(String, String)], hdfs: FileSystem, aggregatePath: String, dimClass: Class[_ <: DimEntity],
                    timeKey: String, openDate: String): Unit = {
    val TEMP = "_temp"
    val pathClosed = aggregatePath + File.separator + ParserJson.getDimName(dimClass) + File.separator + timeKey
    val pathOpened = aggregatePath + File.separator + ParserJson.getDimName(dimClass) + File.separator + openDate
    val pathOpenedTEMP = pathOpened + TEMP
    if (hdfs.exists(new Path(pathClosed))) {
      hdfs.delete(new Path(pathClosed), true)
    }
    if (hdfs.exists(new Path(pathOpenedTEMP))) {
      hdfs.delete(new Path(pathOpenedTEMP), true)
    }

    val closed = lines.filter(keyAndValue => !keyAndValue._1.equals(DimEntity.EXPIRATION_DATE_INFINITY))
      .map(x => (timeKey, x._2))

    if (closed.count() > 0) {
      closed.saveAsSequenceFile(pathClosed)
    }

    lines.filter(keyAndValue => keyAndValue._1.equals(DimEntity.EXPIRATION_DATE_INFINITY))
      .map(x => (openDate, x._2)).saveAsSequenceFile(pathOpenedTEMP)

    if (hdfs.exists(new Path(pathOpened))) {
      hdfs.delete(new Path(pathOpened), true)
    }

    if (hdfs.exists(new Path(pathOpenedTEMP))) {
      hdfs.rename(new Path(pathOpenedTEMP), new Path(pathOpened))
    }


  }

  private def parseDimEntities(lines: RDD[String], dimClass: Class[_ <: DimEntity]): RDD[DimEntity] = {
    lines.filter(row => row.length > 0).map(row => {
      val dim = dimClass.newInstance()
      dim.fillObject(row)
      dim
    })
  }

  private def convertToList(javaList: java.util.List[_ <: DimEntity]): List[_ <: DimEntity] = {
    var scalaExternalList: List[_ <: DimEntity] = List()
    val iterator = javaList.iterator()
    while (iterator.hasNext) {
      scalaExternalList = iterator.next() :: scalaExternalList
    }
    scalaExternalList
  }

}
