package ru.at_consulting.bigdata.dpc.cluster

import java.io.File

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}
import org.joda.time.LocalDateTime
import ru.at_consulting.bigdata.dpc.cluster.groups.GroupTraitFactory
import ru.at_consulting.bigdata.dpc.cluster.loader.ParserJson
import ru.at_consulting.bigdata.dpc.cluster.loaders.{LoadTextFile, Loader}
import ru.at_consulting.bigdata.dpc.cluster.savers.RDDMultipleTextOutputFormat
import ru.at_consulting.bigdata.dpc.cluster.system.ClusterProperties
import ru.at_consulting.bigdata.dpc.dim._
import ru.at_consulting.bigdata.dpc.json.DpcRoot

import scala.collection.JavaConversions._

/**
  * Created by NSkovpin on 10.03.2017.
  */
object ClusterExecutor {

  def execute(sc: SparkContext, hdfs: FileSystem, loader: Loader, jsonPath: String, aggregatePath: String, loggerPath: String,
              timeKeyDateTime: LocalDateTime, test: Boolean = false, optional: Boolean = false, openDate: String = "29991231"): (RDD[(String, String)], RDD[(String, String)], RDD[(String, String)], RDD[(String, String)], RDD[(String, String)], RDD[(String, String)], RDD[(String, String)]) = {
    var jsonPathNew = ""
    val timeKey = timeKeyDateTime.toString(ClusterProperties.TIME_KEY_PATTERN)
    var jsonLines: RDD[String] = null
    var loggerSet: java.util.Set[String] = null
    if (!test) {
      val hasNewAndRddAndLog = CustomExtractor.getNewLines(sc, hdfs, loader, jsonPath, loggerPath, timeKeyDateTime)
      if (hasNewAndRddAndLog._1) {
        jsonLines = hasNewAndRddAndLog._2
        loggerSet = hasNewAndRddAndLog._3
      } else {
        return (null, null, null, null, null, null, null)
      }

    } else {
      jsonLines = loader.loadDataSource(sc, jsonPath)
    }

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
      if (product != null) {
        saveAggregate(product, hdfs, aggregatePath, classOf[ProductDim], timeKey, openDate)
      }
      if (external != null) {
        saveAggregate(external, hdfs, aggregatePath, classOf[ExternalRegionMappingDim], timeKey, openDate)
      }
      if (market != null) {
        saveAggregate(market, hdfs, aggregatePath, classOf[MarketingProductDim], timeKey, openDate)
      }
      if (link != null) {
        saveAggregate(link, hdfs, aggregatePath, classOf[ProductRegionLinkDim], timeKey, openDate)
      }
      if (region != null) {
        saveAggregate(region, hdfs, aggregatePath, classOf[RegionDim], timeKey, openDate)
      }
      if (web != null) {
        saveAggregate(web, hdfs, aggregatePath, classOf[WebEntityDim], timeKey, openDate)
      }
      if (productMap != null) {
        saveAggregate(productMap, hdfs, aggregatePath, classOf[ProductMapDim], timeKey, openDate)
      }

      saveTempLogger(sc, loggerSet, loggerPath, hdfs)

      (null, null, null, null, null, null, null)
    } else {
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
    val aggRdd = LoadTextFile.loadDataSource(sc, pathToAgg)
    parseDimEntities(aggRdd, dimClass)
  }

  def saveAggregate(lines: RDD[(String, String)], hdfs: FileSystem, aggregatePath: String, dimClass: Class[_ <: DimEntity],
                    timeKey: String, openDate: String): Unit = {
    val TEMP = "_temp"
    val CLOSED = "closed"
    val dimPath = aggregatePath + File.separator + ParserJson.getDimName(dimClass)
    val pathClosed = dimPath + File.separator + CLOSED
    val pathOpened = dimPath + File.separator + openDate
    val pathOpenedTEMP = pathOpened + TEMP
    if (hdfs.exists(new Path(pathClosed))) {
      hdfs.delete(new Path(pathClosed), true)
    }
    if (hdfs.exists(new Path(pathOpenedTEMP))) {
      hdfs.delete(new Path(pathOpenedTEMP), true)
    }

    val closed = lines.filter(keyAndValue => !keyAndValue._1.equals(DimEntity.EXPIRATION_DATE_INFINITY))
      .map(x => (x._1, x._2))

    if (closed.count() > 0) {
      closed.partitionBy(new HashPartitioner(10))
        .saveAsHadoopFile(pathClosed, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])

      val foldersToMove = CustomExtractor.getClosedFolders(hdfs, CLOSED, dimPath)
      if (foldersToMove.length > 0) {
        foldersToMove.map(fileStatus => fileStatus.getPath).foreach(closedPath => {

          val closedMonth = closedPath.getName

          val pathDestination = new Path(dimPath + File.separator + closedMonth)
          if (hdfs.exists(pathDestination)) {
            hdfs.delete(pathDestination, true)
          }

          if (hdfs.exists(closedPath)) {
            hdfs.rename(closedPath, pathDestination)
          }

        })
      }

      if (hdfs.exists(new Path(pathClosed))) {
        hdfs.delete(new Path(pathClosed), true)
      }

    }

    lines.filter(keyAndValue => keyAndValue._1.equals(DimEntity.EXPIRATION_DATE_INFINITY))
      .map(x =>  x._2).saveAsTextFile(pathOpenedTEMP)

  }

  def saveTempLogger(sc: SparkContext, logSet: java.util.Set[String], logPath: String, hdfs: FileSystem): Unit = {
    val TEMP = "_temp"
    if (logSet != null && logSet.size() > 0) {

      val tempPath = logPath + TEMP
      if (hdfs.exists(new Path(tempPath))) {
        hdfs.delete(new Path(tempPath), true)
      }


      val rddSet = sc.parallelize(convertToScalaSet(logSet))
      rddSet.coalesce(1, true).saveAsTextFile(tempPath)
    }

  }

  def replaceTempFolder(hdfs: FileSystem, aggregatePath: String, logPath: String, openDate: String): Unit = {
    replaceTempFolder(hdfs, aggregatePath, classOf[ProductDim], openDate)
    replaceTempFolder(hdfs, aggregatePath, classOf[ExternalRegionMappingDim], openDate)
    replaceTempFolder(hdfs, aggregatePath, classOf[MarketingProductDim], openDate)
    replaceTempFolder(hdfs, aggregatePath, classOf[ProductRegionLinkDim], openDate)
    replaceTempFolder(hdfs, aggregatePath, classOf[RegionDim], openDate)
    replaceTempFolder(hdfs, aggregatePath, classOf[WebEntityDim], openDate)
    replaceTempFolder(hdfs, aggregatePath, classOf[ProductMapDim], openDate)
    replaceTempLogger(hdfs, logPath)
  }

  private def replaceTempFolder(hdfs: FileSystem, aggregatePath: String, dimClass: Class[_ <: DimEntity], openDate: String): Unit = {
    val TEMP = "_temp"
    val dimPath = aggregatePath + File.separator + ParserJson.getDimName(dimClass)
    val pathOpened = dimPath + File.separator + openDate
    val pathOpenedTEMP = pathOpened + TEMP

    if (hdfs.exists(new Path(pathOpenedTEMP))) {
      if (hdfs.exists(new Path(pathOpened))) {
        hdfs.delete(new Path(pathOpened), true)
      }

      hdfs.rename(new Path(pathOpenedTEMP), new Path(pathOpened))
    }
  }

  private def replaceTempLogger(hdfs: FileSystem, logPath: String): Unit = {
    val TEMP = "_temp"
    val tempDir = logPath + TEMP
    if (hdfs.exists(new Path(tempDir))) {
      if (hdfs.exists(new Path(logPath))) {
        hdfs.delete(new Path(logPath), true)
      }

      hdfs.rename(new Path(tempDir), new Path(logPath))
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

  def convertToScalaSet(javaSet: java.util.Set[String]): Seq[String] = {
    var sq = Seq.empty[String]
    javaSet.foreach(value => sq :+= value)
    sq
  }

}
