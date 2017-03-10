package ru.at_consulting.bigdata.dpc.cluster.groups

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import ru.at_consulting.bigdata.dpc.dim.DimEntity
import ru.at_consulting.bigdata.dpc.dim.resolver.DimComparatorFactory

import scala.collection.immutable.TreeMap

/**
  * Created by NSkovpin on 09.03.2017.
  */
object DoubleGroupEasier extends GroupTrait {

  override def groupDim(newRdd: RDD[String], historyRdd: RDD[String],
                        dimClass: Class[_ <: DimEntity], broadcast: Broadcast[DimComparatorFactory]): RDD[String] = {
    val parsedNewRDD = newRdd.map(row => {
      val dim = dimClass.newInstance()
      dim.fillObject(row)
      dim
    }).map(dim => {
      ((dim.getFirstId, dim.getSecondId), dim)
    }).groupByKey().map(keyAndIterable => {
      val key = keyAndIterable._1
      val dim = foundLastDim(keyAndIterable._2)
      (key, dim)
    }).map(tupleAndDim => {

      val firstKey = tupleAndDim._1._1
      (firstKey, tupleAndDim._2)
    })

    val parsedHistoryRDD = historyRdd.map(row => {
      val dim = dimClass.newInstance()
      dim.fillObject(row)
      dim
    }).filter(dim => dim.getExpirationDate != null)
      .filter(dim => dim.getExpirationDate.equals(DimEntity.EXPIRATION_DATE_INFINITY))
      .map(dim => {
        (dim.getFirstId, dim)
      })


    val firstGrouping = parsedNewRDD.union(parsedHistoryRDD).groupByKey()

    val rdd = firstGrouping.map(keyAndIterable => {

      val key = keyAndIterable._1

      val lastDim = foundLastDim(keyAndIterable._2.filter(dim => dim.getExpirationDate == null))

      var historyMap = generateHistoryMap(keyAndIterable._2.filter(dim => dim.getExpirationDate != null))

      val answerList = iterateNewDims(historyMap, keyAndIterable._2.filter(dim => dim.getExpirationDate == null),
        dimClass, lastDim, broadcast)
      (key, answerList)
    }).flatMapValues(x => x).map(tuple => tuple._2)

    rdd
  }

  def iterateNewDims(historyMap: Map[String, (DimEntity, Boolean)], iterableDims: Iterable[DimEntity],
                     dimClass: Class[_ <: DimEntity], lastDim: DimEntity, broadcast: Broadcast[DimComparatorFactory]): List[String] = {
    var answerList = List[String]()
    var historyMapVar = historyMap
    for (newDim <- iterableDims) {
      val secondId = newDim.getSecondId
      if (historyMapVar.contains(secondId)) {
        historyMapVar += (secondId -> null)
      } else {
        newDim.setExpirationDate(DimEntity.EXPIRATION_DATE_INFINITY)
        answerList = newDim.stringify() :: answerList
      }
    }

    historyMapVar.filter(keyAndTuple => keyAndTuple._2 != null && !keyAndTuple._2._2).foreach(keyAndTuple => {
      val historyDim = keyAndTuple._2._1
      historyDim.setExpirationDate(lastDim.getEffectiveDate)
      answerList = historyDim.stringify() :: answerList
    })
    answerList
  }

  def generateHistoryMap(iterableDims: Iterable[DimEntity]): Map[String, (DimEntity, Boolean)] = {
    var historyRegionDimMap = Map[String, (DimEntity, Boolean)]()
    val dimIterator = iterableDims.iterator
    while (dimIterator.hasNext) {
      val dim = dimIterator.next()
      historyRegionDimMap += (dim.getSecondId -> (dim, false))
    }
    historyRegionDimMap
  }

}