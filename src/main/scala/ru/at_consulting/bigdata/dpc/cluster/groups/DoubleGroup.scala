package ru.at_consulting.bigdata.dpc.cluster.groups

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import ru.at_consulting.bigdata.dpc.dim.DimEntity
import ru.at_consulting.bigdata.dpc.dim.resolver.DimComparatorFactory

import scala.collection.immutable.TreeMap

/**
  * Created by NSkovpin on 09.03.2017.
  */
object DoubleGroup extends GroupTrait {

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
      val lastDate = if (lastDim == null) {
        null
      } else {
        lastDim.getEffectiveDate
      }
      val allList = keyAndIterable._2.map(dim => (dim, lastDate))
      (key, allList)
    }).flatMapValues(x => x).map(keyAndTuple => {
      ((keyAndTuple._1, keyAndTuple._2._1.getSecondId), keyAndTuple._2)
    }).groupByKey().map(tupleKeyAndIterable => {
      val key = tupleKeyAndIterable._1
      val iterable = tupleKeyAndIterable._2
      val dimTuple = findNewAndHistoryDims(iterable)
      var answerList = List[String]()

      if (dimTuple._1 != null) {
        // found newDim
        val newDim = dimTuple._1._1
        if (dimTuple._2 != null) {
          // found history
          val resolver = broadcast.value.getComparator(dimClass)
          val pair = resolver.resolve(newDim, dimTuple._2._1)

          if (pair.getLeft != null) {
            // newDim has changes
            answerList = pair.getLeft.stringify() :: answerList

            if (pair.getRight != null) {
              answerList = pair.getRight.stringify() :: answerList
            }
          }
        } else {
          // not found history
          newDim.setExpirationDate(DimEntity.EXPIRATION_DATE_INFINITY)
          answerList = newDim.stringify() :: answerList
        }
      } else {
        //not found newDim
        val historyDim = dimTuple._2._1
        if(dimTuple._2._2 != null){
          historyDim.setExpirationDate(dimTuple._2._2)
        }
        answerList = historyDim.stringify() :: answerList
      }
      (key, answerList)
    }).flatMapValues(x => x).map( x => x._2)
    rdd
    //    val rdd = firstGrouping.map(keyAndIterable => {
    //
    //      val key = keyAndIterable._1
    //
    //      val lastDim = foundLastDim(keyAndIterable._2.filter(dim => dim.getExpirationDate == null))
    //
    //      var historyMap = generateHistoryMap(keyAndIterable._2.filter(dim => dim.getExpirationDate != null))
    //
    //      val answerList = iterateNewDims(historyMap, keyAndIterable._2.filter(dim => dim.getExpirationDate == null),
    //        dimClass, lastDim, broadcast)
    //      (key, answerList)
    //    }).flatMapValues(x => x).map(tuple => tuple._2)
    //
    //    rdd
  }

  def generateNewMap(iterableDims: Iterable[DimEntity]): TreeMap[String, List[DimEntity]] = {
    var dateDimTreeMap = new TreeMap[String, List[DimEntity]]()
    val dimIterator = iterableDims.iterator
    while (dimIterator.hasNext) {
      val dim = dimIterator.next()

      var dimList: List[DimEntity] = dateDimTreeMap.getOrElse(dim.getEffectiveDate, List())
      dimList = dim :: dimList

      dateDimTreeMap += (dim.getEffectiveDate -> dimList)

    }
    dateDimTreeMap
  }

  def iterateNewDims(historyMap: Map[String, (DimEntity, Boolean)], iterableDims: Iterable[DimEntity],
                     dimClass: Class[_ <: DimEntity], lastDim: DimEntity, broadcast: Broadcast[DimComparatorFactory]): List[String] = {
    var answerList = List[String]()
    var historyMapVar = historyMap
    for (newDim <- iterableDims) {
      val secondId = newDim.getSecondId

      if (historyMapVar.contains(secondId)) {
        val historyDim = historyMapVar(secondId)
        val resolver = broadcast.value.getComparator(dimClass)
        val pair = resolver.resolve(newDim, historyDim._1.asInstanceOf[DimEntity])

        if (pair.getLeft != null) {
          answerList = pair.getLeft.stringify() :: answerList

          if (pair.getRight != null) {
            answerList = pair.getRight.stringify() :: answerList
          }
          historyMapVar += (secondId -> null)
        } else {
          historyMapVar += (secondId -> (historyDim._1, true))
        }

      } else {
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

  def findNewAndHistoryDims(iterable: Iterable[(DimEntity, String)]): ((DimEntity, String), (DimEntity, String)) = {
    var newDim: (DimEntity, String) = null
    var historyDim: (DimEntity, String) = null
    for (dimTuple <- iterable) {
      if (dimTuple._1.getExpirationDate == null) {
        newDim = dimTuple
      } else {
        historyDim = dimTuple
      }
    }
    (newDim, historyDim)
  }

}