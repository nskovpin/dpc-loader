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

  def groupNewWithHistoryRdd(newRdd: RDD[(String, DimEntity)], historyRdd: RDD[(String, DimEntity)],
                             dimClass: Class[_<:DimEntity], broadcast: Broadcast[DimComparatorFactory]): RDD[DimEntity] = {
    val firstGrouping = newRdd.union(historyRdd).groupByKey()

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
      var answerList = List[DimEntity]()

      if (dimTuple._1._1 != null) {
        // found newDim
        val newDim = dimTuple._1._1
        val historyDim = dimTuple._2._1

        val resolver = broadcast.value.getComparator(dimClass)
        val pair = resolver.resolve(newDim, dimTuple._2._1)

        if (pair.getLeft != null) {
          // newDim has changes
          answerList = pair.getLeft :: answerList
        }
        if (pair.getRight != null) {
          answerList = pair.getRight :: answerList
        }
      } else {
        //not found newDim
        val historyDim = dimTuple._2._1
        if (dimTuple._2._2 != null) {
          historyDim.setExpirationDate(dimTuple._2._2)
        }
        answerList = historyDim :: answerList
      }
      (key, answerList)
    }).flatMapValues(x => x).map(x =>x._2)
    rdd
  }

  override def groupDimRdds(newRdd: RDD[DimEntity], historyRdd: RDD[DimEntity], dimClass: Class[_ <: DimEntity], broadcast: Broadcast[DimComparatorFactory]): RDD[(String, String)] = {
    val parsedNewRDD = newRdd.map(dim => {
      ((dim.getFirstId, dim.getSecondId), dim)
    }).groupByKey().map(keyAndIterable => {
      val key = keyAndIterable._1
      val dim = foundLastDim(keyAndIterable._2)
      (key, dim)
    }).map(tupleAndDim => {

      val firstKey = tupleAndDim._1._1
      (firstKey, tupleAndDim._2)
    })

    val parsedHistoryRDD = historyRdd.filter(dim => dim.getExpirationDate != null).filter(dim => dim.getExpirationDate.equals(DimEntity.EXPIRATION_DATE_INFINITY))
      .map(dim => {
        (dim.getFirstId, dim)
      })

    groupNewWithHistoryRdd(parsedNewRDD, parsedHistoryRDD, dimClass, broadcast)
      .map(x => (x.stringifyExpirationDate(), x.stringify()))
  }

}