package ru.at_consulting.bigdata.dpc.cluster.groups

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import ru.at_consulting.bigdata.dpc.dim.DimEntity
import ru.at_consulting.bigdata.dpc.dim.resolver.DimComparatorFactory

/**
  * Created by NSkovpin on 09.03.2017.
  */
object CommonGroup extends GroupTrait {

  override def groupDim(newRdd: RDD[String], historyRdd: RDD[String], dimClass: Class[_ <: DimEntity],
                        broadcast: Broadcast[DimComparatorFactory]): RDD[String] = {
    val parsedNewRDD = newRdd.map(row => {
      val dim = dimClass.newInstance()
      dim.fillObject(row)
      dim
    }).map(dim => {
      (dim.getFirstId, dim)
    }).groupByKey().map( keyAndIterable =>{
        val key = keyAndIterable._1
        val lastDim = foundLastDim(keyAndIterable._2)
      (key, lastDim)
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

    val lines = parsedNewRDD.fullOuterJoin(parsedHistoryRDD).map(join => {
      val key = join._1

      val newOption = join._2._1 match {
        case Some(dim) => dim
        case None => null
      }

      val historyOption = join._2._2 match {
        case Some(dim) => dim
        case None => null
      }

      if (newOption == null) {

        val historyDim = historyOption.asInstanceOf[DimEntity]
        (key, List(historyDim.stringify()))

      } else {
        val newDim = newOption.asInstanceOf[DimEntity]

        val resolver = broadcast.value.getComparator(dimClass)
        val pair = resolver.resolve(newDim, historyOption.asInstanceOf[DimEntity])

        var list = List[String]()
        if (pair.getLeft != null) {
          list = pair.getLeft.stringify() :: list
        }
        if (pair.getRight != null) {
          list = pair.getRight.stringify() :: list
        }
        (key, list)

      }
    }): RDD[(String, List[String])]
    lines.flatMapValues(x => x).map(keyValue => keyValue._2)
  }



}
