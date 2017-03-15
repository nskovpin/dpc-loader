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

  def joinNewWithHistory(newRdd: RDD[(String, DimEntity)], historyRdd: RDD[(String, DimEntity)],
                         dimClass: Class[_<:DimEntity], broadcast: Broadcast[DimComparatorFactory]): RDD[(String, List[DimEntity])] ={
    newRdd.fullOuterJoin(historyRdd).map(join => {
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
        (key, List(historyDim))

      } else {
        val newDim = newOption.asInstanceOf[DimEntity]

        val resolver = broadcast.value.getComparator(dimClass)
        val pair = resolver.resolve(newDim, historyOption.asInstanceOf[DimEntity])

        var list = List[DimEntity]()
        if (pair.getLeft != null) {
          list = pair.getLeft :: list
        }
        if (pair.getRight != null) {
          list = pair.getRight :: list
        }
        (key, list)

      }
    }): RDD[(String, List[DimEntity])]
  }

  override def groupDimRdds(newRdd: RDD[DimEntity], historyRdd: RDD[DimEntity], dimClass: Class[_ <: DimEntity], broadcast: Broadcast[DimComparatorFactory]): RDD[(String, String)] ={
    val parsedNewRDD = newRdd.map(dim => {
      (dim.getFirstId, dim)
    }).groupByKey().map( keyAndIterable =>{
      val key = keyAndIterable._1
      val lastDim = foundLastDim(keyAndIterable._2)
      (key, lastDim)
    })

    val parsedHistoryRDD = historyRdd.filter(dim => dim.getExpirationDate != null)
      .filter(dim => dim.getExpirationDate.equals(DimEntity.EXPIRATION_DATE_INFINITY)).map(dim => {
      (dim.getFirstId, dim)
    })

    val lines = joinNewWithHistory(parsedNewRDD, parsedHistoryRDD, dimClass, broadcast)
    lines.flatMapValues(x => x).map(keyValue => (keyValue._2.stringifyExpirationDate(), keyValue._2.stringify()))
  }
}
