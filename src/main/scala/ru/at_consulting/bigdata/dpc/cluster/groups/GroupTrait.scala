package ru.at_consulting.bigdata.dpc.cluster.groups

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import ru.at_consulting.bigdata.dpc.dim.DimEntity
import ru.at_consulting.bigdata.dpc.dim.resolver.DimComparatorFactory


/**
  * Created by NSkovpin on 07.03.2017.
  */
trait GroupTrait {

  def foundLastDim(iterable: Iterable[DimEntity]): DimEntity = {
    var dim: DimEntity = null
    for(dimEntity <- iterable){

      if(dim == null){
        dim = dimEntity
      }else{
        val lastDate = DateTime.parse(dim.getEffectiveDate)
        val currentDate = DateTime.parse(dimEntity.getEffectiveDate)

        if(lastDate.isBefore(currentDate)){
          dim = dimEntity
        }
      }
    }
    dim
  }

  def checkNewRdd(newRdd: RDD[String]): Boolean ={
    newRdd.count() > 0
  }

  def findNewAndHistoryDims(iterable: Iterable[(DimEntity, String)]): ((DimEntity, String), (DimEntity, String)) = {
    var newDim: (DimEntity, String) = (null, null)
    var historyDim: (DimEntity, String) = (null, null)
    for (dimTuple <- iterable) {
      if (dimTuple._1.getExpirationDate == null) {
        newDim = dimTuple
      } else {
        historyDim = dimTuple
      }
    }
    (newDim, historyDim)
  }

  def groupRdds(newRdd: RDD[DimEntity], historyRdd: RDD[DimEntity], sc: SparkContext, dimClass: Class[_<:DimEntity]):RDD[(String, String)]={
    val broadcastFactory = sc.broadcast(new DimComparatorFactory)
    groupDimRdds(newRdd, historyRdd, dimClass, broadcastFactory)
  }

  def groupDimRdds(newRdd: RDD[DimEntity], historyRdd: RDD[DimEntity],
               dimClass: Class[_ <: DimEntity], broadcast: Broadcast[DimComparatorFactory]):RDD[(String, String)]

}
