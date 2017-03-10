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

  def group(newRdd: RDD[String], historyRdd: RDD[String], sc: SparkContext, dimClass: Class[_ <: DimEntity]):RDD[String] ={
    val broadcastFactory = sc.broadcast(new DimComparatorFactory)
    groupDim(newRdd, historyRdd, dimClass, broadcastFactory)
  }

  def groupDim(newRdd: RDD[String], historyRdd: RDD[String],
               dimClass: Class[_ <: DimEntity], broadcast: Broadcast[DimComparatorFactory]):RDD[String]



}
