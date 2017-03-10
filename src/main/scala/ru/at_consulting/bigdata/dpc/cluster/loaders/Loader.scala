package ru.at_consulting.bigdata.dpc.cluster.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by NSkovpin on 13.05.2016.
  */
trait Loader {

  def loadDataSource(sc: SparkContext, path: String, delimiter : String): RDD[Array[String]]

  def loadDataSource(sc: SparkContext, path: String): RDD[String]

}
