package ru.at_consulting.bigdata.dpc.cluster.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by NSkovpin on 13.05.2016.
  */
object LoadTextFile extends Loader{

  def load(sc : SparkContext, path: String, delimiter : String): RDD[Array[String]] = {
    sc.textFile(path).map(row => row.split(delimiter, -1));
  }

  override def loadDataSource(sc: SparkContext, path: String, delimiter: String): RDD[Array[String]] = {
      load(sc, path, delimiter);
  }

  override def loadDataSource(sc: SparkContext, path: String): RDD[String] = {
    sc.textFile(path)
  }
}
