package ru.at_consulting.bigdata.dpc.cluster.loaders

import org.apache.hadoop.io.{LongWritable, Text, WritableComparable}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by NSkovpin on 13.05.2016.
  */
object LoadSequenceFile extends Loader{

  def loadRdd(sc: SparkContext, path: String) : RDD[String] = {
    sc.sequenceFile(path, classOf[Long], classOf[Text]).
      map(x => x._2.toString)
  }

  def loadRddRows(sc: SparkContext, path: String, delimiter: String = "\u0001"): RDD[Array[String]] ={
    sc.sequenceFile(path, classOf[Text], classOf[Text]).
      map(x => x._2.toString.split(delimiter, -1))
  }

  override def loadDataSource(sc: SparkContext, path: String, delimiter: String): RDD[Array[String]] ={
    loadRddRows(sc, path , delimiter)
  }

  override def loadDataSource(sc: SparkContext, path: String): RDD[String] = {
    loadRdd(sc, path)
  }

}
