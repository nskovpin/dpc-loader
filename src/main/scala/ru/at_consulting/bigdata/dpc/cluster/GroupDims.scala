package ru.at_consulting.bigdata.dpc.cluster

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import ru.at_consulting.bigdata.dpc.cluster.groups.GroupTraitFactory
import ru.at_consulting.bigdata.dpc.cluster.loader.ParserJson
import ru.at_consulting.bigdata.dpc.cluster.loaders.{LoadSequenceFile, LoadTextFile, Loader}
import ru.at_consulting.bigdata.dpc.cluster.system.ClusterProperties
import ru.at_consulting.bigdata.dpc.dim._

/**
  * Created by NSkovpin on 07.03.2017.
  */
object GroupDims {

  def loadDataSources(sc: SparkContext, clusterProperties: ClusterProperties, loader: Loader, clazz: Class[_], historical: Boolean): RDD[String] = {
    var path = ""
    if (historical) {
      path = clusterProperties.getHdfsOutputDir + File.separator + ParserJson.getDimName(clazz)
    } else {
      path = clusterProperties.getHdfsDataDir + File.separator + ParserJson.getDimName(clazz)
    }
    loader.loadDataSource(sc, path)
  }

  def loadDataSource(sc: SparkContext, loader: Loader, path: String):RDD[String]={
    loader.loadDataSource(sc, path)
  }

  def main(args: Array[String]) {
    println(">>>>>DPC_LOADER_START<<<<<")
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val clusterProperties = new ClusterProperties(args)
    println(">>>>>Properties:")
    println(">>>>>"+clusterProperties.toString)
    println(">>>>>SparkContext start")
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    println(">>>>>SparkContext end")

    val timeKey = clusterProperties.getTimeKey

    ClusterExecutor.execute(sc, fs, LoadSequenceFile,
      clusterProperties.getHdfsJsonPath,
      clusterProperties.getHdfsOutputDir,
      timeKey)

    println(">>>>>DPC_LOADER_END<<<<<")
    sc.stop()
  }

  def loadDataTest(sc:SparkContext, path: String, save: String): Unit ={
    LoadTextFile.loadDataSource(sc, path).map(x => ("0",x)).saveAsSequenceFile(save)
  }

}
