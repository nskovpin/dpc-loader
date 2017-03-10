package ru.at_consulting.bigdata.dpc.cluster.utils

import akka.event.Logging
import org.scalatest.FunSuite
import org.scalatest.tools.ScalaTestFramework
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Created by AnVYankovskiy on 02.03.2015.
 */
object SparkTest extends org.scalatest.Tag("com.qf.test.tags.SparkTest")

trait SparkTestUtils extends FunSuite {
  var sc: SparkContext = _

  /**
   * convenience method for tests that use spark.  Creates a local spark context, and cleans
   * it up even if your test fails.  Also marks the test with the tag SparkTest, so you can
   * turn it off
   *
   * By default, it turn off spark logging, b/c it just clutters up the test output.  However,
   * when you are actively debugging one test, you may want to turn the logs on
   *
   * @param name the name of the test
   * @param silenceSpark true to turn off spark logging
   */
  def sparkTest(name: String, silenceSpark : Boolean = true)(body: => Unit) {
    test(name, SparkTest){
      val origLogLevels = if (silenceSpark) SparkUtil.silenceSpark() else null
      sc = new SparkContext("local[1]", name)
      try {
        body
      }
      finally {
        sc.stop
        sc = null
        // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
        System.clearProperty("spark.master.port")
        if (silenceSpark) Logging.WarningLevel
      }
    }
  }

  def writeToFile(p: String, s: String): Unit = {
    val pw = new java.io.PrintWriter(new java.io.File(p))
    try pw.write(s) finally pw.close()
  }

}

object SparkUtil {
  def silenceSpark() {
    setLogLevels(Level.WARN, Seq("spark", "org.eclipse.jetty", "akka"))
  }

  def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]) = {
    loggers.map{
      loggerName =>
        val logger = Logger.getLogger(loggerName)
        val prevLevel = logger.getLevel()
        logger.setLevel(level)
        loggerName -> prevLevel
    }.toMap
  }

}
