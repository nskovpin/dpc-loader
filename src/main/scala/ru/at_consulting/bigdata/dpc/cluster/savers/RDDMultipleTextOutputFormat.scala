package ru.at_consulting.bigdata.dpc.cluster.savers

import java.io.File

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.joda.time.LocalDateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/**
  * Created by NSkovpin on 04.05.2017.
  */
class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any]{

  override def generateActualKey(key:Any, value: Any) : Any = NullWritable.get()

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    val date: LocalDateTime = LocalDateTime.parse(key.asInstanceOf[String], DateTimeFormat.forPattern("yyyy-MM-dd"))
    date.toString("yyyyMMdd") + File.separator + "part"
    }

}
