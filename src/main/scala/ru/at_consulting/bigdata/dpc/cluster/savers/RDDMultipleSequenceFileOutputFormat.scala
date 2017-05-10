package ru.at_consulting.bigdata.dpc.cluster.savers

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat

/**
  * Created by NSkovpin on 04.05.2017.
  */
class RDDMultipleSequenceFileOutputFormat extends MultipleSequenceFileOutputFormat[Text, Text]{

  override def generateActualKey(key:Text, value: Text) : Text = key

  override def generateFileNameForKeyValue(key: Text, value: Text, name: String): String =
    key.toString

}
