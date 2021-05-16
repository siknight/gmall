package app

import com.wuhui.common.Constant
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.{MyEsKafkaUtil, MyKafkaUtil}

object AlertApp {
  def main(args: Array[String]): Unit = {
    // 1. 从kafka消费数据(事件日志)
    val conf: SparkConf = new SparkConf().setAppName("DAUApp").setMaster("local[1]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val sourceDStream: InputDStream[(String, String)] = MyEsKafkaUtil.getKafkaStream(ssc, Constant.TOPIC_EVENT)
    sourceDStream.print()
  }
}
