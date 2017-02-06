package me.baghino.flink.etexample

import java.time._

import me.baghino.flink.etexample.Util._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.slf4j.LoggerFactory

object EventTime {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Setting parallelism to 1 to make sure that event ids are always increasing
    // This will make reasoning about Flink capabilities easier
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val baseDelayMs = 2000
    val delayCapMs = 8000

    val maxDelayMs = baseDelayMs + delayCapMs

    val source = new PacedEventSource(pace = 10, Duration.ofMillis(baseDelayMs), Duration.ofMillis(delayCapMs))

    val timestampedAndWatermarked =
      env.addSource(source).
        assignTimestampsAndWatermarks(
          new AssignerWithPunctuatedWatermarks[Event] {
            override def extractTimestamp(element: Event, previousElementTimestamp: Long): Long = element.timestamp
            // Easy to get the watermark, as we actually control the delay in the system
            override def checkAndGetNextWatermark(lastElement: Event, extractedTimestamp: Long): Watermark = {
              new Watermark(extractedTimestamp - maxDelayMs)
            }
          })

    val windowed = timestampedAndWatermarked.timeWindowAll(Time.seconds(1))

    logEarliestIdAndTimestamp(windowed)

    env.execute()

  }

}
