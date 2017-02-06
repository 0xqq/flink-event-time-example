package me.baghino.flink.etexample

import java.time._

import me.baghino.flink.etexample.Util._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.slf4j.LoggerFactory

object ETWithWatermarks {

  def main(args: Array[String]) {

    val logger = LoggerFactory.getLogger(getClass)

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

            @transient var highestWatermarkTimestamp = Long.MinValue
            override def checkAndGetNextWatermark(lastElement: Event, extractedTimestamp: Long): Watermark = {
              val expectedWatermarkTimestamp = extractedTimestamp - maxDelayMs
              if (expectedWatermarkTimestamp > highestWatermarkTimestamp) {
                highestWatermarkTimestamp = expectedWatermarkTimestamp
                logger.info(s"NEW WATERMARK ${timestampToString(highestWatermarkTimestamp)}")
              }
              // Flink will automatically watermarks earlier than already observed
              new Watermark(expectedWatermarkTimestamp)
            }
          })

    val windowed = timestampedAndWatermarked.timeWindowAll(Time.seconds(1))

    logEarliestIdAndTimestamp(windowed)

    env.execute()

  }

}
