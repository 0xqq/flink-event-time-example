package me.baghino.flink.etexample

import java.time.Duration

import me.baghino.flink.etexample.Util._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object ProcessingTime {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Setting parallelism to 1 to make sure that event ids are always increasing
    // This will make reasoning about Flink capabilities easier
    env.setParallelism(1)

    val baseDelayMs = 2000
    val delayCapMs = 8000

    val source = new PacedEventSource(pace = 10, Duration.ofMillis(baseDelayMs), Duration.ofMillis(delayCapMs))

    val stream = env.addSource(source)

    val windowed = stream.timeWindowAll(Time.seconds(1))

    logEarliestIdAndTimestamp(windowed)

    env.execute()

  }

}
