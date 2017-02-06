package me.baghino.flink.etexample

import java.time.{Instant, LocalDateTime, ZoneId}

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.slf4j.LoggerFactory

object Util {

  private[this] val logger = LoggerFactory.getLogger(getClass)

  def timestampToString(timestamp: Long): String =
    LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault).toString

  def logEarliestIdAndTimestamp(in: AllWindowedStream[Event, TimeWindow]): Unit =
    in.fold((Long.MaxValue, Long.MaxValue)) { case ((id, ts), e) => (math.min(id, e.id), math.min(ts, e.timestamp)) } .addSink {
      idts => {
        val (id, ts) = idts
        logger.info(f"id: ${id}%3d | ts: ${timestampToString(ts)}")
      }
    }

}
