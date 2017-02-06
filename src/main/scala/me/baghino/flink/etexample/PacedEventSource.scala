package me.baghino.flink.etexample

import java.time.Duration
import java.util
import java.util.concurrent.{ArrayBlockingQueue, Executors, TimeUnit}

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

/**
  * A [[SourceFunction]] that generates [[Event]]s at a given pace, producing events
  * that present some kind of skew or delay relative to processing time.
  * @param pace How many events to generate per second
  * @param baseDelay What is the fixed amount of delay that the event simulates
  * @param delayCap A variable amount of delay on top of the fixed one
  */
final class PacedEventSource(pace: Int, baseDelay: Duration, delayCap: Duration) extends SourceFunction[Event] {

  require(pace > 0, "pace must be positive")

  @transient private[this] var canceled = false

  override def cancel(): Unit = synchronized { canceled = true }

  override def run(ctx: SourceContext[Event]): Unit = {
    val queue = new ArrayBlockingQueue[Event](pace)
    periodicallyEnqueueDelayedEvents(queue)
    while (!canceled) {
      queue.synchronized {
        while (queue.size() > 0) {
          ctx.collect(queue.take())
        }
      }
    }
  }

  /**
    * Every second, generate [[pace]] events, skewed by a delay specified by
    * [[baseDelay]] and [[delayCap]], and push them on the given queue.
    * @param queue The queue that will be filled by delayed events
    */
  private[this] def periodicallyEnqueueDelayedEvents(queue: util.Queue[Event]): Unit = {
    val exec = Executors.newSingleThreadScheduledExecutor()
    val (bd, dc) = (baseDelay.getSeconds.toInt * 1000, delayCap.getSeconds.toInt * 1000)
    exec.scheduleAtFixedRate(new Runnable {
      var n = 0
      override def run(): Unit = {
        val now = System.currentTimeMillis()
        queue.synchronized {
          for ((t, i) <- List.fill(pace)(now - scala.util.Random.nextInt(dc) - bd).zipWithIndex) {
            queue.add(Event(n * pace + i, t))
          }
          n += 1
        }
      }
    }, 0L, 1L, TimeUnit.SECONDS)
  }

}
