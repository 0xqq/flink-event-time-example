Flink Event Time Example
===

A small set of very simple programs to showcase how Flink allows to treat event time, in particular related to windowing.

All the programs will simply generate some randomly skewed events, window them each second and show for each window the earliest event id (processing time) and timestamp (event time).

`me.baghino.flink.etexample.ProcessingTime`: here events go through the pipeline in terms of processing time. You will see always increasing event ids as they are generated sequentially, but you will notice event times out of order.

`me.baghino.flink.etexample.EventTime`: here we will use Flink to make windows by event time, providing it with a way to extract timestamp from our event and a way to have a sense of its expected lateness using a watermark

`me.baghino.flink.etexample.ETWithWatermarks`: same as before but also prints out watermarks as they are observed
