# LateKafka
Late-ack Kafka integration for Akka Streams.  Not this is not full-featured at all!  It basically facilitates a reactive Akka stream for Kafka for direct topic/partition consumption.

The idea is similar to my LateRabbit project and also overlaps the Akka-official reactive-kafka project.  The difference between this project and reactive-kafka is that this project is much more stripped-down for raw speed at the expense of "safety equipment" like retries, etc.

Speed is king for LateKafka, and I've seen speeds nearly hitting 500,000 messages/second through a trivial do-nothing Akka stream (see test code).  This becomes a theoretical bounding-box for performance.

Like LateRabbit there is an object wrapper that contains commitable information (Kafka offsets and partition information) along with whatever your payload object is.

Please see test code for examples of use.

Indended use case is to use the provided FlowActor, which contains a GraphHolder.  All this is to ensure that your Akka RunnableGraph will run as its own thread, which keeps them from crashing into each other if you have more than one.  (You can also just run a flow outside of FlowActor too if you only need one.)

The key here is that LateKafka uses a thread to hold the KafkaConsumer, which is strictly single-threaded.
