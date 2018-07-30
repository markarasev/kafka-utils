import java.util

import org.apache.kafka.clients.consumer.{
  ConsumerRecords,
  KafkaConsumer,
  OffsetAndMetadata,
  ConsumerRecord => JConsumerRecord
}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

package object kafka {

  object ProducerRecord {

    import org.apache.kafka.clients.producer.ProducerRecord

    def apply[K, V](topic: String, value: V): ProducerRecord[K, V] =
      new ProducerRecord[K, V](topic, value)

    def apply[K, V](topic: String, key: K, value: V): ProducerRecord[K, V] =
      new ProducerRecord[K, V](topic, key, value)

  }

  case class ConsumerRecord[K, V](
      topic: String,
      partition: Int,
      offset: Long,
      key: K,
      value: V
  ) {

    val topicPartition: TopicPartition = new TopicPartition(topic, partition)

    val nextOffsetAndMetadata: OffsetAndMetadata = new OffsetAndMetadata(
      offset + 1)

    val nextOffsetMap: Map[TopicPartition, OffsetAndMetadata] = Map(
      topicPartition -> nextOffsetAndMetadata)

    val nextjOffsetMap: util.Map[TopicPartition, OffsetAndMetadata] =
      nextOffsetMap.asJava

    def toJava: JConsumerRecord[K, V] =
      new JConsumerRecord[K, V](
        topic,
        partition,
        offset,
        key,
        value
      )

  }

  case class Topic(underlying: String) extends AnyVal {
    override def toString: String = underlying
  }

  object ConsumerRecord {

    def fromJava[K, V](record: JConsumerRecord[K, V]): ConsumerRecord[K, V] =
      new ConsumerRecord(record.topic(),
                         record.partition(),
                         record.offset(),
                         record.key(),
                         record.value())

  }

  class Consumer[K, V](val u: KafkaConsumer[K, V]) extends AnyVal {

    def close(): Unit = u.close()

    def subscribe(topics: Topic*): Unit =
      u.subscribe(topics.map(_.underlying).asJava)

    def poll(timeout: FiniteDuration): ConsumerRecords[K, V] =
      u.poll(timeout.toMillis)

    def pollAndExtractRecords(
        timeout: FiniteDuration,
        topicToExtract: Topic
    ): Seq[ConsumerRecord[K, V]] =
      u.poll(timeout.toMillis)
        .records(topicToExtract.underlying)
        .asScala
        .to[Seq]
        .map(ConsumerRecord.fromJava)

  }

  object Consumer {

    def apply[K, V](
        conf: cakesolutions.kafka.KafkaConsumer.Conf[K, V]): Consumer[K, V] =
      new Consumer(cakesolutions.kafka.KafkaConsumer(conf))

  }

  sealed trait AutoOffsetReset { def mode: String }
  object AutoOffsetReset {
    case object Earliest extends AutoOffsetReset {
      override val mode = "earliest"
    }
    case object Latest extends AutoOffsetReset {
      override def mode: String = "latest"
    }
    case object None extends AutoOffsetReset {
      override def mode: String = "none"
    }
  }
}
