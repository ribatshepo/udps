package io.gbmm.udps.integration.ustrp

import cats.effect.IO

/** Represents a failure that occurred during message deserialization.
  *
  * @param message human-readable description of the failure
  * @param cause   the underlying exception, if any
  * @param rawBytes the original bytes that could not be deserialized
  */
final case class DeserializationError(
    message: String,
    cause: Option[Throwable],
    rawBytes: Array[Byte]
)

/** Abstraction for deserializing raw Kafka message bytes.
  *
  * Implementations may decode Avro, Protobuf, or pass-through raw bytes.
  */
trait MessageDeserializer {

  /** Attempt to deserialize the given bytes from the specified topic.
    *
    * @param topic the Kafka topic the message was consumed from
    * @param data  the raw message payload
    * @return either a [[DeserializationError]] or the deserialized bytes
    */
  def deserialize(
      topic: String,
      data: Array[Byte]
  ): IO[Either[DeserializationError, Array[Byte]]]
}

/** Trait representing a schema registry client used by [[AvroMessageDeserializer]].
  *
  * A concrete implementation will be provided by UDPS-059. This trait
  * captures the minimal contract needed for Avro deserialization.
  */
trait SchemaRegistryClient {

  /** Decode the wire-format bytes using the schema registry.
    *
    * The first 5 bytes of Confluent wire-format messages contain a magic
    * byte and 4-byte schema ID. The implementation should use these to
    * look up the writer schema and decode the remaining payload.
    *
    * @param topic the topic the message was consumed from
    * @param data  the raw wire-format bytes
    * @return the decoded payload bytes
    */
  def decode(topic: String, data: Array[Byte]): IO[Array[Byte]]
}

/** Avro deserializer that delegates to a [[SchemaRegistryClient]] for
  * schema-aware decoding of Confluent wire-format messages.
  *
  * @param registryClient the schema registry client to use for decoding
  */
final class AvroMessageDeserializer(registryClient: SchemaRegistryClient)
    extends MessageDeserializer {

  private val ConfluentWireFormatMinSize: Int = 5

  override def deserialize(
      topic: String,
      data: Array[Byte]
  ): IO[Either[DeserializationError, Array[Byte]]] =
    if (data == null || data.length < ConfluentWireFormatMinSize) {
      IO.pure(
        Left(
          DeserializationError(
            s"Invalid Avro message: payload is null or shorter than $ConfluentWireFormatMinSize bytes",
            cause = None,
            rawBytes = Option(data).getOrElse(Array.emptyByteArray)
          )
        )
      )
    } else {
      registryClient
        .decode(topic, data)
        .map(decoded => Right(decoded))
        .handleError { ex =>
          Left(
            DeserializationError(
              s"Avro deserialization failed for topic=$topic: ${ex.getMessage}",
              cause = Some(ex),
              rawBytes = data
            )
          )
        }
    }
}

/** Pass-through deserializer that returns the raw bytes unchanged.
  *
  * This is the default deserializer used when no schema-based
  * deserialization is configured.
  */
object RawMessageDeserializer extends MessageDeserializer {

  override def deserialize(
      topic: String,
      data: Array[Byte]
  ): IO[Either[DeserializationError, Array[Byte]]] =
    if (data == null) {
      IO.pure(
        Left(
          DeserializationError(
            s"Null payload received on topic=$topic",
            cause = None,
            rawBytes = Array.emptyByteArray
          )
        )
      )
    } else {
      IO.pure(Right(data))
    }
}
