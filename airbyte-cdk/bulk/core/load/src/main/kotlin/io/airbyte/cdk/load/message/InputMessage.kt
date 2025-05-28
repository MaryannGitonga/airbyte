/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.message

import com.fasterxml.jackson.databind.JsonNode
import com.google.protobuf.ByteString
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.config.DataChannelFormat
import io.airbyte.cdk.load.data.AirbyteType
import io.airbyte.cdk.load.data.AirbyteValue
import io.airbyte.cdk.load.data.ArrayType
import io.airbyte.cdk.load.data.ArrayTypeWithoutSchema
import io.airbyte.cdk.load.data.ArrayValue
import io.airbyte.cdk.load.data.BooleanType
import io.airbyte.cdk.load.data.BooleanValue
import io.airbyte.cdk.load.data.DateType
import io.airbyte.cdk.load.data.IntegerType
import io.airbyte.cdk.load.data.IntegerValue
import io.airbyte.cdk.load.data.NullValue
import io.airbyte.cdk.load.data.NumberType
import io.airbyte.cdk.load.data.NumberValue
import io.airbyte.cdk.load.data.ObjectType
import io.airbyte.cdk.load.data.ObjectTypeWithEmptySchema
import io.airbyte.cdk.load.data.ObjectTypeWithoutSchema
import io.airbyte.cdk.load.data.ObjectValue
import io.airbyte.cdk.load.data.StringType
import io.airbyte.cdk.load.data.StringValue
import io.airbyte.cdk.load.data.TimeTypeWithTimezone
import io.airbyte.cdk.load.data.TimeTypeWithoutTimezone
import io.airbyte.cdk.load.data.TimestampTypeWithTimezone
import io.airbyte.cdk.load.data.TimestampTypeWithoutTimezone
import io.airbyte.cdk.load.data.UnionType
import io.airbyte.cdk.load.data.UnknownType
import io.airbyte.cdk.load.data.json.JsonToAirbyteValue
import io.airbyte.cdk.load.data.json.toJson
import io.airbyte.cdk.load.message.CheckpointMessage.Checkpoint
import io.airbyte.cdk.load.message.CheckpointMessage.Stats
import io.airbyte.cdk.load.message.Meta.Companion.CHECKPOINT_ID_NAME
import io.airbyte.cdk.load.state.CheckpointId
import io.airbyte.cdk.load.state.CheckpointKey
import io.airbyte.cdk.load.util.deserializeToNode
import io.airbyte.cdk.load.util.serializeToJsonBytes
import io.airbyte.cdk.load.util.serializeToString
import io.airbyte.protocol.models.v0.AirbyteGlobalState
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import io.airbyte.protocol.models.v0.AirbyteRecordMessageFileReference
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.protobuf.AirbyteMessageOuterClass
import io.airbyte.protocol.protobuf.AirbyteRecordMessageMetaOuterClass
import io.airbyte.protocol.protobuf.AirbyteRecordMessageOuterClass
import java.io.OutputStream

sealed interface InputMessage {
    fun asProtocolMessage(): AirbyteMessage
    fun asProtobuf(): AirbyteMessageOuterClass.AirbyteMessage =
        AirbyteMessageOuterClass.AirbyteMessage.newBuilder()
            .setAirbyteProtocolMessage(asProtocolMessage().serializeToString())
            .build()

    fun writeProtocolMessage(
        dataChannelFormat: DataChannelFormat = DataChannelFormat.JSONL,
        outputStream: OutputStream
    ): Unit {
        when (dataChannelFormat) {
            DataChannelFormat.JSONL ->
                asProtocolMessage().serializeToJsonBytes().also {
                    outputStream.write(it)
                    outputStream.write('\n'.code)
                }
            DataChannelFormat.PROTOBUF -> asProtobuf().writeDelimitedTo(outputStream)
            else ->
                throw IllegalArgumentException(
                    "Unsupported data channel format: $dataChannelFormat"
                )
        }
        outputStream.flush()
    }
}

data class InputRecord(
    val stream: DestinationStream,
    val data: AirbyteValue,
    val emittedAtMs: Long,
    val meta: Meta?,
    val serialized: String,
    val fileReference: AirbyteRecordMessageFileReference? = null,
    val checkpointId: CheckpointId? = null,
    val unknownFieldNames: Set<String> = emptySet(),
) : InputMessage {
    /** Convenience constructor, primarily intended for use in tests. */
    constructor(
        stream: DestinationStream,
        data: String,
        emittedAtMs: Long,
        changes: MutableList<Meta.Change> = mutableListOf(),
        fileReference: AirbyteRecordMessageFileReference? = null,
        checkpointId: CheckpointId? = null,
        unknownFieldNames: Set<String> = emptySet(),
    ) : this(
        stream = stream,
        data = JsonToAirbyteValue().convert(data.deserializeToNode()),
        emittedAtMs = emittedAtMs,
        meta = Meta(changes),
        serialized = "",
        fileReference,
        checkpointId,
        unknownFieldNames
    )

    override fun asProtobuf(): AirbyteMessageOuterClass.AirbyteMessage {
        val recordBuilder =
            AirbyteRecordMessageOuterClass.AirbyteRecordMessage.newBuilder()
                .setStreamName(stream.descriptor.name)
                .setEmittedAtMs(emittedAtMs)
        checkpointId?.let { recordBuilder.setPartitionId(it.value) }
        stream.descriptor.namespace?.let { recordBuilder.setStreamNamespace(it) }
        meta?.let { meta ->
            recordBuilder.setMeta(
                AirbyteRecordMessageMetaOuterClass.AirbyteRecordMessageMeta.newBuilder()
                    .addAllChanges(
                        meta.changes.map {
                            AirbyteRecordMessageMetaOuterClass.AirbyteRecordMessageMetaChange
                                .newBuilder()
                                .setField(it.field)
                                .setChange(
                                    AirbyteRecordMessageMetaOuterClass.AirbyteRecordChangeType
                                        .valueOf(it.change.name)
                                )
                                .setReason(
                                    AirbyteRecordMessageMetaOuterClass.AirbyteRecordChangeReasonType
                                        .valueOf(it.reason.name)
                                )
                                .build()
                        }
                    )
            )
        }
        val orderedSchema = stream.schemaInAirbyteProxyOrder
        data as ObjectValue
        orderedSchema.forEach { field ->
            val protoField =
                if (field.type is UnknownType || field.type is UnionType) {
                    data.values[field.name]?.let {
                        AirbyteRecordMessageOuterClass.AirbyteValue.newBuilder()
                            .setJson(ByteString.copyFrom(it.toJson().serializeToJsonBytes()))
                            .build()
                    }
                        ?: toProtobuf(NullValue, field.type)
                } else {
                    toProtobuf(data.values[field.name] ?: NullValue, field.type)
                }
            println("converting field $field (value=${data.values[field.name]}) to $protoField")
            recordBuilder.addData(protoField)
        }

        return AirbyteMessageOuterClass.AirbyteMessage.newBuilder().setRecord(recordBuilder).build()
    }

    private fun toProtobuf(
        value: AirbyteValue,
        type: AirbyteType
    ): AirbyteRecordMessageOuterClass.AirbyteValue {
        val b = AirbyteRecordMessageOuterClass.AirbyteValue.newBuilder()
        if (value is NullValue) {
            return b.setIsNull(true).build()
        }
        fun setJson(value: AirbyteValue) =
            b.setJson(ByteString.copyFrom(value.toJson().serializeToJsonBytes()))
        when (type) {
            is BooleanType ->
                if (value is BooleanValue) b.setBoolean(value.value) else b.setIsNull(true)
            is StringType ->
                if (value is StringValue) b.setString(value.value) else b.setIsNull(true)
            is NumberType ->
                if (value is NumberValue) {
                    if (value.value.equals(value.value.toDouble())) {
                        b.setNumber(value.value.toDouble())
                    } else {
                        b.setBigDecimal(value.value.toString())
                    }
                } else {
                    setJson(value)
                }
            is IntegerType ->
                if (value is IntegerValue) {
                    if (value.value.equals(value.value.toLong())) {
                        b.setInteger(value.value.toLong())
                    } else {
                        b.setBigInteger(value.value.toString())
                    }
                } else {
                    b.setIsNull(true)
                }
            is DateType -> if (value is StringValue) b.setDate(value.value) else b.setIsNull(true)
            is TimeTypeWithTimezone ->
                if (value is StringValue) b.setTimeWithTimezone(value.value) else b.setIsNull(true)
            is TimeTypeWithoutTimezone ->
                if (value is StringValue) b.setTimeWithoutTimezone(value.value)
                else b.setIsNull(true)
            is TimestampTypeWithTimezone ->
                if (value is StringValue) {
                    b.setTimestampWithTimezone(value.value)
                } else {
                    b.setIsNull(true)
                }
            is TimestampTypeWithoutTimezone ->
                if (value is StringValue) {
                    b.setTimestampWithoutTimezone(value.value)
                } else {
                    b.setIsNull(true)
                }
            is ArrayType,
            ArrayTypeWithoutSchema ->
                if (value is ArrayValue) {
                    b.setJson(ByteString.copyFrom(value.toJson().serializeToJsonBytes()))
                } else {
                    b.setIsNull(true)
                }
            is ObjectType,
            ObjectTypeWithEmptySchema,
            ObjectTypeWithoutSchema ->
                if (value is ObjectValue) {
                    b.setJson(ByteString.copyFrom(value.toJson().serializeToJsonBytes()))
                } else {
                    b.setIsNull(true)
                }
            is UnionType,
            is UnknownType -> b.setJson(ByteString.copyFrom(value.toJson().serializeToJsonBytes()))
        }

        return b.build()
    }

    override fun asProtocolMessage(): AirbyteMessage =
        AirbyteMessage()
            .withType(AirbyteMessage.Type.RECORD)
            .withRecord(
                AirbyteRecordMessage()
                    .withStream(stream.descriptor.name)
                    .withNamespace(stream.descriptor.namespace)
                    .withEmittedAt(emittedAtMs)
                    .withData(data.toJson())
                    .also {
                        if (meta != null) {
                            it.withMeta(meta.asProtocolObject())
                        }
                        if (fileReference != null) {
                            it.withFileReference(fileReference)
                        }
                        if (checkpointId != null) {
                            it.additionalProperties[CHECKPOINT_ID_NAME] = checkpointId.value
                        }
                    }
            )
}

data class InputFile(
    val file: DestinationFile,
) : InputMessage {
    constructor(
        stream: DestinationStream,
        emittedAtMs: Long,
        fileMessage: DestinationFile.AirbyteRecordMessageFile,
    ) : this(
        DestinationFile(
            stream,
            emittedAtMs,
            fileMessage,
        )
    )
    override fun asProtocolMessage(): AirbyteMessage = file.asProtocolMessage()
}

sealed interface InputCheckpoint : InputMessage

data class InputStreamCheckpoint(val checkpoint: StreamCheckpoint) : InputCheckpoint {
    constructor(
        streamNamespace: String?,
        streamName: String,
        blob: String,
        sourceRecordCount: Long,
        destinationRecordCount: Long? = null,
        checkpointKey: CheckpointKey? = null,
    ) : this(
        StreamCheckpoint(
            Checkpoint(
                DestinationStream.Descriptor(streamNamespace, streamName),
                state = blob.deserializeToNode()
            ),
            Stats(sourceRecordCount),
            destinationRecordCount?.let { Stats(it) },
            emptyMap(),
            0L,
            checkpointKey,
        )
    )
    override fun asProtocolMessage(): AirbyteMessage = checkpoint.asProtocolMessage()
}

data class InputGlobalCheckpoint(
    val sharedState: JsonNode?,
    val checkpointKey: CheckpointKey? = null
) : InputCheckpoint {
    override fun asProtocolMessage(): AirbyteMessage =
        AirbyteMessage()
            .withType(AirbyteMessage.Type.STATE)
            .withState(
                AirbyteStateMessage()
                    .withType(AirbyteStateMessage.AirbyteStateType.GLOBAL)
                    .withGlobal(AirbyteGlobalState().withSharedState(sharedState))
                    .also {
                        if (checkpointKey != null) {
                            it.additionalProperties["partition_id"] =
                                checkpointKey.checkpointId.value
                            it.additionalProperties["id"] = checkpointKey.checkpointIndex.value
                        }
                    }
            )
}

data class InputStreamComplete(val streamComplete: DestinationRecordStreamComplete) : InputMessage {
    override fun asProtocolMessage(): AirbyteMessage = streamComplete.asProtocolMessage()
}

data class InputMessageOther(val airbyteMessage: AirbyteMessage) : InputMessage {
    override fun asProtocolMessage(): AirbyteMessage = airbyteMessage
}
