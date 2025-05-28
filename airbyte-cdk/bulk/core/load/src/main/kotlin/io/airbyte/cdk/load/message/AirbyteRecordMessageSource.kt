/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.message

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.cdk.load.command.DestinationStream.AirbyteProxyField
import io.airbyte.cdk.load.data.AirbyteValueJsonlProxy
import io.airbyte.cdk.load.data.AirbyteValueProtobufProxy
import io.airbyte.cdk.load.data.AirbyteValueProxy
import io.airbyte.cdk.load.data.ArrayType
import io.airbyte.cdk.load.data.ArrayTypeWithoutSchema
import io.airbyte.cdk.load.data.BooleanType
import io.airbyte.cdk.load.data.DateType
import io.airbyte.cdk.load.data.IntegerType
import io.airbyte.cdk.load.data.NumberType
import io.airbyte.cdk.load.data.ObjectType
import io.airbyte.cdk.load.data.ObjectTypeWithEmptySchema
import io.airbyte.cdk.load.data.ObjectTypeWithoutSchema
import io.airbyte.cdk.load.data.StringType
import io.airbyte.cdk.load.data.TimeTypeWithTimezone
import io.airbyte.cdk.load.data.TimeTypeWithoutTimezone
import io.airbyte.cdk.load.data.TimestampTypeWithTimezone
import io.airbyte.cdk.load.data.TimestampTypeWithoutTimezone
import io.airbyte.cdk.load.data.UnionType
import io.airbyte.cdk.load.data.UnknownType
import io.airbyte.cdk.load.util.serializeToString
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteRecordMessageMetaChange.Change
import io.airbyte.protocol.models.v0.AirbyteRecordMessageMetaChange.Reason
import io.airbyte.protocol.protobuf.AirbyteMessageOuterClass

sealed interface AirbyteRecordMessageSource {
    val emittedAtMs: Long
    val sourceMeta: Meta
    val fileReference: FileReference?
    fun asJsonRecord(orderedSchema: Array<AirbyteProxyField>): JsonNode
    fun asAirbyteValueProxy(): AirbyteValueProxy
}

@JvmInline
value class AirbyteRecordJsonSource(val source: AirbyteMessage) : AirbyteRecordMessageSource {
    override val emittedAtMs: Long
        get() = source.record.emittedAt
    override val sourceMeta: Meta
        get() =
            Meta(
                changes =
                    source.record.meta?.changes?.map { change ->
                        Meta.Change(
                            field = change.field,
                            change = change.change,
                            reason = change.reason,
                        )
                    }
                        ?: emptyList()
            )

    override val fileReference: FileReference?
        get() = source.record.fileReference?.let { FileReference.fromProtocol(it) }

    override fun asJsonRecord(orderedSchema: Array<AirbyteProxyField>): JsonNode =
        source.record.data

    override fun asAirbyteValueProxy(): AirbyteValueProxy =
        AirbyteValueJsonlProxy(source.record.data as ObjectNode)
}

@JvmInline
value class AirbyteRecordProtobufSource(val source: AirbyteMessageOuterClass.AirbyteMessage) :
    AirbyteRecordMessageSource {
    override val emittedAtMs: Long
        get() = source.record.emittedAtMs
    override val sourceMeta: Meta
        get() =
            Meta(
                changes =
                    source.record.meta?.changesList?.map { change ->
                        Meta.Change(
                            field = change.field,
                            change = Change.fromValue(change.change.name),
                            reason = Reason.fromValue(change.reason.name)
                        )
                    }
                        ?: emptyList()
            )

    override val fileReference: FileReference?
        get() = null

    override fun asJsonRecord(orderedSchema: Array<AirbyteProxyField>): JsonNode {
        val proxy = asAirbyteValueProxy()
        val objectNode = JsonNodeFactory.instance.objectNode()
        orderedSchema.forEach {
            val result =
                when (it.type) {
                    is ArrayType,
                    ArrayTypeWithoutSchema,
                    is ObjectType,
                    ObjectTypeWithoutSchema,
                    ObjectTypeWithEmptySchema,
                    is UnionType,
                    is UnknownType ->
                        proxy.getJsonNode(it)?.let { v -> objectNode.set<JsonNode>(it.name, v) }
                    BooleanType -> proxy.getBoolean(it)?.let { v -> objectNode.put(it.name, v) }
                    IntegerType -> proxy.getInteger(it)?.let { v -> objectNode.put(it.name, v) }
                    NumberType -> proxy.getNumber(it)?.let { v -> objectNode.put(it.name, v) }
                    StringType -> proxy.getString(it)?.let { v -> objectNode.put(it.name, v) }
                    DateType -> proxy.getDate(it)?.let { v -> objectNode.put(it.name, v) }
                    TimeTypeWithTimezone ->
                        proxy.getTimeWithTimezone(it)?.let { v -> objectNode.put(it.name, v) }
                    TimeTypeWithoutTimezone ->
                        proxy.getTimeWithoutTimezone(it)?.let { v -> objectNode.put(it.name, v) }
                    TimestampTypeWithTimezone ->
                        proxy.getTimestampWithTimezone(it)?.let { v -> objectNode.put(it.name, v) }
                    TimestampTypeWithoutTimezone ->
                        proxy.getTimestampWithoutTimezone(it)?.let { v ->
                            objectNode.put(it.name, v)
                        }
                }
        }
        println("converted back to json ${objectNode.serializeToString()}")
        return objectNode
    }

    override fun asAirbyteValueProxy(): AirbyteValueProxy =
        AirbyteValueProtobufProxy(source.record.dataList)
}
