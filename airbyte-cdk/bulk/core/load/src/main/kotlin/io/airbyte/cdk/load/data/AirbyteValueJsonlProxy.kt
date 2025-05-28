/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.data

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.cdk.load.command.DestinationStream.AirbyteProxyField
import io.airbyte.cdk.load.util.serializeToJsonBytes
import java.math.BigDecimal
import java.math.BigInteger

class AirbyteValueJsonlProxy(private val data: ObjectNode) : AirbyteValueProxy {
    private inline fun <T> getNullable(
        field: AirbyteProxyField,
        getter: (AirbyteProxyField) -> T
    ): T? = data.get(field.name)?.let { if (it.isNull) null else getter(field) }

    override fun getBoolean(field: AirbyteProxyField): Boolean? =
        getNullable(field) { data.get(it.name).booleanValue() }

    override fun getString(field: AirbyteProxyField): String? =
        getNullable(field) { data.get(it.name).asText() }

    override fun getInteger(field: AirbyteProxyField): BigInteger? =
        getNullable(field) { data.get(it.name).bigIntegerValue() }

    override fun getNumber(field: AirbyteProxyField): BigDecimal? =
        getNullable(field) { data.get(it.name).decimalValue() }

    override fun getDate(field: AirbyteProxyField): String? =
        getNullable(field) { data.get(it.name).asText() }

    override fun getTimeWithTimezone(field: AirbyteProxyField): String? =
        getNullable(field) { data.get(it.name).asText() }

    override fun getTimeWithoutTimezone(field: AirbyteProxyField): String? =
        getNullable(field) { data.get(it.name).asText() }

    override fun getTimestampWithTimezone(field: AirbyteProxyField): String? =
        getNullable(field) { data.get(it.name).asText() }

    override fun getTimestampWithoutTimezone(field: AirbyteProxyField): String? =
        getNullable(field) { data.get(it.name).asText() }

    override fun getJsonBytes(field: AirbyteProxyField): ByteArray? =
        getNullable(field) { data.get(it.name).serializeToJsonBytes() }

    override fun getJsonNode(field: AirbyteProxyField): JsonNode? =
        getNullable(field) { data.get(it.name) }
}
