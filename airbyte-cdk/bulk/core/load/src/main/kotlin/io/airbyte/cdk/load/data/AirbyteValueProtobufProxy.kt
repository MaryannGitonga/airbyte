/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.data

import com.fasterxml.jackson.core.io.BigDecimalParser
import com.fasterxml.jackson.core.io.BigIntegerParser
import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.load.command.DestinationStream.AirbyteProxyField
import io.airbyte.cdk.load.util.Jsons
import io.airbyte.protocol.protobuf.AirbyteRecordMessageOuterClass
import java.math.BigDecimal
import java.math.BigInteger

class AirbyteValueProtobufProxy(
    private val data: List<AirbyteRecordMessageOuterClass.AirbyteValue>
) : AirbyteValueProxy {
    private inline fun <T> getNullable(
        field: AirbyteProxyField,
        getter: (AirbyteProxyField) -> T
    ): T? = if (data[field.index].isNull) null else getter(field)

    override fun getBoolean(field: AirbyteProxyField): Boolean? =
        getNullable(field) { data[it.index].boolean }

    override fun getString(field: AirbyteProxyField): String? =
        getNullable(field) { data[it.index].string }

    override fun getInteger(field: AirbyteProxyField): BigInteger? =
        getNullable(field) {
            if (data[it.index].hasBigInteger()) {
                BigIntegerParser.parseWithFastParser(data[it.index].bigInteger)
            } else {
                data[it.index].integer.toBigInteger()
            }
        }

    override fun getNumber(field: AirbyteProxyField): BigDecimal? =
        getNullable(field) {
            if (data[it.index].hasBigDecimal()) {
                BigDecimalParser.parseWithFastParser(data[it.index].bigDecimal)
            } else if (data[it.index].hasNumber()) {
                data[it.index].number.toBigDecimal()
            } else {
                null
            }
        }

    override fun getDate(field: AirbyteProxyField): String? =
        getNullable(field) { data[field.index].date }

    override fun getTimeWithTimezone(field: AirbyteProxyField): String? =
        getNullable(field) { data[field.index].timeWithTimezone }

    override fun getTimeWithoutTimezone(field: AirbyteProxyField): String? =
        getNullable(field) { data[field.index].timeWithoutTimezone }

    override fun getTimestampWithTimezone(field: AirbyteProxyField): String? =
        getNullable(field) { data[field.index].timestampWithTimezone }

    override fun getTimestampWithoutTimezone(field: AirbyteProxyField): String? =
        getNullable(field) { data[field.index].timestampWithoutTimezone }

    override fun getJsonBytes(field: AirbyteProxyField): ByteArray? =
        getNullable(field) { data[field.index].json.toByteArray() }

    override fun getJsonNode(field: AirbyteProxyField): JsonNode? =
        getJsonBytes(field)?.let { Jsons.readTree(it) }
}
