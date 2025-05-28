/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.data

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.load.command.DestinationStream.AirbyteProxyField
import java.math.BigDecimal
import java.math.BigInteger

interface AirbyteValueProxy {
    fun getBoolean(field: AirbyteProxyField): Boolean?
    fun getString(field: AirbyteProxyField): String?
    fun getInteger(field: AirbyteProxyField): BigInteger?
    fun getNumber(field: AirbyteProxyField): BigDecimal?
    fun getDate(field: AirbyteProxyField): String?
    fun getTimeWithTimezone(field: AirbyteProxyField): String?
    fun getTimeWithoutTimezone(field: AirbyteProxyField): String?
    fun getTimestampWithTimezone(field: AirbyteProxyField): String?
    fun getTimestampWithoutTimezone(field: AirbyteProxyField): String?
    fun getJsonBytes(field: AirbyteProxyField): ByteArray?
    fun getJsonNode(field: AirbyteProxyField): JsonNode?
}
