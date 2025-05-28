/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.file

import com.google.common.io.CountingInputStream
import io.airbyte.cdk.load.message.DestinationMessage
import io.airbyte.cdk.load.message.DestinationMessageFactory
import io.airbyte.protocol.protobuf.AirbyteMessageOuterClass.AirbyteMessage
import java.io.InputStream

class ProtobufDataChannelReader(private val destinationMessageFactory: DestinationMessageFactory) :
    DataChannelReader {
    private val parser = AirbyteMessage.parser()
    override fun read(inputStream: InputStream): Sequence<DestinationMessage> = sequence {
        val countingInputStream = CountingInputStream(inputStream)
        var count = countingInputStream.count
        while (true) {
            val protoMessage = parser.parseDelimitedFrom(inputStream) ?: break
            val newCount = countingInputStream.count
            val serializedSizeBytes = newCount - count
            count = newCount
            yield(
                destinationMessageFactory.fromAirbyteProtobufMessage(
                    protoMessage,
                    serializedSizeBytes
                )
            )
        }
    }
}
