package com.presisco.process.storm.bolt

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.lazystorm.bolt.jdbc.MapJdbcClientBolt
import org.apache.storm.topology.BasicOutputCollector

class SimpleUpdateBolt : MapJdbcClientBolt() {

    override fun process(
            boltName: String,
            streamName: String,
            data: List<*>,
            table: String,
            client: MapJdbcClient,
            collector: BasicOutputCollector
    ): List<*> {
        val primaryKey = when (streamName) {
            "update_blog" -> "mid"
            "update_comment" -> "cid"
            else -> throw IllegalStateException("unexpected stream: $boltName:$streamName")
        }
        val fields = (data.first() as Map<String, *>).keys.minus(primaryKey).toMutableList()
        val updateParams = fields.joinToString(separator = ", ", transform = {"`$it` = ?"})
        fields.add(primaryKey)
        client.executeBatch(
                { "update `$table` set $updateParams where $primaryKey = ?" },
                data as List<Map<String, *>>,
                client.buildTypeMapSubset(table, data),
                fields
        )
        return emptyList<Any>()
    }

}