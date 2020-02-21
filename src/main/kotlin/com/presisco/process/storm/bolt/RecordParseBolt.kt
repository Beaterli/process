package com.presisco.process.storm.bolt

import com.presisco.gsonhelper.MapHelper
import com.presisco.lazystorm.DATA_FIELD_NAME
import com.presisco.lazystorm.bolt.LazyBasicBolt
import com.presisco.lazystorm.mapValueToHashMap
import com.presisco.process.getString
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.slf4j.LoggerFactory

class RecordParseBolt: LazyBasicBolt<Any>() {
    private val logger = LoggerFactory.getLogger(RecordParseBolt::class.java)

    @Transient
    private lateinit var mapHelper: MapHelper

    override fun prepare(stormConf: MutableMap<Any?, Any?>?, context: TopologyContext?) {
        mapHelper = MapHelper()
    }

    override fun execute(tuple: Tuple, collector: BasicOutputCollector) {
        val record = mapHelper.fromJson(tuple.getStringByField("value"))
        record["version"]?:return
        val version = record.getString("version")
        val parsed = record.mapValueToHashMap { value ->
            if(value == null){
                null
            } else if (value is String && value.startsWith("{")){
                mapHelper.fromJson(value)
            } else {
                value
            }
        }
        when(version){
            "repost", "search1" -> collector.emitDataToStreams("blog", parsed)
            "comment1" -> collector.emitDataToStreams("comment", parsed)
            else -> logger.info("unknown version: $version")
        }
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        declarer.declareStream("blog", Fields(DATA_FIELD_NAME))
        declarer.declareStream("comment", Fields(DATA_FIELD_NAME))
    }
}