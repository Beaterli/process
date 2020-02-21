package com.presisco.process.storm.bolt

import com.presisco.lazystorm.DATA_FIELD_NAME
import com.presisco.lazystorm.bolt.redis.JedisSingletonBolt
import com.presisco.process.getInt
import com.presisco.process.getString
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values

class ExistenceBolt : JedisSingletonBolt<Map<String, *>>() {

    override fun execute(tuple: Tuple, collector: BasicOutputCollector) {
        val cmd = getCommand()
        val data = getInput(tuple)
        val id = tuple.getStringByField("id")

        when (tuple.sourceStreamId) {
            "blog" -> {
                val prevScrapTime = cmd.hget("blog_map", id)
                val scrapTime = data.getString("scrap_time")

                if (prevScrapTime == null) {
                    collector.emit("new_blog", Values(data, id))
                    cmd.hset("blog_map", id, scrapTime)
                } else if (prevScrapTime < scrapTime) {
                    val updateMap = hashMapOf<String, Any?>(
                            "mid" to data.getString("mid"),
                            "content" to data.getString("content"),
                            "like" to data.getInt("like"),
                            "comment" to data.getInt("comment"),
                            "repost" to data.getInt("repost"),
                            "scrap_time" to scrapTime
                    )
                    if (data["repost_id"] != null) {
                        updateMap["repost_id"] = data["repost_id"]
                        updateMap["repost_link"] = data["repost_link"]
                    }
                    collector.emit("update_blog", Values(updateMap, id))
                    cmd.hset("blog_map", id, scrapTime)
                }
            }
            "comment" -> {
                val prevScrapTime = cmd.hget("comment_map", id)
                val scrapTime = data.getString("scrap_time")

                if (prevScrapTime == null) {
                    collector.emit("new_comment", Values(data, id))
                    cmd.hset("comment_map", id, scrapTime)
                } else if (prevScrapTime < scrapTime) {
                    collector.emit("update_comment", Values(hashMapOf(
                            "cid" to data.getString("cid"),
                            "like" to data.getInt("like"),
                            "scrap_time" to scrapTime
                    ), id))
                    cmd.hset("comment_map", id, scrapTime)
                }
            }
            "root" -> {
                val prevKeyword = cmd.hget("root_map", id)
                if (prevKeyword == null) {
                    collector.emit("new_root", Values(data, id))
                    cmd.hset("root_map", id, data.getString("keyword"))
                }
            }
            else -> {
                val exception = IllegalStateException("unsupported stream: ${tuple.sourceComponent}:${tuple.sourceStreamId}")
                collector.reportError(exception)
                throw exception
            }
        }

        closeCommand(cmd)
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        with(declarer){
            declareStream("new_blog", Fields(DATA_FIELD_NAME, "id"))
            declareStream("new_comment", Fields(DATA_FIELD_NAME, "id"))
            declareStream("new_root", Fields(DATA_FIELD_NAME, "id"))
            declareStream("update_blog", Fields(DATA_FIELD_NAME, "id"))
            declareStream("update_comment", Fields(DATA_FIELD_NAME, "id"))
        }
    }
}