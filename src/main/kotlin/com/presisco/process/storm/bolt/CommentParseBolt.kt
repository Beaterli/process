package com.presisco.process.storm.bolt

import com.presisco.lazystorm.DATA_FIELD_NAME
import com.presisco.lazystorm.bolt.LazyBasicBolt
import com.presisco.lazystorm.getHashMap
import com.presisco.process.firstMatch
import com.presisco.process.getList
import com.presisco.process.getMap
import com.presisco.process.getString
import com.presisco.process.weibo.MicroBlog
import com.presisco.process.weibo.MicroBlog.numberRegex
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values

class CommentParseBolt : LazyBasicBolt<Map<String, *>>() {

    fun <T> Map<String, *>.maybeFirstOfList(key: String): T {
        return if (this[key] is List<*>) {
            this.getList<T>(key)[0]
        } else {
            this[key] as T
        }
    }

    override fun execute(tuple: Tuple, collector: BasicOutputCollector) {
        val input = getInput(tuple)

        val scrapTime = input.getString("createtime")
        val data = input.getMap<String, Any?>("data")
        val meta = input.getMap<String, Any?>("meta")

        if (data.containsKey("all")) {
            return
        }

        val rectified = hashMapOf<String, Any?>()
        val cid = MicroBlog.encodeMid(data.maybeFirstOfList("comment_id"))
        rectified["cid"] = cid

        if (data["create_time"] == null) {
            rectified["time"] = null
        } else {
            var timeString = data.maybeFirstOfList<String>("create_time")
            timeString = if (timeString.startsWith("<div")) {
                timeString.substringAfter(">")
                        .substringBefore("<")
            } else {
                timeString
            }
            if (timeString.contains("楼")) {
                timeString = timeString.substringAfter("楼 ")
            }
            rectified["time"] = timeString
        }

        rectified["scrap_time"] = scrapTime
        rectified["content"] = data["content"]
        if (rectified["content"] != null) {
            rectified["content"] = data.getString("content").trim()
        }

        val uid = MicroBlog.uidFromUserUrl(data.maybeFirstOfList("user_link"))
        val userUrl = "//weibo.com/$uid"
        collector.emit("user", Values(
                hashMapOf(
                        "uid" to uid,
                        "url" to userUrl
                ),
                uid))
        rectified["uid"] = uid
        rectified["mid"] = MicroBlog.url2codedMid(meta.getHashMap("user_data").getString("keyword"))
        val likeText = if (data["like"] != null) {
            data.maybeFirstOfList<String>("like").firstMatch(numberRegex)
        } else {
            null
        }
        rectified["like"] = if (likeText != null && likeText != "") {
            likeText.toInt()
        } else {
            0
        }

        collector.emit("comment", Values(rectified, cid))
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        declarer.declareStream("comment", Fields(DATA_FIELD_NAME, "id"))
        declarer.declareStream("user", Fields(DATA_FIELD_NAME, "uid"))
    }

}