package com.presisco.process.storm.bolt

import com.presisco.lazystorm.DATA_FIELD_NAME
import com.presisco.lazystorm.bolt.LazyBasicBolt
import com.presisco.process.*
import com.presisco.process.weibo.MicroBlog
import com.presisco.process.weibo.MicroBlog.alignTime
import com.presisco.process.weibo.MicroBlog.nicknameRegex
import com.presisco.process.weibo.MicroBlog.numberRegex
import com.presisco.process.weibo.MicroBlog.timeFromXml
import com.presisco.process.weibo.MicroBlog.timeFromXmlText
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values

class BlogParseBolt: LazyBasicBolt<Map<String, *>>() {

    fun Map<String, *>.intOrZero(key: String): Int {
        return if (this[key] != null) {
            val number = this.getString(key).firstMatch(numberRegex)
            if (number == null) {
                0
            } else {
                number.toInt()
            }
        } else {
            0
        }
    }

    override fun execute(tuple: Tuple, collector: BasicOutputCollector) {
        val input = getInput(tuple)

        val scrapTime = input.getString("createtime")
        val data = input.getMap<String, Any?>("data")
        val meta = input.getMap<String, Any?>("meta")

        val rectified = hashMapOf<String, Any?>()
        if (data["url"] == null) {
            return
        }
        rectified["url"] = data["url"]
        rectified["repost"] = data.intOrZero("repost")
        rectified["comment"] = data.intOrZero("comment")
        rectified["like"] = data.intOrZero("like")
        if (data["content"] != null) {
            rectified["content"] = data.getString("content").trim()
        }
        if (data["text"] != null) {
            rectified["content"] = data.getString("text").trim()
        }
        val mid = MicroBlog.url2codedMid(data["url"] as String)
        if (mid == "") {
            return
        }

        rectified["mid"] = mid

        if (data.containsKey("create_time")) {
            val timeString = data["create_time"] as String?
            if (timeString == null) {
                rectified["time"] = null
            } else if (timeString.contains("<div")) {
                rectified["time"] = data.getString("create_time").extractValues(timeFromXml)
                        .first()
                        .trim()
            } else {
                rectified["time"] = timeString
            }
        } else if (data.containsKey("time")) {
            val timeString = data.getString("time").replace("\n", "")
            if (timeString.contains("<a")) {
                rectified["time"] = timeString.extractValues(timeFromXmlText)
                        .first()
                        .substringBefore(" 转赞人数")
                        .trim()
            } else {
                rectified["time"] = timeString.trim()
            }
        } else {
            rectified["time"] = null
        }

        if (rectified["time"] != null) {
            rectified["time"] = alignTime(scrapTime, rectified.getString("time"))
        }

        rectified["scrap_time"] = scrapTime

        collector.emitDataToStreams("tag", hashMapOf(
                "mid" to mid,
                "content" to rectified["content"]
        ))

        val uid = MicroBlog.uidFromBlogUrl(data["url"] as String)
        val userUrl = "//weibo.com/$uid"
        rectified["uid"] = uid
        val username = if (data["name"] != null) {
            if (data.getString("name").contains("<a ")) {
                data.getString("name").extractValues(nicknameRegex).first().trim()
            } else {
                data.getString("name").trim()
            }
        } else if (data["user"] != null) {
            data.getString("user").trim()
        } else {
            null
        }

        collector.emit("user", Values(
                hashMapOf(
                        "uid" to uid,
                        "name" to username,
                        "url" to userUrl
                ),
                uid
        ))

        val source = meta.getHashMap("user_data").getString("keyword")
        if (source.contains("//weibo.com")) {
            rectified["repost_link"] = source
            val repostId = MicroBlog.url2codedMid(source)
            rectified["repost_id"] = repostId
        } else {
            rectified["repost_link"] = null
            rectified["repost_id"] = null
            collector.emit("root", Values(hashMapOf(
                    "mid" to mid,
                    "keyword" to source
            ), mid))
        }

        collector.emit("blog", Values(rectified, mid))
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        declarer.declareStream("blog", Fields(DATA_FIELD_NAME, "id"))
        declarer.declareStream("user", Fields(DATA_FIELD_NAME, "uid"))
        declarer.declareStream("tag", Fields(DATA_FIELD_NAME))
        declarer.declareStream("root", Fields(DATA_FIELD_NAME, "id"))
    }

}