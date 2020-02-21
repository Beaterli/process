package com.presisco.process.storm.bolt

import com.presisco.lazystorm.DATA_FIELD_NAME
import com.presisco.lazystorm.bolt.redis.JedisSingletonBolt
import com.presisco.lazystorm.mapToArrayList
import com.presisco.process.getString
import com.presisco.process.weibo.MicroBlog
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values

class TagIdentifyBolt: JedisSingletonBolt<Map<String, *>>() {

    override fun execute(tuple: Tuple, collector: BasicOutputCollector) {
        val blog = getInput(tuple)

        val content = blog.getString("content")
        val mid = blog.getString("mid")
        val tags = MicroBlog.extractTags(content)
        if (tags.isEmpty()){
            return
        }

        val cmd = getCommand()
        val blogWithTags = tags.mapToArrayList { tag_info ->
            val tag = tag_info.second
            var tid = cmd.hget("tag_id_map", tag)
            if (tid == null){
                val newIndex = cmd.incr("tag_id_gen").toString()
                tid = if(cmd.hsetnx("tag_id_map", tag, newIndex) == 1L){
                    newIndex
                }else{
                    cmd.hget("tag_id_map", tag)
                }
                collector.emit("new_tag", Values(hashMapOf(
                        "tid" to tid.toInt(),
                        "tag" to tag,
                        "type" to tag_info.first
                ), tid.toInt()))
            }
            hashMapOf(
                    "mid" to mid,
                    "tid" to tid.toInt()
            )
        }

        closeCommand(cmd)
        collector.emit("blog_with_tag", Values(blogWithTags, mid))
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        declarer.declareStream("new_tag", Fields(DATA_FIELD_NAME, "id"))
        declarer.declareStream("blog_with_tag", Fields(DATA_FIELD_NAME, "id"))
    }
}