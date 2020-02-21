package com.presisco.process.storm.bolt

import com.presisco.lazystorm.DATA_FIELD_NAME
import com.presisco.lazystorm.bolt.redis.JedisSingletonBolt
import com.presisco.process.UserState
import com.presisco.process.getString
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values

class UserIdentifyBolt: JedisSingletonBolt<Map<String, *>>() {

    override fun execute(tuple: Tuple, collector: BasicOutputCollector) {
        val userData = getInput(tuple)
        val cmd = getCommand()

        val uid = userData.getString("uid")
        val dataUserState = if (userData.containsKey("name")){
            UserState.NAME
        } else {
            UserState.UID
        }

        val cacheUserState = if (cmd.hexists("user_map", uid)) {
            cmd.hget("user_map", uid).toInt()
        } else {
            UserState.VOID
        }

        if (dataUserState > cacheUserState){
            cmd.hset("user_map", uid, dataUserState.toString())
            collector.emit("user", Values(userData, uid))
        }

        closeCommand(cmd)
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        declarer.declareStream("user", Fields(DATA_FIELD_NAME, "id"))
    }

}