package com.presisco.process

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.lazystorm.Launch
import com.presisco.lazystorm.connector.DataSourceLoader
import com.presisco.lazystorm.connector.JedisPoolLoader
import com.presisco.lazystorm.connector.LoaderManager
import com.presisco.lazystorm.topology.LazyTopoBuilder
import redis.clients.jedis.Jedis

object Entrance: Launch() {

    override val createCustomBolt = { name: String, _: Map<String, Any?> ->
        throw IllegalStateException("unsupported bolt name: $name")
    }
    override val createCustomSpout = { name: String, _: Map<String, Any?> -> throw IllegalStateException("unsupported spout name: $name") }

    val prepareRedis = {
        builder: LazyTopoBuilder ->
        val jdbc = MapJdbcClient(LoaderManager.getLoader<DataSourceLoader>("data_source", "mysql").getConnector())
        val cmd = LoaderManager.getLoader<JedisPoolLoader>("redis", "singleton").getConnector().resource
        cmd.flushDB()

        fun Jedis.hmsetNotEmpty(key:String, map: Map<String, String>){
            if(map.isNotEmpty()){
                this.hmset(key, map)
            }
        }

        val uids = hashMapOf<String, String>()
        jdbc.select("select * from blog_user")
                .forEach {
                    val uid = it.getString("uid")
                    uids[uid] = if(it["name"] == null){
                        UserState.UID.toString()
                    } else {
                        UserState.NAME.toString()
                    }
                }
        cmd.hmsetNotEmpty("user_map", uids)

        val tids = hashMapOf<String, String>()
        var maxId = 0
        jdbc.select("select * from tag")
                .forEach {
                    val tag = it.getString("tag")
                    val tid = it.getInt("tid")
                    tids[tag] = tid.toString()
                    if(tid > maxId){
                        maxId = tid
                    }
                }
        cmd.hmsetNotEmpty("tag_id_map", tids)
        cmd.setnx("tag_id_gen", maxId.toString())

        cmd.hmsetNotEmpty("blog_map", jdbc.select("select mid, scrap_time from blog").toFlattenStringMap("mid", "scrap_time"))
        cmd.hmsetNotEmpty("comment_map", jdbc.select("select cid, scrap_time from comment").toFlattenStringMap("cid", "scrap_time"))
        cmd.hmsetNotEmpty("root_map", jdbc.select("select mid, keyword from root").toFlattenStringMap("mid", "keyword"))

        cmd.close()
    }

    /**
     * Topology入口
     *
     * @param args 命令行参数
     */
    @JvmStatic
    fun main(args: Array<String>) {
        launch(args, prepareRedis)
    }
}