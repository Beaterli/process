package com.presisco.process

import com.presisco.gsonhelper.MapHelper
import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.lazyjdbc.condition
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.junit.Test

class BuildDemoData {

    val roots = listOf("Ib7ij2r9i", "IbjCn93Bs")
    val dumpClient = MapJdbcClient(
            HikariDataSource(
                    HikariConfig(
                            mapOf(
                                    "dataSourceClassName" to "org.sqlite.SQLiteDataSource",
                                    "dataSource.url" to "jdbc:sqlite:E:/database/scrappy_dump.db",
                                    "maximumPoolSize" to "1"
                            ).toProperties()
                    )
            )
    )
    val demoClient = MapJdbcClient(
            HikariDataSource(
                    HikariConfig(
                            mapOf(
                                    "dataSourceClassName" to "org.sqlite.SQLiteDataSource",
                                    "dataSource.url" to "jdbc:sqlite:E:/database/demo_records.db",
                                    "maximumPoolSize" to "1"
                            ).toProperties()
                    )
            )
    )
    val mainClient = MapJdbcClient(
            HikariDataSource(
                    HikariConfig(
                            mapOf(
                                    "dataSourceClassName" to "com.mysql.cj.jdbc.MysqlDataSource",
                                    "dataSource.url" to "jdbc:mysql://localhost:3306/weibo?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC",
                                    "dataSource.user" to "root",
                                    "dataSource.password" to "experimental",
                                    "maximumPoolSize" to "1"
                            ).toProperties()
                    )
            )
    )

    @Test
    fun produce() {
        val mids = hashSetOf<String>()
        mainClient.buildSelect("mid")
                .from("blog")
                .where(condition("root_id", "in", roots))
                .execute()
                .forEach { mids.add(it["mid"] as String) }
        println("watching ${mids.size} mids")
        demoClient.executeSQL("create table if not exists data(json text not null)")
        val itr = dumpClient.selectIterator(
                10000,
                "select * from data where createtime like '2019-10-1%' and version in ('repost', 'search1', 'comment1')"
        )
        val mapHelper = MapHelper()
        val demoSet = arrayListOf<Map<String, *>>()
        var counter = 0
        while (itr.hasNext()) {
            val record = itr.next()
            counter++
            if (counter % 100000 == 0) {
                println("scanned $counter records")
            }

            val version = record.getString("version")

            val data = record.getString("data")
            val meta = record.getString("meta")
            var contains = false
            if (version == "comment1") {
                for (mid in mids) {
                    if (meta.contains(mid)) {
                        contains = true
                    }
                }
                if (!contains) {
                    continue
                }
            } else {
                for (mid in mids) {
                    if (data.contains(mid)) {
                        contains = true
                    }
                }
                if (!contains) {
                    continue
                }
            }

            val json = mapHelper.toJson(record)
            demoSet.add(mapOf("json" to json))
        }
        demoClient.insert("data", demoSet)
        println("complete!")
    }

}