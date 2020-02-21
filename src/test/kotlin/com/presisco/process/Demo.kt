package com.presisco.process

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.Before
import org.junit.Test

class Demo {

    val roots = arrayOf("Ib7ij2r9i", "IbjCn93Bs")
    val demoClient = MapJdbcClient(
            HikariDataSource(
                    HikariConfig(
                            mapOf(
                                    "dataSourceClassName" to "org.sqlite.SQLiteDataSource",
                                    "dataSource.url" to "jdbc:sqlite:D:/database/demo_records.db",
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
    val producer = KafkaProducer<String, String>(
            mapOf(
                    "bootstrap.servers" to "192.168.56.101:9092",
                    "acks" to "all",
                    "retires" to "0",
                    "key.serializer" to "org.apache.kafka.common.serialization.StringSerializer",
                    "value.serializer" to "org.apache.kafka.common.serialization.StringSerializer"
            )
    )

    @Before
    fun deleteEvents() {
        with(mainClient) {
            roots.forEach {
                executeSQL("delete from comment where mid in (select mid from blog where root_id = '$it')")
                executeSQL("delete from blog where root_id = '$it'")
                executeSQL("delete from root where mid = '$it'")
            }
        }
        println("removed all related data from main database!")
    }

    @Test
    fun produce() {
        val records = demoClient.buildSelect("*")
                .from("data")
                .execute()

        var counter = 0
        var finished = false
        val counterDisplay = Thread(Runnable {
            while (!finished) {
                Thread.sleep(1000)
                println("transferred $counter records")
                counter = 0
            }
        })
        counterDisplay.start()

        records.forEach {
            val json = it.getString("json")
            producer.send(ProducerRecord("scrappy", "weibo", json)).get().offset()
            counter++
        }
        println("records of root: ${roots.joinToString(separator = ", ")} are sent to kafka!")
        producer.close()
        finished = true
    }

}