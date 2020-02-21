package com.presisco.process

import com.presisco.gsonhelper.MapHelper
import com.presisco.lazyjdbc.client.MapJdbcClient
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.Test

class FakeScrappy {

    val scrappyClient = MapJdbcClient(
            HikariDataSource(
                    HikariConfig(
                            mapOf(
                                    "dataSourceClassName" to "org.sqlite.SQLiteDataSource",
                                    "dataSource.url" to "jdbc:sqlite:G:/scrappy/scrappy_dump.db",
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

    var counter = 0

    val interval = 0

    @Test
    fun feed(){
        val iterator = scrappyClient.selectIterator(1000, "select * from data")
        val mapHelper = MapHelper()
        val counterDisplay = Thread(Runnable {
            while (true) {
                Thread.sleep(1000)
                println("transferred $counter records")
                counter = 0
            }
        })
        counterDisplay.start()
        while (iterator.hasNext()) {
            val start = System.currentTimeMillis()
            val row = iterator.next()
            val json = mapHelper.toJson(row)
            val offset = producer.send(ProducerRecord("scrappy", "weibo", json)).get().offset()
            counter++
            val id = (row["id"] as Number).toInt()
            if(id % 100000 == 0){
                println("read@$id")
            }
            val wait = interval - (System.currentTimeMillis() - start)
            if (wait > 0) {
                Thread.sleep(wait)
            }
        }
        producer.close()
        counterDisplay.interrupt()
    }

}