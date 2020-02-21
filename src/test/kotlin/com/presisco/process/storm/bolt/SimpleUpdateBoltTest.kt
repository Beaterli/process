package com.presisco.process.storm.bolt

import com.presisco.lazystorm.test.LazyBasicBoltTest
import com.presisco.lazystorm.test.SimpleDataTuple
import com.presisco.process.Entrance
import org.junit.Before
import org.junit.Test

class SimpleUpdateBoltTest: LazyBasicBoltTest(Entrance, "sample/config.json", "update") {

    @Before
    fun prepare() {
        fakeEmptyPrepare()
    }

    @Test
    fun updateTest() {
        val inputTuple = SimpleDataTuple(listOf("data"), listOf(arrayListOf(
                hashMapOf(
                        "cid" to "1",
                        "like" to 1,
                        "scrap_time" to "now"
                ),
                hashMapOf(
                        "cid" to "2",
                        "like" to 2,
                        "scrap_time" to "next"
                )
        )), "update_window", "update_comment")
        val collector = fakeBasicOutputCollector()
        bolt.execute(inputTuple, collector)
    }

}