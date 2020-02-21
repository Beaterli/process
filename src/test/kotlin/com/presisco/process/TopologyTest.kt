package com.presisco.process

import org.junit.Test

class TopologyTest {

    @Test
    fun validate() {
        Entrance.main(
                arrayOf(
                        "config=sample/debug-config.json",
                        "mode=local"
                )
        )
    }

}