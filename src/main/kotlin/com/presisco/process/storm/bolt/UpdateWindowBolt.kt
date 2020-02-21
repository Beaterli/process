package com.presisco.process.storm.bolt

import com.presisco.lazystorm.bolt.LazyWindowedBolt
import com.presisco.process.arrayListValues
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values
import org.apache.storm.windowing.TupleWindow

class UpdateWindowBolt : LazyWindowedBolt<Map<String, *>>() {

    override fun execute(window: TupleWindow) {
        val streamUpdateQueue = hashMapOf<String, HashMap<Any?, Any?>>()

        window.get().forEach { tuple ->
            val fromStream = tuple.sourceStreamId
            val id = tuple.getValue(1)
            val input = tuple.getValue(0)

            if (!streamUpdateQueue.containsKey(fromStream)) {
                streamUpdateQueue[fromStream] = hashMapOf()
            }

            streamUpdateQueue[fromStream]!![id] = input
        }
        streamUpdateQueue.forEach { (stream, queue) ->
            collector.emit(stream, Values(queue.arrayListValues()))
        }
    }

}