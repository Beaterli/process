package com.presisco.process.storm.bolt

import com.presisco.lazystorm.bolt.LazyWindowedBolt
import com.presisco.process.arrayListValues
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values
import org.apache.storm.windowing.TupleWindow

class WriteWindowBolt : LazyWindowedBolt<Map<String, *>>() {

    override fun execute(window: TupleWindow) {
        val streamWriteQueue = hashMapOf<String, HashMap<Any?, Any?>>()
        window.get().forEach { tuple ->
            val fromStream = tuple.sourceStreamId
            val id = tuple.getValue(1)
            val input = tuple.getValue(0)

            if (!streamWriteQueue.containsKey(fromStream)) {
                streamWriteQueue[fromStream] = hashMapOf()
            }

            streamWriteQueue[fromStream]!![id] = input
        }
        streamWriteQueue.forEach { (stream, queue) ->
            val list = arrayListOf<Map<*, *>>()
            queue.values.forEach {
                if(it is Map<*, *>) {
                    list.add(it)
                } else {
                    list.addAll(it as List<Map<*, *>>)
                }
            }
            collector.emit(stream, Values(list))
        }
    }

}