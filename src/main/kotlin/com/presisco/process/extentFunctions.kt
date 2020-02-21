package com.presisco.process

import com.presisco.gsonhelper.MapHelper
import java.time.Instant
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.*

fun String.json2Map() = MapHelper().fromJson(this)

fun Set<String>.toLabels() = if (this.isEmpty()) {
    ""
} else {
    this.joinToString(prefix = ":", separator = ":") { "`$it`" }
}

fun <T> Map<*, T>.mergeKeySets(): Map<String, T> {
    val merged = hashMapOf<String, T>()
    this.forEach { fuzzyKey, value ->
        when (fuzzyKey) {
            is Collection<*> -> fuzzyKey.forEach { key ->
                merged[key as String] = value
            }
            else -> merged[fuzzyKey.toString()] = value
        }
    }
    return merged
}

fun Map<String, String>.toProperties(): Properties {
    val prop = Properties()
    prop.putAll(this)
    return prop
}

fun nowMs() = Instant.now().toEpochMilli()

fun <T> Map<String, *>.byType(key: String): T =
    if (this.containsKey(key)) this[key] as T else throw IllegalStateException("$key not defined in map")

fun Map<String, *>.getInt(key: String) = this.byType<Number>(key).toInt()

fun Map<String, *>.getLong(key: String) = this.byType<Number>(key).toLong()

fun Map<String, *>.getString(key: String) = this.byType<String>(key)

fun Map<String, *>.getBoolean(key: String) = this.byType<Boolean>(key)

fun <K, V> Map<String, *>.getMap(key: String) = this.byType<Map<K, V>>(key)

fun Map<String, *>.getHashMap(key: String) = this.byType<HashMap<String, Any?>>(key)

fun <E> Map<String, *>.getList(key: String) = this.byType<List<E>>(key)

fun <E> Map<String, *>.getArrayList(key: String) = this.byType<ArrayList<E>>(key)

fun Map<String, *>.getListOfMap(key: String) = this[key] as List<Map<String, *>>

fun Map<String, *>.getAsDouble(key: String) = this.byType<Double>(key)

fun <V> Map<*, V>.arrayListValues(): ArrayList<V>{
    val list = arrayListOf<V>()
    list.addAll(this.values)
    return list
}

fun Map<String, *>.addFieldToNewMap(pair: Pair<String, Any?>): HashMap<String, Any?> {
    val newMap = hashMapOf(pair)
    newMap.putAll(this)
    return newMap
}

fun <K, V> MutableMap<K, MutableList<V>>.addToValue(key: K, value: V) {
    if (!this.containsKey(key)) {
        this[key] = arrayListOf()
    }
    this[key]!!.add(value)
}

fun <V> List<Map<String, *>>.toFlattenMap(keyField: String, valueField: String): HashMap<String, V> {
    val map = hashMapOf<String, V>()
    this.forEach { item -> map[item.getString(keyField)] = item[valueField] as V }
    return map
}

fun List<Map<String, *>>.toFlattenStringMap(keyField: String, valueField: String): HashMap<String, String> {
    val map = hashMapOf<String, String>()
    this.forEach { item -> map[item.getString(keyField)] = item[valueField].toString() }
    return map
}

fun String.firstMatch(regex: Regex): String? {
    val match = regex.matchEntire(this)
    return if (match == null) {
        null
    } else {
        match.groupValues[1]
    }
}

fun String.extractValues(regex: Regex): List<String> {
    return regex.findAll(this).map { it.groupValues[1] }.toList()
}

val dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")!!

fun LocalDateTime.toFormatString() = this.format(dateTimeFormat)