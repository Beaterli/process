package com.presisco.process.weibo

import com.presisco.process.extractValues
import com.presisco.process.firstMatch
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object MicroBlog {
    val validTimeRegex = "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}".toRegex()

    val blogUrlRegex = "//weibo\\.com/(.+?)/([A-Za-z0-9]{9}).*".toRegex()
    val userUrlIdRegex = "//weibo\\.com/([A-Za-z0-9/]*)".toRegex()
    val decimalMidRange = listOf(0..1, 2..8, 9..15)
    val codedMidRanges = listOf(0..0, 1..4, 5..8)
    val customRadixTable = listOf(
        '0', '1', '2', '3', '4', '5', '6',
        '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
        'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w',
        'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
        'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W',
        'X', 'Y', 'Z'
    )
    val customRadix = customRadixTable.size

    fun String.fromCustomBase(): Long {
        var value = 0L
        this.forEach {
            value *= customRadix
            value += customRadixTable.indexOf(it)
        }
        return value
    }

    fun String.toCustomBase(): String {
        var value = this.toLong()

        val bits = arrayListOf<Char>()
        while (value > 0) {
            val bitValue = (value % customRadix).toInt()
            bits.add(0, customRadixTable[bitValue])
            value /= customRadix
        }

        return bits.joinToString(separator = "")
    }

    fun url2codedMid(url: String): String {
        val matchResult = blogUrlRegex.findAll(url).iterator()
        if (!matchResult.hasNext()) {
            return ""
        }
        val codedMid = matchResult.next().groupValues[2]
        return codedMid
    }

    fun url2mid(url: String): Long {
        val codedMid = url2codedMid(url)
        val midBuilder = StringBuilder()
        codedMidRanges.forEach { midBuilder.append(codedMid.substring(it).fromCustomBase()) }
        return midBuilder.toString().toLong()
    }

    fun uidFromBlogUrl(url: String): String {
        val matchResult = blogUrlRegex.findAll(url).iterator()
        if (!matchResult.hasNext()) {
            return ""
        }
        val uid = matchResult.next().groupValues[1]
        return uid
    }

    fun encodeMid(value: String): String {
        val codeBuilder = StringBuilder()
        decimalMidRange.forEach { codeBuilder.append(value.substring(it).toCustomBase()) }
        return codeBuilder.toString()
    }

    fun decodeMid(value: String) = value.fromCustomBase()

    fun uidFromUserUrl(url: String): String {
        val matchResult = userUrlIdRegex.findAll(url).first()
        val uid = matchResult.groupValues[1]
        return uid.replace("u/", "")
    }

    fun isValidTime(timeString: String) = validTimeRegex.matches(timeString)

    val minuteRegex = Regex("(\\d+)分钟.+?")
    val hourRegex = Regex("(\\d+)小时.+?")
    val hourAndMinuteDay = Regex("今天\\s?(\\d{2}):(\\d{2}).*")
    val monthAndDayRegex = Regex("(\\d+)月(\\d+)日 (\\d{2}):(\\d{2}).*")
    val yearAndMonthRegex = Regex("(\\d+)-(\\d+)-(\\d+) (\\d+):(\\d+).*")

    val scrapTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    val timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")

    fun String.toSystemMs() = LocalDateTime.parse(this, scrapTimeFormatter)

    fun alignTime(scrapTime: String, time: String): String {
        val scrapDateTime = scrapTime.toSystemMs()

        return if (time.contains("刚刚")) {
            timeFormatter.format(scrapDateTime)
        } else if (time.contains("秒")) {
            timeFormatter.format(scrapDateTime)
        } else if (time.contains("分钟前")) {
            timeFormatter.format(
                    scrapDateTime.minusMinutes(
                            time.firstMatch(minuteRegex)!!.toLong()
                    )
            )
        } else if (time.contains("小时前")) {
            timeFormatter.format(
                    scrapDateTime.minusHours(
                            time.firstMatch(hourRegex)!!.toLong()
                    )
            )
        } else if (time.contains("今天")) {
            val values = hourAndMinuteDay.matchEntire(time)!!.groupValues
            timeFormatter.format(
                    scrapDateTime.withHour(values[1].toInt())
                            .withMinute(values[2].toInt())
            )
        } else if (time.contains("年")) {
            time.replace("年", "-")
                    .replace("月", "-")
                    .replace("日", "")
        } else if (time.contains(monthAndDayRegex)) {
            val values = monthAndDayRegex.matchEntire(time)!!.groupValues
            timeFormatter.format(
                    scrapDateTime.withMonth(values[1].toInt())
                            .withDayOfMonth(values[2].toInt())
                            .withHour(values[3].toInt())
                            .withMinute(values[4].toInt())
            )
        } else if (time.contains(yearAndMonthRegex)) {
            val values = yearAndMonthRegex.matchEntire(time)!!.groupValues
            timeFormatter.format(
                    scrapDateTime.withYear(values[1].toInt())
                            .withMonth(values[2].toInt())
                            .withDayOfMonth(values[3].toInt())
                            .withHour(values[4].toInt())
                            .withMinute(values[5].toInt())
            )
        } else {
            time
        }
    }

    val numberRegex = Regex(".*?([0-9]+).*")
    val timeFromXml = Regex("title=\"(.+?)\"")
    val timeFromXmlText = Regex(">(.+?)</")
    val nicknameRegex = Regex("nick-name=\"(.+?)\" ")
    val quoteUserRegex = Regex("(@[^:\\s]+)")
    val topicRegex = Regex("(#.+?#)")

    fun extractTags(content: String): List<Pair<String, String>> {
        val topContent = content.substringBefore("//@")
        val tags = arrayListOf<Pair<String, String>>()
        tags.addAll(topContent.extractValues(quoteUserRegex).map { "user" to it })
        tags.addAll(topContent.extractValues(topicRegex).map { "topic" to it })
        return tags
    }

}