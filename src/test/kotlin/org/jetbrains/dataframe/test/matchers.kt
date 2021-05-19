package org.jetbrains.dataframe.test

import io.kotest.matchers.Matcher
import io.kotest.matchers.MatcherResult
import io.kotest.matchers.neverNullMatcher

fun containNTimes(substring: String, n: Int): Matcher<String?> = neverNullMatcher { value ->
    var start = 0
    val positiveMessage = "String should contain `$substring` exactly $n times"
    val negativeMessage = "String should contain `$substring` more or less than $n times"
    fun result(passed: Boolean) = MatcherResult(passed, positiveMessage, negativeMessage)
    for (k in 0 until n) {
        val i = value.indexOf(substring, start)
        if (i == -1) return@neverNullMatcher result(false)
        start = i + substring.length
    }

    val i = value.indexOf(substring, start)
    result(i == -1)
}
