package org.jetbrains.dataframe.io

import org.jetbrains.dataframe.*
import java.io.File
import java.net.URL

enum class SupportedFormats {
    CSV,
    JSON
}

internal fun guessFormat(file: File): SupportedFormats? = when(file.extension.toLowerCase()){
    "json" -> SupportedFormats.JSON
    "csv" -> SupportedFormats.CSV
    else -> null
}

internal fun guessFormat(url: URL): SupportedFormats? = guessFormat(url.path)

internal fun guessFormat(url: String): SupportedFormats? = when{
    url.endsWith(".csv") -> SupportedFormats.CSV
    url.endsWith(".json") -> SupportedFormats.JSON
    else -> null
}

fun DataFrame.Companion.read(file: File): AnyFrame = when(guessFormat(file)) {
    SupportedFormats.CSV -> readCSV(file)
    SupportedFormats.JSON -> readJson(file)
    else -> try {
        readCSV(file)
    } catch(e: Exception) {
        readJson(file)
    }
}

fun DataFrame.Companion.read(url: URL): AnyFrame = when(guessFormat(url)) {
    SupportedFormats.CSV -> readCSV(url)
    SupportedFormats.JSON -> readJson(url)
    else -> try {
        readCSV(url)
    } catch(e: Exception) {
        readJson(url)
    }
}

fun DataFrame.Companion.read(path: String): AnyFrame = when(guessFormat(path)) {
    SupportedFormats.CSV -> readCSV(path)
    SupportedFormats.JSON -> readJson(path)
    else -> try {
        readCSV(path)
    } catch(e: Exception) {
        readJson(path)
    }
}
