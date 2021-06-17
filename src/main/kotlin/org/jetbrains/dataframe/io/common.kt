package org.jetbrains.dataframe.io

import org.jetbrains.dataframe.AnyFrame
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.Many
import org.jetbrains.dataframe.columns.guessColumnType
import org.jetbrains.dataframe.emptyDataFrame
import org.jetbrains.dataframe.toDataFrame
import java.io.IOException
import java.io.InputStream
import java.lang.Exception
import java.net.URL

internal fun catchHttpResponse(url: URL, body: (InputStream) -> AnyFrame): AnyFrame {
    try {
        val stream = url.openStream()
        return body(stream)
    } catch (e: IOException) {
        if (e.message?.startsWith("Server returned HTTP response code") == true) {
            val response = khttp.get(url.toString())
            try {
                return DataFrame.readJsonStr(response.text)
            } catch (e2: Exception) {
                throw e
            }
        }
        throw e
    }
}

fun <T> Many<Many<T>>.toDataFrame(containsColumns: Boolean = false): AnyFrame = when {
    containsColumns -> {
        mapNotNull {
            if (it.size == 0) null
            val name = it[0].toString()
            val values = it.drop(1)
            guessColumnType(name, values)
        }.toDataFrame()
    }
    size == 0 -> emptyDataFrame(0)
    else -> {
        val header = get(0).map { it.toString() }
        val data = drop(1)
        header.mapIndexed { colIndex, name ->
            val values = data.map { row ->
                if(row.size <= colIndex) null
                else row[colIndex]
            }
            guessColumnType(name, values)
        }.toDataFrame()
    }
}
