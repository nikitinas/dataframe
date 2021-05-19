package org.jetbrains.dataframe.io

import org.jetbrains.dataframe.AnyFrame
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.emptyDataFrame
import java.io.IOException
import java.io.InputStream
import java.lang.Exception
import java.net.URL

internal fun catchHttpResponse(url: URL, body: (InputStream) -> AnyFrame): AnyFrame {
    try {
        val stream = url.openStream()
        return body(stream)
    } catch (e: IOException) {
        if(e.message?.startsWith("Server returned HTTP response code") == true){
            val response = khttp.get(url.toString())
            try {
                return DataFrame.readJsonStr(response.text)
            }catch (e2: Exception){
                throw e
            }
        }
        throw e
    }
}