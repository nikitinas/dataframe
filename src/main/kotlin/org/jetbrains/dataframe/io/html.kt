package org.jetbrains.dataframe.io

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.images.Image
import org.jetbrains.dataframe.impl.truncate

fun <T> DataFrame<T>.toHTML(configuration: DisplayConfiguration = DisplayConfiguration.DEFAULT) = buildString {
    append("<html><body>")
    append("<table><tr>")
    columns().forEach {
        append("<th style=\"text-align:left\">${it.name()}</th>")
    }
    append("</tr>")
    val limit = configuration.rowsLimit
    rows().take(limit).forEach { row ->
        append("<tr>")
        columns().forEach { col ->
            val cellVal = row[col]
            val tooltip: String
            val content: String
            when(cellVal) {
                is Image -> {
                    tooltip = cellVal.url
                    content = "<img src=\"${cellVal.url}\"/>"
                }
                else -> {
                    tooltip = renderValue(cellVal)
                    content = tooltip.truncate(configuration.cellContentLimit)
                }
            }
            val attributes = configuration.cellFormatter?.invoke(row, col)?.attributes()?.joinToString(";") { "${it.first}:${it.second}" }.orEmpty()
            append("<td style=\"text-align:left;$attributes\" title=\"$tooltip\">$content</td>")
        }
        append("</tr>")
    }
    append("</table>")
    if (limit < nrow())
        append("<p>... only showing top $limit of ${nrow()} rows</p>")
    append("</body></html>")
}

data class DisplayConfiguration(
    var rowsLimit: Int = 20,
    var cellContentLimit: Int = 40,
    var cellFormatter: RowColFormatter<*>? = null,
) {
    companion object {
        val DEFAULT = DisplayConfiguration()
    }
}
