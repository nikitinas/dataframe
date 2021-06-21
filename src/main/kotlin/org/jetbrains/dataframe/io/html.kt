package org.jetbrains.dataframe.io

import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.RowColFormatter
import org.jetbrains.dataframe.images.Image
import org.jetbrains.dataframe.impl.truncate
import org.jetbrains.dataframe.size

internal val tooltipLimit = 1000

fun <T> DataFrame<T>.toHTML(configuration: DisplayConfiguration = DisplayConfiguration.DEFAULT, getFooter: (DataFrame<T>)->String = {"DataFrame [${it.size}]" }) = buildString {
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
                    tooltip = renderValueForHtml(cellVal, tooltipLimit)
                    content = renderValueForHtml(cellVal, configuration.cellContentLimit)
                }
            }
            val attributes = configuration.cellFormatter?.invoke(row, col)?.attributes()?.joinToString(";") { "${it.first}:${it.second}" }.orEmpty()
            append("<td style=\"text-align:left;$attributes\" title=\"$tooltip\">$content</td>")
        }
        append("</tr>")
    }
    append("</table>")
    val footer = getFooter(this@toHTML)
    if (limit < nrow())
        append("<p>... $footer</p>")
    else append("<p>$footer</p>")
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

internal fun String.escapeNewLines() = replace("\n", "\\n")

internal fun String.escapeHTML(): String {
    val str = this
    return buildString {
        for (c in str) {
            if (c.code > 127 || c == '"' || c == '\'' || c == '<' || c == '>' || c == '&') {
                append("&#")
                append(c.code)
                append(';')
            } else {
                append(c)
            }
        }
    }
}
