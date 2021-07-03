package org.jetbrains.dataframe.io

import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.RowColFormatter
import org.jetbrains.dataframe.jupyter.CellRenderer
import org.jetbrains.dataframe.jupyter.ImageCellRenderer
import org.jetbrains.dataframe.size

internal const val tooltipLimit = 1000
internal fun getDefaultFooter(df: DataFrame<*>): String {
    return "DataFrame [${df.size}]"
}

public fun <T> DataFrame<T>.toHTML(
    configuration: DisplayConfiguration = DisplayConfiguration.DEFAULT,
    getFooter: (DataFrame<T>) -> String = ::getDefaultFooter,
    cellRenderer: CellRenderer = ImageCellRenderer,
): String = buildString {
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
            val tooltip = cellRenderer.tooltip(cellVal, configuration)
            val content = cellRenderer.content(cellVal, configuration)
            val attributes = configuration.cellFormatter?.invoke(row, col)?.attributes()?.joinToString(";") { "${it.first}:${it.second}" }.orEmpty()
            append("<td style=\"text-align:left;$attributes\" title=\"$tooltip\">$content</td>")
        }
        append("</tr>")
    }
    append("</table>")
    val footer = getFooter(this@toHTML)
    if (limit < nrow()) {
        append("<p>... $footer</p>")
    } else append("<p>$footer</p>")
    append("</body></html>")
}

public data class DisplayConfiguration(
    var rowsLimit: Int = 20,
    var cellContentLimit: Int = 40,
    var cellFormatter: RowColFormatter<*>? = null,
) {
    public companion object {
        public val DEFAULT: DisplayConfiguration = DisplayConfiguration()
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
