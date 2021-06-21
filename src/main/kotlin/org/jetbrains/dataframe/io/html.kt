package org.jetbrains.dataframe.io

import org.jetbrains.dataframe.AnyFrame
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.RowColFormatter
import org.jetbrains.dataframe.images.Image
import org.jetbrains.dataframe.size

internal val tooltipLimit = 1000

fun <T> DataFrame<T>.toHTML(configuration: DisplayConfiguration = DisplayConfiguration.DEFAULT, getFooter: (DataFrame<T>)->String = {"DataFrame [${it.size}]" }) = buildString {

    val df = this@toHTML
    val nullClassName = "null"
    val expanderClassName = "expander"
    val head = """<head><style type="text/css">
            div.$nullClassName{
                color: #b3b3cc;
            }
            a.$expanderClassName {
                cursor: pointer;
            }
        </style></head>""".trimIndent()
    append("<html>$head<body>")
    val limit = configuration.rowsLimit

    val expandedDataFrames = mutableMapOf<String, String>()
    var expadedDataFrameId = 1

    fun renderTable(df:AnyFrame, header: Boolean = true): String = buildString {
        append("<table>")
        if(header) {
            append("<tr>")
            df.columns().forEach {
                append("<th style=\"text-align:left\">${it.name()}</th>")
            }
        }
        append("</tr>")
        df.rows().take(limit).forEach { row ->
            append("<tr>")
            df.columns().forEach { col ->
                val cellVal = row[col]
                val tooltip: String
                val content: String
                when (cellVal) {
                    is Image -> {
                        tooltip = cellVal.url
                        content = "<img src=\"${cellVal.url}\"/>"
                    }
                    else -> {
                        tooltip = renderValueForHtml(cellVal, tooltipLimit)
                        if(cellVal is AnyFrame) {
                            val id = "df" + expadedDataFrameId++
                            val expanded = renderTable(cellVal, true)
                            expandedDataFrames[id] = expanded.replace("\"", "\\\"")
                            val collapsed = "[${cellVal.size}]"
                            content = """<div id="$id"><a class="$expanderClassName" onClick="expand('$id');">$collapsed</a></div>"""
                        }
                        else content = renderValueForHtml(cellVal, configuration.cellContentLimit, nullClassName)
                    }
                }
                val attributes = configuration.cellFormatter?.invoke(row, col)?.attributes()
                    ?.joinToString(";") { "${it.first}:${it.second}" }.orEmpty()
                append("<td style=\"text-align:left;$attributes\" title=\"$tooltip\">$content</td>")
            }
            append("</tr>")
        }
        append("</table>")
    }
    append(renderTable(df))

    val footer = getFooter(df)
    if (limit < nrow())
        append("<p>... $footer</p>")
    else append("<p>$footer</p>")
    append("</body>")

    val script = """<script type="text/javascript">
      var data = {
         ${expandedDataFrames.map { """${it.key}: "${it.value}",""" }.joinToString("\n") }
      }
      function expand(id) {
        document.getElementById(id).innerHTML = data[id]
      }
    </script>"""
    append(script)
    append("</html>")
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
