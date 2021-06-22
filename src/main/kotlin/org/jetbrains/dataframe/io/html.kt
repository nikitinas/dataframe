package org.jetbrains.dataframe.io

import org.jetbrains.dataframe.AnyFrame
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.RowColFormatter
import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.ColumnGroup
import org.jetbrains.dataframe.images.Image
import org.jetbrains.dataframe.isNumber
import org.jetbrains.dataframe.size
import org.jetbrains.kotlinx.jupyter.api.HTML
import java.io.InputStreamReader

internal val tooltipLimit = 1000

internal val nullClassName = "null"
internal val expanderClassName = "expander"

internal data class ColumnDataForJs(val name: String, val nested: List<ColumnDataForJs>, val rightAlign: Boolean, val values: List<String>)

internal fun getResourceText(resource: String): String {
    val res = DataFrame::class.java.getResourceAsStream(resource) ?: error("Resource '$resource' not found")
    return InputStreamReader(res).readText()
}

internal fun tableJs(columns: List<ColumnDataForJs>, id: Int): String {
    val template = getResourceText("/table.js")
    var index = 0
    val data = buildString {
        append("[")
        fun dfs(col: ColumnDataForJs): Int {
            val children = col.nested.map { dfs(it) }
            val colIndex = index++
            val values = col.values.joinToString(",", prefix = "[", postfix = "]") {
                "\"" + it.escapeQuotes() + "\""
            }
            append("{ name: \"${col.name.escapeQuotes()}\", children: $children, rightAlign: ${col.rightAlign}, values: $values }, \n")
            return colIndex
        }
        columns.forEach { dfs(it) }
        append("]")
    }
    return template.replace("COLUMNS", data).replace("ID", id.toString())
}

internal var tableId = 0

internal fun tableJs(df: AnyFrame, id: Int, configuration: DisplayConfiguration = DisplayConfiguration.DEFAULT, limit: Int): String {

    fun AnyCol.toJs(): ColumnDataForJs {
        val values = values().take(limit).map { renderValueForHtml(it, configuration.cellContentLimit, nullClassName)}
        return ColumnDataForJs(name(), if (this is ColumnGroup<*>) columns().map { it.toJs() } else emptyList(), isNumber(), values)
    }

    val data = df.columns().map { it.toJs() }
    return tableJs(data, id)
}

internal fun renderTable(df: AnyFrame, configuration: DisplayConfiguration = DisplayConfiguration.DEFAULT, limit: Int): String = buildString {
    append("<table>")

    // region header
    append("<thead>")
    append("<tr>")
    df.columns().forEach {
        append("<th style=\"text-align:left\">${it.name()}</th>")
    }
    append("</tr>")
    append("</thead>")
    // endregion

    append("<tbody>")
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
       /*             if (cellVal is AnyFrame) {
                        val id = "df" + expandedDataFrameId++
                        val expanded = renderTable(cellVal)
                        expandedDataFrames[id] = expanded.replace("\"", "\\\"")
                        val collapsed = "[${cellVal.size}]"
                        content =
                            """<div id="$id"><a class="$expanderClassName" onClick="expand('$id');">$collapsed</a></div>"""
                    } else  */
                        content = renderValueForHtml(cellVal, configuration.cellContentLimit, nullClassName)
                }
            }
            val attributes = configuration.cellFormatter?.invoke(row, col)?.attributes()
                ?.joinToString(";") { "${it.first}:${it.second}" }.orEmpty()

            append("<td style=\"text-align:left;$attributes\" title=\"$tooltip\">$content</td>")
        }
        append("</tr>")
    }
    append("</tbody>")
    append("</table>")
}

data class HtmlData(val style: String, val body: String, val script: String){
    override fun toString() = """
        <html>
        <head>
            <style type="text/css">
                $style
            </style>
        </head>
        <body>
            $body
        </body>
        <script>
            $script
        </script>
        </html>
    """.trimIndent()

    fun toJupyter() = HTML(toString())

    operator fun plus(other: HtmlData) = HtmlData(style + other.style, body + other.body, script + other.script)
}

internal fun initHtml() : HtmlData = HtmlData(style = getResourceText("/table.css"), script = getResourceText("/init.js"), body="")

fun <T> DataFrame<T>.toHTML(
    configuration: DisplayConfiguration = DisplayConfiguration.DEFAULT,
    includeInit: Boolean = false,
    getFooter: (DataFrame<T>) -> String = { "DataFrame [${it.size}]" }
): HtmlData {

    val df = this@toHTML

    val id = tableId++

    val style = """
            div.$nullClassName{
                color: #b3b3cc;
            }
            a.$expanderClassName {
                cursor: pointer;
            }
        """.trimIndent()

    // region body

    val limit = configuration.rowsLimit

    val expandedDataFrames = mutableMapOf<String, String>()
    var expadedDataFrameId = 1

    var body = getResourceText("/table.html").replace("ID", id.toString())

    val footer = getFooter(df)
    if (limit < nrow())
        body += "<p>... $footer</p>"
    else body += "<p>$footer</p>"

    // endregion

    // region script

    val script = tableJs(df, id, configuration, limit)

    /*
    """<script type="text/javascript">
      var data = {
         ${expandedDataFrames.map { """${it.key}: "${it.value}",""" }.joinToString("\n")}
      }
      function expand(id) {
        document.getElementById(id).innerHTML = data[id]
      } 
    </script>""" */

    // endregion

    val html = HtmlData(style, body, script)
    return if(includeInit) initHtml() + html
    else html
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

internal fun String.escapeQuotes() = replace("\"", "\\\"")

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
