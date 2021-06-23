package org.jetbrains.dataframe.io

import org.jetbrains.dataframe.AnyFrame
import org.jetbrains.dataframe.AnyMany
import org.jetbrains.dataframe.AnyRow
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.DataFrameSize
import org.jetbrains.dataframe.Many
import org.jetbrains.dataframe.RowColFormatter
import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.ColumnGroup
import org.jetbrains.dataframe.index
import org.jetbrains.dataframe.io.DataFrameFormatter.Companion.withClass
import org.jetbrains.dataframe.isSubtypeOf
import org.jetbrains.dataframe.owner
import org.jetbrains.dataframe.size
import org.jetbrains.kotlinx.jupyter.api.HTML
import java.io.InputStreamReader
import java.lang.Appendable
import java.lang.StringBuilder
import java.util.LinkedList

internal val tooltipLimit = 1000

internal val nullClassName = "null"

internal data class DataFrameReference(val dfId: Int, val size: DataFrameSize)

internal data class ColumnDataForJs(val name: String, val nested: List<ColumnDataForJs>, val rightAlign: Boolean, val values: List<Any>)

internal val formatter = DataFrameFormatter("formatNull", "formatCurlyBrackets", "formatNumbers", "formatDataframes", "formatComma", "formatColumnNames", "formatSquareBrackets")

internal fun getResources(vararg resource: String) = resource.joinToString(separator = "\n") { getResourceText(it) }

internal fun getResourceText(resource: String, vararg replacement: Pair<String, Any>): String {
    val res = DataFrame::class.java.getResourceAsStream(resource) ?: error("Resource '$resource' not found")
    var template = InputStreamReader(res).readText()
    replacement.forEach {
        template = template.replace(it.first, it.second.toString())
    }
    return template
}

internal fun tableJs(columns: List<ColumnDataForJs>, id: Int): String {
    var index = 0
    val data = buildString {
        append("[")
        fun dfs(col: ColumnDataForJs): Int {
            val children = col.nested.map { dfs(it) }
            val colIndex = index++
            val values = col.values.joinToString(",", prefix = "[", postfix = "]") {
                when(it) {
                    is String -> "\"" + it.escapeForHtmlInJs() + "\""
                    is DataFrameReference -> {
                        val text = "DataFrame [${it.size}]"
                        "{ frameId: ${it.dfId}, value: \"$text\" }"
                    }
                    else -> error("Unsupported value type: ${it.javaClass}")
                }
            }
            append("{ name: \"${col.name.escapeForHtmlInJs()}\", children: $children, rightAlign: ${col.rightAlign}, values: $values }, \n")
            return colIndex
        }
        columns.forEach { dfs(it) }
        append("]")
    }
    val js = getResourceText("/addTable.js", "COLUMNS" to data, "ID" to id)
    return js
}

internal var tableId = 0

// TODO: use configuration.cellFormatter to format cells (currently disabled)
// TODO: display tooltips for column headers and data

internal fun AnyFrame.toHtmlData(configuration: DisplayConfiguration = DisplayConfiguration.DEFAULT, limit: Int): HtmlData {

    val scripts = mutableListOf<String>()
    val queue = LinkedList<Pair<AnyFrame, Int>>()

    fun AnyCol.toJs(): ColumnDataForJs {
        val values = values().take(limit).map {
            if(it is AnyFrame){
                val id = tableId++
                queue.add(it to id)
                DataFrameReference(id, it.size)
            }
            else formatter.format(it, configuration.cellContentLimit)
        }
        return ColumnDataForJs(name(), if (this is ColumnGroup<*>) columns().map { it.toJs() } else emptyList(), isSubtypeOf<Number?>(), values)
    }

    val id = tableId++
    queue.add(this to id)
    while(!queue.isEmpty()){
        val (nextDf, nextId) = queue.pop()
        val preparedColumns = nextDf.columns().map { it.toJs() }
        val js = tableJs(preparedColumns, nextId)
        scripts.add(js)
    }
    val body = getResourceText("/table.html", "ID" to id)
    val script = scripts.joinToString("\n") + "\n" + getResourceText("/renderTable.js", "ID" to id)
    return HtmlData("", body, script)
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

    operator fun plus(other: HtmlData) = HtmlData(style + "\n" + other.style, body + "\n" + other.body, script + "\n" + other.script)
}

internal fun initHtml() : HtmlData = HtmlData(style = getResources("/table.css", "/formatting.css"), script = getResourceText("/init.js"), body="")

fun <T> DataFrame<T>.toHTML(
    configuration: DisplayConfiguration = DisplayConfiguration.DEFAULT,
    includeInit: Boolean = false,
    getFooter: (DataFrame<T>) -> String = { "DataFrame [${it.size}]" }
): HtmlData {

    val limit = configuration.rowsLimit

    val footer = getFooter(this)
    val bodyFooter = if (limit < nrow())
        "<p>... $footer</p>"
    else "<p>$footer</p>"

    val tableHtml = toHtmlData(configuration, limit)
    val html = tableHtml + HtmlData("", bodyFooter, "")

    return if(includeInit) initHtml() + html else html
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

internal fun String.escapeForHtmlInJs() = replace("\"", "\\\"").escapeNewLines()

internal fun String.escapeForHtmlInJsMultiline() = replace("\"", "\\\"").replace("\n", "<br>")

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

internal class DataFrameFormatter(val nullClass: String, val curlyBracketsClass: String, val numberClass: String, val dataFrameClass: String, val commaClass: String, val colNameClass: String, val squareBracketsClass: String) {

    companion object {
        private fun String.withClass(css: String) = "<span class=\"$css\">$this</span>"
    }

    private class FormatBuilder(val limit: Int) {

        private var length: Int = 0
        private val sb = StringBuilder()

        val isFull get() = length >= limit

        fun append(str: String, css: String? = null){
            if(isFull) return
            val truncate = length + str.length > limit
            val s = if(truncate) str.substring(0, limit - length) else str
            length += s.length
            if(css != null)
                sb.append(s.withClass(css))
            else sb.append(s)
            if(truncate || isFull) sb.append("...")
        }

        override fun toString() = sb.toString()
    }

    fun format(value: Any?, limit: Int): String {
        val builder = FormatBuilder(limit)
        builder.render(value)
        return builder.toString()
    }

    private fun FormatBuilder.render(value: Any?) {
        if(isFull) return
        when (value) {
            null -> append("null", nullClass)
            is AnyRow -> {
                fun Any?.skip() = this == null || (this is Many<*> && this.isEmpty())
                val values = value.owner.columns().map { it.name() to it[value.index] }.filter { !it.second.skip() }
                if (values.isEmpty()) append("{ }", nullClass)
                else {
                    append("{ ", curlyBracketsClass)
                    var first = true
                    values.forEach {
                        if(isFull) return
                        if (first) first = false
                        else append(", ", commaClass)
                        append(it.first + ": ", colNameClass)
                        render(it.second)
                    }
                    append(" }", curlyBracketsClass)
                }
            }
            is AnyFrame -> append("DataFrame [${value.size}]", dataFrameClass)
            is AnyMany ->
                when {
                    value.isEmpty() -> append("[ ]", nullClass)
                    else -> {
                        append("[", squareBracketsClass)
                        var first = true
                        value.forEach {
                            if(isFull) return
                            if (first) first = false
                            else append(", ", commaClass)
                            render(it)
                        }
                        append("]", squareBracketsClass)
                    }
                }
            is Number -> append(value.toString(), numberClass)
            else -> append(value.toString())
        }
    }

}
