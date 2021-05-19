package org.jetbrains.dataframe.io

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVRecord
import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.impl.ColumnNameGenerator
import java.io.*
import java.math.BigDecimal
import java.net.URL
import java.nio.charset.Charset
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.zip.GZIPInputStream

enum class CSVType(val format: CSVFormat) {
    DEFAULT(CSVFormat.DEFAULT.withAllowMissingColumnNames().withIgnoreSurroundingSpaces()),
    TDF(CSVFormat.TDF.withAllowMissingColumnNames())
}

private val defaultCharset = Charsets.UTF_8

private val setOfNullStrings = setOf("NA", "N/A", "null")

internal fun isCompressed(fileOrUrl: String) = listOf("gz", "zip").contains(fileOrUrl.split(".").last())

internal fun isCompressed(file: File) = listOf("gz", "zip").contains(file.extension)

internal fun isCompressed(url: URL) = isCompressed(url.path)

fun DataFrame.Companion.readDelimStr(
    text: String,
    colTypes: Map<String, ColType> = mapOf(),
    skipLines: Int = 0,
    readLines: Int? = null
) = readDelim(StringReader(text), CSVType.DEFAULT.format.withHeader(), setOfNullStrings, colTypes, skipLines, readLines)

fun DataFrame.Companion.read(
    fileOrUrl: String,
    delimiter: Char,
    headers: List<String> = listOf(),
    nullStrings: Set<String> = setOfNullStrings,
    colTypes: Map<String, ColType> = mapOf(),
    skipLines: Int = 0,
    readLines: Int? = null,
    duplicate: Boolean = true,
    charset: Charset = Charsets.UTF_8
) =
    catchHttpResponse(asURL(fileOrUrl)) {
        readDelim(
            it, delimiter,
            headers, nullStrings, isCompressed(fileOrUrl),
            getCSVType(fileOrUrl), colTypes,
            skipLines, readLines,
            duplicate, charset
        )
    }

fun DataFrame.Companion.readCSV(
    fileOrUrl: String,
    delimiter: Char = ',',
    headers: List<String> = listOf(),
    nullStrings: Set<String> = setOfNullStrings,
    colTypes: Map<String, ColType> = mapOf(),
    skipLines: Int = 0,
    readLines: Int? = null,
    duplicate: Boolean = true,
    charset: Charset = Charsets.UTF_8
) =
    catchHttpResponse(asURL(fileOrUrl)) {
        readDelim(
            it, delimiter,
            headers, nullStrings, isCompressed(fileOrUrl),
            CSVType.DEFAULT, colTypes,
            skipLines, readLines,
            duplicate, charset
        )
    }

fun DataFrame.Companion.readCSV(
    file: File,
    delimiter: Char = ',',
    headers: List<String> = listOf(),
    nullStrings: Set<String> = setOfNullStrings,
    colTypes: Map<String, ColType> = mapOf(),
    skipLines: Int = 0,
    readLines: Int? = null,
    duplicate: Boolean = true,
    charset: Charset = Charsets.UTF_8
) =
    readDelim(FileInputStream(file), delimiter,
        headers, nullStrings, isCompressed(file),
        CSVType.DEFAULT, colTypes,
        skipLines, readLines,
        duplicate, charset)

fun DataFrame.Companion.readCSV(
    url: URL,
    delimiter: Char = ',',
    headers: List<String> = listOf(),
    nullStrings: Set<String> = setOfNullStrings,
    colTypes: Map<String, ColType> = mapOf(),
    skipLines: Int = 0,
    readLines: Int? = null,
    duplicate: Boolean = true,
    charset: Charset = Charsets.UTF_8
) =
    readDelim(url.openStream(), delimiter,
        headers, nullStrings, isCompressed(url),
        CSVType.DEFAULT, colTypes,
        skipLines, readLines,
        duplicate, charset)

fun DataFrame.Companion.readTSV(
    fileOrUrl: String,
    delimiter: Char = '\t',
    headers: List<String> = listOf(),
    nullStrings: Set<String> = setOfNullStrings,
    colTypes: Map<String, ColType> = mapOf(),
    skipLines: Int = 0,
    readLines: Int? = null,
    duplicate: Boolean = true,
    charset: Charset = Charsets.UTF_8
) =
    catchHttpResponse(asURL(fileOrUrl)) {
        readDelim(
            it, delimiter,
            headers, nullStrings, isCompressed(fileOrUrl),
            CSVType.TDF, colTypes,
            skipLines, readLines,
            duplicate, charset
        )
    }

fun DataFrame.Companion.readTSV(
    file: File,
    delimiter: Char = '\t',
    headers: List<String> = listOf(),
    nullStrings: Set<String> = setOfNullStrings,
    colTypes: Map<String, ColType> = mapOf(),
    skipLines: Int = 0,
    readLines: Int? = null,
    duplicate: Boolean = true,
    charset: Charset = Charsets.UTF_8
) =
    readDelim(FileInputStream(file), delimiter,
        headers, nullStrings, isCompressed(file),
        CSVType.TDF, colTypes,
        skipLines, readLines,
        duplicate, charset)


private fun getCSVType(path: String): CSVType =
    when(path.substringAfterLast('.').toLowerCase()) {
        "csv" -> CSVType.DEFAULT
        "tdf" -> CSVType.TDF
        else -> throw IOException("Unknown file format")
    }

private fun asStream(fileOrUrl: String) = (if (isURL(fileOrUrl)) {
    URL(fileOrUrl).toURI()
} else {
    File(fileOrUrl).toURI()
}).toURL().openStream()

private fun asURL(fileOrUrl: String): URL = (if (isURL(fileOrUrl)) {
    URL(fileOrUrl).toURI()
} else {
    File(fileOrUrl).toURI()
}).toURL()

private fun getFormat(type: CSVType, delimiter: Char, headers: List<String>, duplicate: Boolean): CSVFormat =
    type.format.withDelimiter(delimiter).withHeader(*headers.toTypedArray()).withAllowDuplicateHeaderNames(duplicate)

fun DataFrame.Companion.readDelim(
    inStream: InputStream,
    delimiter: Char = ',',
    headers: List<String> = listOf(),
    nullStrings: Set<String> = setOfNullStrings,
    isCompressed: Boolean = false,
    csvType: CSVType,
    colTypes: Map<String, ColType> = mapOf(),
    skipLines: Int = 0,
    readLines: Int? = null,
    duplicate: Boolean = true,
    charset: Charset = defaultCharset
): AnyFrame =
    if (isCompressed) {
        InputStreamReader(GZIPInputStream(inStream), charset)
    } else {
        BufferedReader(InputStreamReader(inStream, charset))
    }.run {
        readDelim(this, getFormat(csvType, delimiter, headers, duplicate), nullStrings, colTypes, skipLines, readLines)
    }

internal fun isURL(fileOrUrl: String): Boolean = listOf("http:", "https:", "ftp:").any { fileOrUrl.startsWith(it) }

enum class ColType {
    Int,
    Long,
    Double,
    Boolean,
    BigDecimal,
    LocalDate,
    LocalTime,
    LocalDateTime,
    String,
}

fun ColType.toType() = when (this) {
    ColType.Int -> Int::class
    ColType.Long -> Long::class
    ColType.Double -> Double::class
    ColType.Boolean -> Boolean::class
    ColType.BigDecimal -> BigDecimal::class
    ColType.LocalDate -> LocalDate::class
    ColType.LocalTime -> LocalTime::class
    ColType.LocalDateTime -> LocalDateTime::class
    ColType.String -> String::class
}

fun DataFrame.Companion.readDelim(
    reader: Reader,
    format: CSVFormat = CSVFormat.DEFAULT.withHeader(),
    nullStrings: Set<String> = setOfNullStrings,
    colTypes: Map<String, ColType> = mapOf(),
    skipLines: Int = 0,
    readLines: Int? = null
): AnyFrame {

    var reader = reader
    if (skipLines > 0) {
        reader = BufferedReader(reader)
        repeat(skipLines) { reader.readLine() }
    }

    format.parse(reader).use { csvParser ->
        val records = if (readLines == null) {
            csvParser.records
        } else {
            require(readLines >= 0) { "`readLines` must not be negative" }
            val records = ArrayList<CSVRecord>(readLines)
            val iter = csvParser.iterator()
            var count = readLines ?: 0
            while (iter.hasNext() && 0 < count--) {
                records.add(iter.next())
            }
            records
        }

        val columnNames = csvParser.headerNames.takeIf { it.isNotEmpty() }
            ?: (1..records[0].count()).map { index -> "X${index}" }

        val generator = ColumnNameGenerator()
        val uniqueNames = columnNames.map { generator.addUnique(it) }

        val cols = uniqueNames.mapIndexed { colIndex, colName ->
            val defaultColType = colTypes[".default"]
            val colType = colTypes[colName] ?: defaultColType
            var hasNulls = false
            val values = records.map { it[colIndex]?.emptyAsNull(nullStrings).also { if (it == null) hasNulls = true } }
            val column = column(colName, values, hasNulls)
            when (colType) {
                null -> column.tryParseAny()
                ColType.String -> column
                else -> {
                    val parser = Parsers[colType.toType()]!!
                    column.parse(parser)
                }
            }
        }
        return cols.asDataFrame<Unit>()
    }
}

internal fun String.emptyAsNull(nullStrings: Set<String>): String? =
    when {
        this.isEmpty() -> null
        nullStrings.contains(this) -> null
        else -> this
    }

fun AnyFrame.writeCSV(file: File, format: CSVFormat = CSVFormat.DEFAULT.withHeader()) =
    writeCSV(FileWriter(file), format)

fun AnyFrame.writeCSV(path: String, format: CSVFormat = CSVFormat.DEFAULT.withHeader()) =
    writeCSV(FileWriter(path), format)

fun AnyFrame.writeCSV(writer: Appendable, format: CSVFormat = CSVFormat.DEFAULT.withHeader()) =
    format.print(writer).use { printer ->
        printer.printRecord(columnNames())
        forEach {
            printer.printRecord(it.values)
        }
    }