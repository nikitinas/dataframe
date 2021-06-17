package org.jetbrains.dataframe

import org.jetbrains.dataframe.annotations.DataSchema
import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.name
import org.jetbrains.dataframe.columns.size
import org.jetbrains.dataframe.columns.type
import org.jetbrains.dataframe.columns.values
import org.jetbrains.dataframe.columns.ndistinct
import kotlin.reflect.KType

fun <T> DataFrame<T>.describe(columns: ColumnsSelector<T, *> = { numberCols() }) = describe(this[columns])
fun <T> DataColumn<T>.describe() = describe(listOf(this))

@DataSchema
interface GeneralColumnDescriptionSchema{
    val column: String
    val count: Int
    val nulls: Int
}

@DataSchema
interface ColumnDescriptionSchema: GeneralColumnDescriptionSchema {
    val unique: Int
    val top: Any
    val freq: Int
    val type: KType
}

@DataSchema
interface NumberColumnDescriptionSchema: GeneralColumnDescriptionSchema {
    val mean: Double
    val min: Any
    val max: KType
}

internal fun describe(cols: List<AnyCol>): DataFrame<ColumnDescriptionSchema> {
    val hasNumeric = cols.any { it.isNumber() }
    val hasCategorical = cols.any { !it.isNumber() }
    var df = cols.toDataFrame {
        GeneralColumnDescriptionSchema::column { name }
        GeneralColumnDescriptionSchema::count { size }
        GeneralColumnDescriptionSchema::nulls { values.count { it == null } }

        if (hasCategorical) {
            ColumnDescriptionSchema::unique { ndistinct }
            ColumnDescriptionSchema::top { values.groupBy { it }.maxByOrNull { it.value.size }?.key }
            ColumnDescriptionSchema::type { type.toString() }
        }
        if (hasNumeric) {
            NumberColumnDescriptionSchema::mean { if(it.isNumber()) (it as DataColumn<Number?>).mean() else null }
            NumberColumnDescriptionSchema::min { if(it.isComparable()) (it as DataColumn<Comparable<Any?>>).min() else null }
            NumberColumnDescriptionSchema::max { if(it.isComparable()) (it as DataColumn<Comparable<Any?>>).max() else null }
        }
    }
    if (hasCategorical) df = df.add(ColumnDescriptionSchema::freq) {
        val top = it[ColumnDescriptionSchema::top]
        val data = cols[index]
        data.values.count { it == top }
    }.move(ColumnDescriptionSchema::freq).after(ColumnDescriptionSchema::top)

    return df.typed()
}

