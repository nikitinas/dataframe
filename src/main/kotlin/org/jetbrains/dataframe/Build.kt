package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.impl.ColumnNameGenerator
import org.jetbrains.dataframe.impl.DataFrameImpl
import org.jetbrains.dataframe.impl.TreeNode
import org.jetbrains.dataframe.impl.asList
import org.jetbrains.dataframe.impl.columns.DataColumnWithParentImpl
import org.jetbrains.dataframe.impl.columns.MapColumnWithParent
import org.jetbrains.dataframe.impl.getOrPut
import java.lang.UnsupportedOperationException
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.full.declaredMembers
import kotlin.reflect.full.withNullability
import kotlin.reflect.jvm.javaField

class IterableDataFrameBuilder<T>(val source: Iterable<T>) {
    internal val columns = mutableListOf<AnyCol>()

    fun add(column: AnyCol) = columns.add(column)

    inline fun <reified R> add(name: String, noinline expression: T.(T) -> R?) =
        add(column(name, source.map { expression(it, it) }))

    inline infix fun <reified R> String.to(noinline expression: T.(T) -> R?) = add(this, expression)

    inline infix operator fun <reified R> String.invoke(noinline expression: T.(T) -> R?) = add(this, expression)

    inline infix operator fun <reified R> KProperty<R>.invoke(noinline expression: T.(T) -> R) = add(name, expression)
}

fun <T> Iterable<T>.toDataFrame(body: IterableDataFrameBuilder<T>.() -> Unit): AnyFrame {
    val builder = IterableDataFrameBuilder<T>(this)
    builder.body()
    return dataFrameOf(builder.columns)
}

inline fun <reified T> Iterable<T>.toDataFrame() = T::class.declaredMembers
    .filter { it.parameters.toList().size == 1 }
    .filter { it is KProperty }
    .map {
        val property = (it as KProperty)
        property.javaField?.isAccessible = true
        var nullable = false
        val values = this.map { obj ->
            if (obj == null) {
                nullable = true
                null
            } else {
                val value = it.call(obj)
                if (value == null) nullable = true
                value
            }
        }
        DataColumn.create(it.name, values, property.returnType.withNullability(nullable))
    }.let { dataFrameOf(it) }

fun DataFrame.Companion.of(columns: Iterable<AnyCol>) = dataFrameOf(columns)
fun DataFrame.Companion.of(vararg header: String) = dataFrameOf(header.toList())
fun DataFrame.Companion.of(vararg columns: AnyCol) = dataFrameOf(columns.asIterable())

fun dataFrameOf(columns: Iterable<AnyCol>): AnyFrame {
    val cols = columns.map { it.unbox() }
    if(cols.isEmpty()) return DataFrame.empty()
    return DataFrameImpl<Unit>(cols)
}

fun dataFrameOf(vararg header: ColumnReference<*>) = DataFrameBuilder(header.map { it.name() })

fun dataFrameOf(vararg columns: AnyCol): AnyFrame = dataFrameOf(columns.asIterable())

fun dataFrameOf(vararg header: String) = dataFrameOf(header.toList())

fun dataFrameOf(header: List<String>) = DataFrameBuilder(header)

fun emptyDataFrame(nrow: Int) = DataFrame.empty(nrow)


// TODO: remove checks for ColumnWithParent types
internal fun AnyCol.unbox(): AnyCol = when (this) {
    is ColumnWithPath<*> -> data.unbox()
    is DataColumnWithParentImpl<*> -> source.unbox()
    is MapColumnWithParent<*> -> source.unbox()
    else -> this
}

fun <T> Iterable<AnyCol>.asDataFrame() = dataFrameOf(this).typed<T>()

@JvmName("toDataFrameColumnPathAnyCol")
fun <T> Iterable<Pair<ColumnPath, AnyCol>>.toDataFrame(): DataFrame<T> {

    val nameGenerator = ColumnNameGenerator()
    val columnNames = mutableListOf<String>()
    val columnGroups = mutableListOf<MutableList<Pair<ColumnPath, AnyCol>>?>()
    val columns = mutableListOf<AnyCol?>()
    val columnIndices = mutableMapOf<String, Int>()
    val columnGroupName = mutableMapOf<String, String>()

    forEach { (path, col) ->
        when(path.size){
            0 -> {}
            1 -> {
                val name = path[0]
                val uniqueName = nameGenerator.addUnique(name)
                val index = columns.size
                columnNames.add(uniqueName)
                columnGroups.add(null)
                columns.add(col.rename(uniqueName))
                columnIndices[uniqueName] = index
            }
            else -> {
                val name = path[0]
                val uniqueName = columnGroupName.getOrPut(name){
                    nameGenerator.addUnique(name)
                }
                val index = columnIndices.getOrPut(uniqueName){
                    columnNames.add(uniqueName)
                    columnGroups.add(mutableListOf())
                    columns.add(null)
                    columns.size - 1
                }
                val list = columnGroups[index]!!
                list.add(path.subList(1, path.size) to col)
            }
        }
    }
    columns.indices.forEach { index ->
        val group = columnGroups[index]
        if(group != null){
            val nestedDf = group.toDataFrame<Unit>()
            val col = DataColumn.create(columnNames[index], nestedDf)
            assert(columns[index] == null)
            columns[index] = col
        } else assert(columns[index] != null)
    }
    return columns.map { it!! }.asDataFrame()
}

@JvmName("toDataFrameAnyCol")
fun Iterable<AnyCol>.toDataFrame() = asDataFrame<Unit>()

class DataFrameBuilder(private val columnNames: List<String>) {

    operator fun invoke(vararg columns: AnyCol) = invoke(columns.asIterable())

    operator fun invoke(columns: Iterable<AnyCol>): AnyFrame {
        val cols = columns.asList()
        require(cols.size == columnNames.size) { "Number of columns differs from number of column names" }
        return cols.mapIndexed { i, col ->
            col.rename(columnNames[i])
        }.asDataFrame<Unit>()
    }

    operator fun invoke(vararg values: Any?) = invoke(values.asIterable())

    @JvmName("invoke1")
    operator fun invoke(args: Iterable<Any?>): AnyFrame {

        val values = args.asList()

        require(columnNames.size > 0 && values.size.rem(columnNames.size) == 0) {
            "Data dimension ${columnNames.size} is not compatible with length of data vector ${values.size}"
        }

        val columnValues = values
            .mapIndexed { i, value -> i.rem(columnNames.size) to value }
            .groupBy { it.first }.values.map {
                it.map { it.second }
            }

        val columns = columnNames.zip(columnValues).map { (columnName, values) ->
            guessColumnType(columnName, values)
        }

        return dataFrameOf(columns)
    }
    operator fun invoke(args: Sequence<Any?>) = invoke(*args.toList().toTypedArray())
}

internal fun Iterable<KClass<*>>.commonParent() = commonParents(this).withMostSuperclasses() ?: Any::class

internal fun Iterable<KClass<*>>.commonType(nullable: Boolean) = commonParent().createStarProjectedType(nullable)

internal fun guessValueType(values: List<Any?>): KType {
    var nullable = false
    val types = values.map {
        if (it == null) nullable = true
        it?.javaClass
    }.distinct().mapNotNull { it?.kotlin }
    return types.commonType(nullable)
}

internal fun guessColumnType(name: String, values: List<Any?>) = guessValueType(values).let {
    DataColumn.create(name, values, it)
}