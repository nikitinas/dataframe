package org.jetbrains.dataframe.jupyter

import org.jetbrains.dataframe.AnyFrame
import org.jetbrains.dataframe.AnyRow
import org.jetbrains.dataframe.FormattedFrame
import org.jetbrains.dataframe.GroupedDataFrame
import org.jetbrains.dataframe.annotations.DataSchema
import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.ColumnGroup
import org.jetbrains.dataframe.dataFrameOf
import org.jetbrains.dataframe.impl.codeGen.ReplCodeGenerator
import org.jetbrains.dataframe.internal.codeGen.CodeWithConverter
import org.jetbrains.dataframe.ncol
import org.jetbrains.dataframe.stubs.DataFrameToListNamedStub
import org.jetbrains.dataframe.stubs.DataFrameToListTypedStub
import org.jetbrains.dataframe.toDataFrame
import org.jetbrains.kotlinx.jupyter.api.KotlinKernelHost
import org.jetbrains.kotlinx.jupyter.api.VariableName
import org.jetbrains.kotlinx.jupyter.api.annotations.JupyterLibrary
import org.jetbrains.kotlinx.jupyter.api.declare
import org.jetbrains.kotlinx.jupyter.api.libraries.JupyterIntegration
import kotlin.reflect.KClass
import kotlin.reflect.KProperty

internal val newDataSchemas = mutableListOf<KClass<*>>()

@JupyterLibrary
internal class Integration : JupyterIntegration() {

    override fun Builder.onLoaded() {
        val codeGen = ReplCodeGenerator.create()
        val config = JupyterConfiguration()

        onLoaded {
            declare("dataFrameConfig" to config)
        }

        with(JupyterHtmlRenderer(config.display, this)) {
            render<AnyFrame>({ it })
            render<FormattedFrame<*>>({ it.df }, modifyConfig = { getDisplayConfiguration(it) })
            render<AnyRow>({ it.toDataFrame() }, { "DataRow [${it.ncol}]" })
            render<ColumnGroup<*>>({ it.df })
            render<AnyCol>({ dataFrameOf(listOf(it)) }, { "DataColumn [${it.nrow()}]" })
            render<GroupedDataFrame<*, *>>({ it.plain() })
        }

        import("org.jetbrains.dataframe.*")
        import("org.jetbrains.dataframe.annotations.*")
        import("org.jetbrains.dataframe.io.*")

        fun KotlinKernelHost.execute(codeWithConverter: CodeWithConverter, property: KProperty<*>): VariableName? {
            val code = codeWithConverter.with(property.name)
            return if (code.isNotBlank()) {
                val result = execute(code)
                if (codeWithConverter.hasConverter) {
                    result.name
                } else null
            } else null
        }

        updateVariable<AnyFrame> { df, property ->
            execute(codeGen.process(df, property), property)
        }

        updateVariable<AnyRow> { row, property ->
            execute(codeGen.process(row, property), property)
        }

        updateVariable<DataFrameToListNamedStub> { stub, prop ->
            val code = codeGen.process(stub).with(prop.name)
            execute(code).name
        }

        updateVariable<DataFrameToListTypedStub> { stub, prop ->
            val code = codeGen.process(stub).with(prop.name)
            execute(code).name
        }

        fun KotlinKernelHost.addDataSchemas(classes: List<KClass<*>>) {
            val code = classes.map {
                codeGen.process(it)
            }.joinToString("\n").trim()

            if (code.isNotEmpty()) {
                execute(code)
            }
        }

        onClassAnnotation<DataSchema> { addDataSchemas(it) }

        afterCellExecution { snippet, result ->
            if (newDataSchemas.isNotEmpty()) {
                addDataSchemas(newDataSchemas)
                newDataSchemas.clear()
            }
        }
    }
}

public fun KotlinKernelHost.useSchemas(schemaClasses: Iterable<KClass<*>>) {
    newDataSchemas.addAll(schemaClasses)
}

public fun KotlinKernelHost.useSchemas(vararg schemaClasses: KClass<*>): Unit = useSchemas(schemaClasses.asIterable())

public inline fun <reified T> KotlinKernelHost.useSchema(): Unit = useSchemas(T::class)
