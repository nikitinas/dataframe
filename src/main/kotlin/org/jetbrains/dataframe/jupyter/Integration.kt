package org.jetbrains.dataframe.jupyter

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.annotations.DataSchema
import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.ColumnGroup
import org.jetbrains.dataframe.impl.codeGen.ReplCodeGenerator
import org.jetbrains.dataframe.internal.codeGen.CodeWithConverter
import org.jetbrains.dataframe.io.toHTML
import org.jetbrains.dataframe.stubs.DataFrameToListNamedStub
import org.jetbrains.dataframe.stubs.DataFrameToListTypedStub
import org.jetbrains.kotlinx.jupyter.api.*
import org.jetbrains.kotlinx.jupyter.api.annotations.JupyterLibrary
import org.jetbrains.kotlinx.jupyter.api.libraries.*
import kotlin.reflect.KClass
import kotlin.reflect.KProperty

internal val newDataSchemas = mutableListOf<KClass<*>>()

@JupyterLibrary
internal class Integration : JupyterIntegration(){

    override fun Builder.onLoaded() {

        val codeGen = ReplCodeGenerator.create()
        val config = JupyterConfiguration()

        onLoaded {
            declare("dataFrameConfig" to config)
        }

        render<AnyFrame> { HTML(it.toHTML(config.display)) }
        render<FormattedFrame<*>> { HTML(it.toHTML(config.display)) }
        render<AnyRow> { HTML(it.toDataFrame().toHTML(config.display) { "DataRow [${it.ncol}]" }) }
        render<ColumnGroup<*>> { it.df }
        render<AnyCol> { HTML(dataFrameOf(listOf(it)).toHTML(config.display) { "DataColumn [${it.nrow()}]" }) }
        render<GroupedDataFrame<*, *>> { it.plain() }

        import("org.jetbrains.dataframe.*")
        import("org.jetbrains.dataframe.annotations.*")
        import("org.jetbrains.dataframe.io.*")

        fun KotlinKernelHost.execute(codeWithConverter: CodeWithConverter, property: KProperty<*>): VariableName? {
            val code = codeWithConverter.with(property.name)
            return if(code.isNotBlank())
            {
                val result = execute(code)
                if(codeWithConverter.hasConverter)
                    result.name
                else null
            }
            else null
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

fun KotlinKernelHost.useSchemas(schemaClasses: Iterable<KClass<*>>){
    newDataSchemas.addAll(schemaClasses)
}

fun KotlinKernelHost.useSchemas(vararg schemaClasses: KClass<*>) = useSchemas(schemaClasses.asIterable())

inline fun <reified T> KotlinKernelHost.useSchema() = useSchemas(T::class)