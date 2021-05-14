package org.jetbrains.dataframe.jupyter

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.annotations.DataSchema
import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.ColumnGroup
import org.jetbrains.dataframe.impl.codeGen.ReplCodeGenerator
import org.jetbrains.dataframe.io.toHTML
import org.jetbrains.dataframe.stubs.DataFrameToListNamedStub
import org.jetbrains.dataframe.stubs.DataFrameToListTypedStub
import org.jetbrains.kotlinx.jupyter.api.*
import org.jetbrains.kotlinx.jupyter.api.annotations.JupyterLibrary
import org.jetbrains.kotlinx.jupyter.api.libraries.*
import kotlin.reflect.KClass

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
        render<AnyRow> { it.toDataFrame() }
        render<ColumnGroup<*>> { it.df }
        render<AnyCol> { dataFrameOf(listOf(it)) }
        render<GroupedDataFrame<*, *>> { it.plain() }

        import("org.jetbrains.dataframe.*")
        import("org.jetbrains.dataframe.annotations.*")
        import("org.jetbrains.dataframe.io.*")

        updateVariable<AnyFrame> { df, property ->
            codeGen.process(df, property).let {
                val code = it.with(property.name)
                if(code.isNotBlank())
                {
                    val result = execute(code)
                    if(it.hasConverter)
                        result.name
                    else null
                }
                else null
            }
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