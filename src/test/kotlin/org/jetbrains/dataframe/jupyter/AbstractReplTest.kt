package org.jetbrains.dataframe.jupyter

import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.types.shouldBeInstanceOf
import jupyter.kotlin.DependsOn
import org.jetbrains.kotlinx.jupyter.ReplForJupyter
import org.jetbrains.kotlinx.jupyter.ReplForJupyterImpl
import org.jetbrains.kotlinx.jupyter.api.Code
import org.jetbrains.kotlinx.jupyter.api.MimeTypedResult
import org.jetbrains.kotlinx.jupyter.libraries.EmptyResolutionInfoProvider
import org.junit.Before
import kotlin.reflect.KClass
import kotlin.reflect.full.memberProperties
import kotlin.script.experimental.jvm.util.classpathFromClassloader

abstract class AbstractReplTest {
    private val repl: ReplForJupyter = ReplForJupyterImpl(EmptyResolutionInfoProvider, classpath, isEmbedded = true)

    @Before
    fun initRepl() {
        // Jupyter integration is loaded after some code was executed, so we do it here
        // We also define here a class to retrieve values without rendering
        exec("class $WRAP(val $WRAP_VAL: Any?)")
    }

    fun exec(code: Code): Any? {
        return repl.eval(code).resultValue
    }

    @JvmName("execTyped")
    inline fun <reified T: Any> exec(code: Code): T {
        val res = exec(code)
        res.shouldBeInstanceOf<T>()
        return res
    }

    fun execHtml(code: Code): String {
        val res = exec<MimeTypedResult>(code)
        val html = res["text/html"]
        html.shouldNotBeNull()
        return html
    }

    fun execWrapped(code: Code): Any? {
        val wrapped = exec(code)!!
        @Suppress("UNCHECKED_CAST")
        val clazz = wrapped::class as KClass<Any>
        val prop = clazz.memberProperties.single { it.name == WRAP_VAL }
        return prop.get(wrapped)
    }

    companion object {
        @JvmStatic
        protected val WRAP = "W"

        private const val WRAP_VAL = "v"

        private val classpath = run {
            val scriptArtifacts = setOf(
                "kotlin-jupyter-lib",
                "kotlin-jupyter-api",
                "kotlin-jupyter-shared-compiler",
                "kotlin-stdlib",
                "kotlin-reflect",
                "kotlin-script-runtime",
            )
            classpathFromClassloader(DependsOn::class.java.classLoader).orEmpty().filter { file ->
                val name = file.name
                (name == "main" && file.parentFile.name == "kotlin")
                        || (file.extension == "jar" && scriptArtifacts.any { name.startsWith(it) })
            }
        }
    }
}
