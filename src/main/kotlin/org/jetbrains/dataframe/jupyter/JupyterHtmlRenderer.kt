package org.jetbrains.dataframe.jupyter

import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.io.DisplayConfiguration
import org.jetbrains.dataframe.io.getDefaultFooter
import org.jetbrains.dataframe.io.toHTML
import org.jetbrains.kotlinx.jupyter.api.HTML
import org.jetbrains.kotlinx.jupyter.api.libraries.JupyterIntegration

internal class JupyterHtmlRenderer(
    val display: DisplayConfiguration,
    val builder: JupyterIntegration.Builder,
)

internal inline fun <reified T : Any> JupyterHtmlRenderer.render(
    crossinline getDf: (T) -> DataFrame<*>,
    noinline getFooter: (DataFrame<*>) -> String = ::getDefaultFooter,
    crossinline modifyConfig: T.(DisplayConfiguration) -> DisplayConfiguration = { it }
) = builder.renderWithHost<T> { host, value ->
    val contextRenderer = JupyterCellRenderer(this.notebook, host)
    HTML(getDf(value).toHTML(value.modifyConfig(display), getFooter, contextRenderer))
}
