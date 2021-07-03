package org.jetbrains.dataframe.jupyter

import org.jetbrains.dataframe.images.Image
import org.jetbrains.dataframe.io.DisplayConfiguration
import org.jetbrains.dataframe.io.internallyRenderable
import org.jetbrains.dataframe.io.renderValueForHtml
import org.jetbrains.dataframe.io.tooltipLimit
import org.jetbrains.kotlinx.jupyter.api.MimeTypedResult
import org.jetbrains.kotlinx.jupyter.api.Notebook
import org.jetbrains.kotlinx.jupyter.api.Renderable
import org.jetbrains.kotlinx.jupyter.api.libraries.ExecutionHost

public interface CellRenderer {
    /**
     * Returns [value] rendered to HTML text, or null if such rendering is impossible
     */
    public fun content(value: Any?, configuration: DisplayConfiguration): String

    /**
     * Returns cell tooltip for this [value]
     */
    public fun tooltip(value: Any?, configuration: DisplayConfiguration): String
}

public abstract class ChainedCellRenderer(
    private val parent: CellRenderer,
) : CellRenderer {
    public abstract fun maybeContent(value: Any?, configuration: DisplayConfiguration): String?
    public abstract fun maybeTooltip(value: Any?, configuration: DisplayConfiguration): String?

    public override fun content(value: Any?, configuration: DisplayConfiguration): String {
        return maybeContent(value, configuration) ?: parent.content(value, configuration)
    }

    public override fun tooltip(value: Any?, configuration: DisplayConfiguration): String {
        return maybeTooltip(value, configuration) ?: parent.tooltip(value, configuration)
    }
}

public object DefaultCellRenderer : CellRenderer {
    public override fun content(value: Any?, configuration: DisplayConfiguration): String {
        return renderValueForHtml(value, configuration.cellContentLimit)
    }

    public override fun tooltip(value: Any?, configuration: DisplayConfiguration): String {
        return renderValueForHtml(value, tooltipLimit)
    }
}

public object ImageCellRenderer : ChainedCellRenderer(DefaultCellRenderer) {
    public override fun maybeContent(value: Any?, configuration: DisplayConfiguration): String? {
        return (value as? Image)?.let { "<img src=\"${it.url}\"/>" }
    }

    public override fun maybeTooltip(value: Any?, configuration: DisplayConfiguration): String? {
        return (value as? Image)?.url
    }
}

internal class JupyterCellRenderer(
    private val notebook: Notebook,
    private val host: ExecutionHost,
) : ChainedCellRenderer(ImageCellRenderer) {
    override fun maybeContent(value: Any?, configuration: DisplayConfiguration): String? {
        val renderersProcessor = notebook.renderersProcessor
        if (internallyRenderable(value)) return null
        val renderedVal = renderersProcessor.renderValue(host, value)
        val finalVal = if (renderedVal is Renderable) renderedVal.render(notebook) else renderedVal
        if (finalVal is MimeTypedResult && "text/html" in finalVal) return finalVal["text/html"]
        return renderValueForHtml(finalVal, configuration.cellContentLimit)
    }

    override fun maybeTooltip(value: Any?, configuration: DisplayConfiguration): String? {
        return null
    }
}
