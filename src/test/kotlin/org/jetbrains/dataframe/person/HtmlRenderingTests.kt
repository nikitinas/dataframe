package org.jetbrains.dataframe.person

import org.jetbrains.dataframe.group
import org.jetbrains.dataframe.into
import org.jetbrains.dataframe.io.toHTML
import org.junit.Ignore
import org.junit.Test
import java.awt.Desktop
import java.io.File

class HtmlRenderingTests: BaseTest() {

    @Test
    //@Ignore
    fun test() {
        val html = typed.group{ age and name }.into("group").toHTML(includeInit = true).toString()

        val file = File("temp.html")// File.createTempFile("df_rendering", ".html")
        file.writeText(html)
        val uri = file.toURI()
        val desktop = Desktop.getDesktop()
        desktop.browse(uri)
        println(html)
    }
}