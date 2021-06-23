package org.jetbrains.dataframe.person

import org.jetbrains.dataframe.AnyFrame
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.group
import org.jetbrains.dataframe.groupBy
import org.jetbrains.dataframe.into
import org.jetbrains.dataframe.io.read
import org.jetbrains.dataframe.io.toHTML
import org.jetbrains.dataframe.print
import org.junit.Ignore
import org.junit.Test
import java.awt.Desktop
import java.io.File

class HtmlRenderingTests: BaseTest() {

    fun AnyFrame.browse(){
        val file = File("temp.html")// File.createTempFile("df_rendering", ".html")
        file.writeText(toHTML(includeInit = true).toString())
        val uri = file.toURI()
        val desktop = Desktop.getDesktop()
        desktop.browse(uri)
    }

    @Test
    @Ignore
    fun test() {
        typed.group{ name and age }.into("group").browse()
    }
}