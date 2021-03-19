package org.jetbrains.dataframe.keywords

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.kotlin.dsl.get
import org.gradle.kotlin.dsl.register
import java.io.File

class KeywordsGeneratorPlugin: Plugin<Project> {
    override fun apply(target: Project): Unit = with(target){
        val genSrcDir = buildDir.resolve("generatedSrc")
        val sourceSets = project.extensions.getByName("sourceSets") as SourceSetContainer
        val mainSourceSet: SourceSet = sourceSets.named("main").get()
        mainSourceSet.addDir(genSrcDir)

        val genTask = tasks.register<GeneratorTask>(GeneratorTask.NAME) {
            srcDir = genSrcDir
        }

        tasks["compileKotlin"].dependsOn(genTask)
    }

    private fun SourceSet.addDir(dir: File) {
        java.setSrcDirs(java.srcDirs + dir)
    }
}
