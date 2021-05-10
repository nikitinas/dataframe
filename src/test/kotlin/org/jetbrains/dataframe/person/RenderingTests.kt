package org.jetbrains.dataframe.person

import io.kotest.matchers.shouldBe
import org.jetbrains.dataframe.index
import org.jetbrains.dataframe.io.toHTML
import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.FormattingDSL.gray
import org.jetbrains.dataframe.FormattingDSL.green
import org.jetbrains.dataframe.FormattingDSL.red
import org.junit.Test

class RenderingTests: BaseTest() {

    @Test
    fun `render to html`() {
        val src = df.toHTML()
        println(src)
    }

    @Test
    fun `render to string`(){
        val expected = """
            Data Frame [7 x 4]

            |name:String |age:Int |city:String? |weight:Int? |
            |------------|--------|-------------|------------|
            |Alice       |15      |London       |54          |
            |Bob         |45      |Dubai        |87          |
            |Mark        |20      |Moscow       |null        |
            |Mark        |40      |Milan        |null        |
            |Bob         |30      |Tokyo        |68          |
            |Alice       |20      |null         |55          |
            |Mark        |30      |Moscow       |90          |
        """.trimIndent()

        typed.toString().trim() shouldBe expected
    }

    @Test
    fun `conditional formatting`(){
        val formatter = typed.format { intCols().withoutNulls() }.with {
            if(it > 10) background(white) and bold and italic
            else textColor(linear(it, 30.5 to red, 50 to green)) and underline
        }.formatter!!

        for(row in 0 until typed.nrow())
            formatter(typed[row], typed.age)!!.attributes().size shouldBe if(typed[row].age > 10) 3 else 2
    }

    @Test
    fun `override format`() {
        val formatter = typed.format { age }.linearBg(20 to green, 80 to red)
            .format { age and weight }.where { index % 2 == 0 }.with { background(gray) }.formatter!!

        for(row in 0 until typed.nrow() step 2)
            formatter(typed[row], typed.age)!!.attributes() shouldBe listOf("background-color" to gray.encode())

        for(row in 1 until typed.nrow() step 2)
            formatter(typed[row], typed.age)!!.attributes() shouldBe listOf("background-color" to linearGradient(typed[row].age.toDouble(), 20.0, green, 80.0, red).encode())
    }
}