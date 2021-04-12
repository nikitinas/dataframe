package org.jetbrains.dataframe.io

import io.kotlintest.shouldBe
import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.annotations.DataSchema
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.MapColumn
import org.jetbrains.dataframe.columns.ColumnGroup
import org.jetbrains.dataframe.images.Image
import org.jetbrains.dataframe.impl.codeGen.CodeGenerator
import org.jetbrains.dataframe.impl.codeGen.CodeGeneratorImpl
import org.jetbrains.dataframe.impl.codeGen.ReplCodeGenerator
import org.junit.Test

class PlaylistJsonTest {

    @DataSchema(isOpen = false)
    interface DataFrameType4 {
        val url: String
        val width: Int
        val height: Int
    }

    @DataSchema(isOpen = false)
    interface DataFrameType5 {
        val url: String
        val width: Int
        val height: Int
    }

    @DataSchema(isOpen = false)
    interface DataFrameType6 {
        val url: String
        val width: Int
        val height: Int
    }

    @DataSchema(isOpen = false)
    interface DataFrameType7 {
        val url: String?
        val width: Int?
        val height: Int?
    }

    @DataSchema(isOpen = false)
    interface DataFrameType8 {
        val url: String?
        val width: Int?
        val height: Int?
    }

    @DataSchema(isOpen = false)
    interface DataFrameType3 {
        val default: DataRow<DataFrameType4>
        val medium: DataRow<DataFrameType5>
        val high: DataRow<DataFrameType6>
        val standard: DataRow<DataFrameType7>
        val maxres: DataRow<DataFrameType8>
    }

    @DataSchema(isOpen = false)
    interface DataFrameType9 {
        val kind: String
        val videoId: String
    }

    @DataSchema(isOpen = false)
    interface DataFrameType2 {
        val publishedAt: String
        val channelId: String
        val title: String
        val description: String
        val thumbnails: DataRow<DataFrameType3>
        val channelTitle: String
        val playlistId: String
        val position: Int
        val resourceId: DataRow<DataFrameType9>
    }

    @DataSchema(isOpen = false)
    interface DataFrameType1 {
        val kind: String
        val etag: String
        val id: String
        val snippet: DataRow<DataFrameType2>
    }

    @DataSchema(isOpen = false)
    interface DataFrameType10 {
        val totalResults: Int
        val resultsPerPage: Int
    }

    @DataSchema
    interface DataRecord {
        val kind: String
        val etag: String
        val nextPageToken: String
        val items: DataFrame<DataFrameType1>
        val pageInfo: DataRow<DataFrameType10>
    }

    val DataFrameBase<DataFrameType1>.etag: DataColumn<String> @JvmName("DataFrameType1_etag") get() = this["etag"] as DataColumn<String>
    val DataRowBase<DataFrameType1>.etag: String @JvmName("DataFrameType1_etag") get() = this["etag"] as String
    val DataFrameBase<DataFrameType1>.id: DataColumn<String> @JvmName("DataFrameType1_id") get() = this["id"] as DataColumn<String>
    val DataRowBase<DataFrameType1>.id: String @JvmName("DataFrameType1_id") get() = this["id"] as String
    val DataFrameBase<DataFrameType1>.kind: DataColumn<String> @JvmName("DataFrameType1_kind") get() = this["kind"] as DataColumn<String>
    val DataRowBase<DataFrameType1>.kind: String @JvmName("DataFrameType1_kind") get() = this["kind"] as String
    val DataFrameBase<DataFrameType1>.snippet: ColumnGroup<DataFrameType2> @JvmName("DataFrameType1_snippet") get() = this["snippet"] as ColumnGroup<DataFrameType2>
    val DataRowBase<DataFrameType1>.snippet: org.jetbrains.dataframe.DataRow<DataFrameType2> @JvmName("DataFrameType1_snippet") get() = this["snippet"] as org.jetbrains.dataframe.DataRow<DataFrameType2>
    val DataFrameBase<DataFrameType2>.channelId: DataColumn<String> @JvmName("DataFrameType2_channelId") get() = this["channelId"] as DataColumn<String>
    val DataRowBase<DataFrameType2>.channelId: String @JvmName("DataFrameType2_channelId") get() = this["channelId"] as String
    val DataFrameBase<DataFrameType2>.channelTitle: DataColumn<String> @JvmName("DataFrameType2_channelTitle") get() = this["channelTitle"] as DataColumn<String>
    val DataRowBase<DataFrameType2>.channelTitle: String @JvmName("DataFrameType2_channelTitle") get() = this["channelTitle"] as String
    val DataFrameBase<DataFrameType2>.description: DataColumn<String> @JvmName("DataFrameType2_description") get() = this["description"] as DataColumn<String>
    val DataRowBase<DataFrameType2>.description: String @JvmName("DataFrameType2_description") get() = this["description"] as String
    val DataFrameBase<DataFrameType2>.playlistId: DataColumn<String> @JvmName("DataFrameType2_playlistId") get() = this["playlistId"] as DataColumn<String>
    val DataRowBase<DataFrameType2>.playlistId: String @JvmName("DataFrameType2_playlistId") get() = this["playlistId"] as String
    val DataFrameBase<DataFrameType2>.position: DataColumn<Int> @JvmName("DataFrameType2_position") get() = this["position"] as DataColumn<Int>
    val DataRowBase<DataFrameType2>.position: Int @JvmName("DataFrameType2_position") get() = this["position"] as Int
    val DataFrameBase<DataFrameType2>.publishedAt: DataColumn<String> @JvmName("DataFrameType2_publishedAt") get() = this["publishedAt"] as DataColumn<String>
    val DataRowBase<DataFrameType2>.publishedAt: String @JvmName("DataFrameType2_publishedAt") get() = this["publishedAt"] as String
    val DataFrameBase<DataFrameType2>.resourceId: ColumnGroup<DataFrameType9> @JvmName("DataFrameType2_resourceId") get() = this["resourceId"] as ColumnGroup<DataFrameType9>
    val DataRowBase<DataFrameType2>.resourceId: org.jetbrains.dataframe.DataRow<DataFrameType9> @JvmName("DataFrameType2_resourceId") get() = this["resourceId"] as org.jetbrains.dataframe.DataRow<DataFrameType9>
    val DataFrameBase<DataFrameType2>.thumbnails: ColumnGroup<DataFrameType3> @JvmName("DataFrameType2_thumbnails") get() = this["thumbnails"] as ColumnGroup<DataFrameType3>
    val DataRowBase<DataFrameType2>.thumbnails: org.jetbrains.dataframe.DataRow<DataFrameType3> @JvmName("DataFrameType2_thumbnails") get() = this["thumbnails"] as org.jetbrains.dataframe.DataRow<DataFrameType3>
    val DataFrameBase<DataFrameType2>.title: DataColumn<String> @JvmName("DataFrameType2_title") get() = this["title"] as DataColumn<String>
    val DataRowBase<DataFrameType2>.title: String @JvmName("DataFrameType2_title") get() = this["title"] as String
    val DataFrameBase<DataFrameType3>.default: ColumnGroup<DataFrameType4> @JvmName("DataFrameType3_default") get() = this["default"] as ColumnGroup<DataFrameType4>
    val DataRowBase<DataFrameType3>.default: org.jetbrains.dataframe.DataRow<DataFrameType4> @JvmName("DataFrameType3_default") get() = this["default"] as org.jetbrains.dataframe.DataRow<DataFrameType4>
    val DataFrameBase<DataFrameType3>.high: ColumnGroup<DataFrameType6> @JvmName("DataFrameType3_high") get() = this["high"] as ColumnGroup<DataFrameType6>
    val DataRowBase<DataFrameType3>.high: DataRow<DataFrameType6> @JvmName("DataFrameType3_high") get() = this["high"] as DataRow<DataFrameType6>
    val DataFrameBase<DataFrameType3>.maxres: ColumnGroup<DataFrameType8> @JvmName("DataFrameType3_maxres") get() = this["maxres"] as ColumnGroup<DataFrameType8>
    val DataRowBase<DataFrameType3>.maxres: org.jetbrains.dataframe.DataRow<DataFrameType8> @JvmName("DataFrameType3_maxres") get() = this["maxres"] as org.jetbrains.dataframe.DataRow<DataFrameType8>
    val DataFrameBase<DataFrameType3>.medium: ColumnGroup<DataFrameType5> @JvmName("DataFrameType3_medium") get() = this["medium"] as ColumnGroup<DataFrameType5>
    val DataRowBase<DataFrameType3>.medium: org.jetbrains.dataframe.DataRow<DataFrameType5> @JvmName("DataFrameType3_medium") get() = this["medium"] as org.jetbrains.dataframe.DataRow<DataFrameType5>
    val DataFrameBase<DataFrameType3>.standard: ColumnGroup<DataFrameType7> @JvmName("DataFrameType3_standard") get() = this["standard"] as ColumnGroup<DataFrameType7>
    val DataRowBase<DataFrameType3>.standard: org.jetbrains.dataframe.DataRow<DataFrameType7> @JvmName("DataFrameType3_standard") get() = this["standard"] as org.jetbrains.dataframe.DataRow<DataFrameType7>
    val DataFrameBase<DataFrameType4>.height: DataColumn<Int> @JvmName("DataFrameType4_height") get() = this["height"] as DataColumn<Int>
    val DataRowBase<DataFrameType4>.height: Int @JvmName("DataFrameType4_height") get() = this["height"] as Int
    val DataFrameBase<DataFrameType4>.url: DataColumn<String> @JvmName("DataFrameType4_url") get() = this["url"] as DataColumn<String>
    val DataRowBase<DataFrameType4>.url: String @JvmName("DataFrameType4_url") get() = this["url"] as String
    val DataFrameBase<DataFrameType4>.width: DataColumn<Int> @JvmName("DataFrameType4_width") get() = this["width"] as DataColumn<Int>
    val DataRowBase<DataFrameType4>.width: Int @JvmName("DataFrameType4_width") get() = this["width"] as Int
    val DataFrameBase<DataFrameType5>.height: DataColumn<Int> @JvmName("DataFrameType5_height") get() = this["height"] as DataColumn<Int>
    val DataRowBase<DataFrameType5>.height: Int @JvmName("DataFrameType5_height") get() = this["height"] as Int
    val DataFrameBase<DataFrameType5>.url: DataColumn<String> @JvmName("DataFrameType5_url") get() = this["url"] as DataColumn<String>
    val DataRowBase<DataFrameType5>.url: String @JvmName("DataFrameType5_url") get() = this["url"] as String
    val DataFrameBase<DataFrameType5>.width: DataColumn<Int> @JvmName("DataFrameType5_width") get() = this["width"] as DataColumn<Int>
    val DataRowBase<DataFrameType5>.width: Int @JvmName("DataFrameType5_width") get() = this["width"] as Int
    val DataFrameBase<DataFrameType6>.height: DataColumn<Int> @JvmName("DataFrameType6_height") get() = this["height"] as DataColumn<Int>
    val DataRowBase<DataFrameType6>.height: Int @JvmName("DataFrameType6_height") get() = this["height"] as Int
    val DataFrameBase<DataFrameType6>.url: DataColumn<String> @JvmName("DataFrameType6_url") get() = this["url"] as DataColumn<String>
    val DataRowBase<DataFrameType6>.url: String @JvmName("DataFrameType6_url") get() = this["url"] as String
    val DataFrameBase<DataFrameType6>.width: DataColumn<Int> @JvmName("DataFrameType6_width") get() = this["width"] as DataColumn<Int>
    val DataRowBase<DataFrameType6>.width: Int @JvmName("DataFrameType6_width") get() = this["width"] as Int
    val DataFrameBase<DataFrameType7>.height: DataColumn<Int?> @JvmName("DataFrameType7_height") get() = this["height"] as DataColumn<Int?>
    val DataRowBase<DataFrameType7>.height: Int? @JvmName("DataFrameType7_height") get() = this["height"] as Int?
    val DataFrameBase<DataFrameType7>.url: DataColumn<String?> @JvmName("DataFrameType7_url") get() = this["url"] as DataColumn<String?>
    val DataRowBase<DataFrameType7>.url: String? @JvmName("DataFrameType7_url") get() = this["url"] as String?
    val DataFrameBase<DataFrameType7>.width: DataColumn<Int?> @JvmName("DataFrameType7_width") get() = this["width"] as DataColumn<Int?>
    val DataRowBase<DataFrameType7>.width: Int? @JvmName("DataFrameType7_width") get() = this["width"] as Int?
    val DataFrameBase<DataFrameType8>.height: DataColumn<Int?> @JvmName("DataFrameType8_height") get() = this["height"] as DataColumn<Int?>
    val DataRowBase<DataFrameType8>.height: Int? @JvmName("DataFrameType8_height") get() = this["height"] as Int?
    val DataFrameBase<DataFrameType8>.url: DataColumn<String?> @JvmName("DataFrameType8_url") get() = this["url"] as DataColumn<String?>
    val DataRowBase<DataFrameType8>.url: String? @JvmName("DataFrameType8_url") get() = this["url"] as String?
    val DataFrameBase<DataFrameType8>.width: DataColumn<Int?> @JvmName("DataFrameType8_width") get() = this["width"] as DataColumn<Int?>
    val DataRowBase<DataFrameType8>.width: Int? @JvmName("DataFrameType8_width") get() = this["width"] as Int?
    val DataFrameBase<DataFrameType9>.kind: DataColumn<String> @JvmName("DataFrameType9_kind") get() = this["kind"] as DataColumn<String>
    val DataRowBase<DataFrameType9>.kind: String @JvmName("DataFrameType9_kind") get() = this["kind"] as String
    val DataFrameBase<DataFrameType9>.videoId: DataColumn<String> @JvmName("DataFrameType9_videoId") get() = this["videoId"] as DataColumn<String>
    val DataRowBase<DataFrameType9>.videoId: String @JvmName("DataFrameType9_videoId") get() = this["videoId"] as String
    val DataFrameBase<DataFrameType10>.resultsPerPage: DataColumn<Int> @JvmName("DataFrameType10_resultsPerPage") get() = this["resultsPerPage"] as DataColumn<Int>
    val DataRowBase<DataFrameType10>.resultsPerPage: Int @JvmName("DataFrameType10_resultsPerPage") get() = this["resultsPerPage"] as Int
    val DataFrameBase<DataFrameType10>.totalResults: DataColumn<Int> @JvmName("DataFrameType10_totalResults") get() = this["totalResults"] as DataColumn<Int>
    val DataRowBase<DataFrameType10>.totalResults: Int @JvmName("DataFrameType10_totalResults") get() = this["totalResults"] as Int
    val DataFrameBase<DataRecord>.etag: DataColumn<String> @JvmName("DataRecord_etag") get() = this["etag"] as DataColumn<String>
    val DataRowBase<DataRecord>.etag: String @JvmName("DataRecord_etag") get() = this["etag"] as String
    val DataFrameBase<DataRecord>.items: DataColumn<DataFrame<DataFrameType1>> @JvmName("DataRecord_items") get() = this["items"] as DataColumn<DataFrame<DataFrameType1>>
    val DataRowBase<DataRecord>.items: org.jetbrains.dataframe.DataFrame<DataFrameType1> @JvmName("DataRecord_items") get() = this["items"] as org.jetbrains.dataframe.DataFrame<DataFrameType1>
    val DataFrameBase<DataRecord>.kind: DataColumn<String> @JvmName("DataRecord_kind") get() = this["kind"] as DataColumn<String>
    val DataRowBase<DataRecord>.kind: String @JvmName("DataRecord_kind") get() = this["kind"] as String
    val DataFrameBase<DataRecord>.nextPageToken: DataColumn<String> @JvmName("DataRecord_nextPageToken") get() = this["nextPageToken"] as DataColumn<String>
    val DataRowBase<DataRecord>.nextPageToken: String @JvmName("DataRecord_nextPageToken") get() = this["nextPageToken"] as String
    val DataFrameBase<DataRecord>.pageInfo: ColumnGroup<DataFrameType10> @JvmName("DataRecord_pageInfo") get() = this["pageInfo"] as ColumnGroup<DataFrameType10>
    val DataRowBase<DataRecord>.pageInfo: org.jetbrains.dataframe.DataRow<DataFrameType10> @JvmName("DataRecord_pageInfo") get() = this["pageInfo"] as org.jetbrains.dataframe.DataRow<DataFrameType10>

    fun generateExtensionProperties(): List<String> {
        val types = listOf(
                DataFrameType1::class,
                DataFrameType2::class,
                DataFrameType3::class,
                DataFrameType4::class,
                DataFrameType5::class,
                DataFrameType6::class,
                DataFrameType7::class,
                DataFrameType8::class,
                DataFrameType9::class,
                DataFrameType10::class,
                DataRecord::class)
        val codeGen = ReplCodeGenerator.create()
        return types.map { codeGen.process(it) }
    }

    val path = "data/playlistItems.json"
    val df = DataFrame.read(path)
    val typed = df.typed<DataRecord>()
    val item = typed.items[0]

    @Test
    fun `deep update`() {

        val updated = item.update { snippet.thumbnails.default.url }.with {Image(it)}
        updated.snippet.thumbnails.default.url.type shouldBe getType<Image>()
    }

    @Test
    fun `deep update group`() {

        val updated = item.update { snippet.thumbnails.default }.with { it.url }
        updated.snippet.thumbnails["default"].type shouldBe getType<String>()
    }

    @Test
    fun `deep batch update`() {

        val updated = item.update { snippet.thumbnails.default.url and snippet.thumbnails.high.url }.with { Image(it) }
        updated.snippet.thumbnails.default.url.type shouldBe getType<Image>()
        updated.snippet.thumbnails.high.url.type shouldBe getType<Image>()
    }

    @Test
    fun `deep batch update all`() {

        val updated = item.update { colsDfs { it.name == "url" } }.with { (it as? String)?.let{ Image(it) } }
        updated.snippet.thumbnails.default.url.type shouldBe getType<Image>()
        updated.snippet.thumbnails.maxres.url.type shouldBe getType<Image?>()
        updated.snippet.thumbnails.standard.url.type shouldBe getType<Image?>()
        updated.snippet.thumbnails.medium.url.type shouldBe getType<Image>()
        updated.snippet.thumbnails.high.url.type shouldBe getType<Image>()
    }

    @Test
    fun `select group`(){

        item.select { snippet.thumbnails.default }.ncol() shouldBe 1
        item.select { snippet.thumbnails.default.all() }.ncol() shouldBe 3
    }

    @Test
    fun `deep remove`() {

        val item2 = item.remove { snippet.thumbnails.default and snippet.thumbnails.maxres and snippet.channelId and etag }
        item2.ncol() shouldBe item.ncol() - 1
        item2.snippet.ncol() shouldBe item.snippet.ncol() - 1
        item2.snippet.thumbnails.ncol() shouldBe item.snippet.thumbnails.ncol() - 2
    }

    @Test
    fun `remove all from group`() {

        val item2 = item.remove { snippet.thumbnails.default and snippet.thumbnails.maxres and snippet.thumbnails.medium and snippet.thumbnails.high and snippet.thumbnails.standard }
        item2.snippet.ncol() shouldBe item.snippet.ncol() - 1
        item2.snippet.tryGetColumnGroup("thumbnails") shouldBe null
    }

    @Test
    fun `deep move with rename`() {

        val moved = item.move { snippet.thumbnails.default }.into { snippet + "movedDefault" }
        moved.snippet.thumbnails.columnNames() shouldBe item.snippet.thumbnails.remove { default }.columnNames()
        moved.snippet.ncol() shouldBe item.snippet.ncol() + 1
        (moved.snippet["movedDefault"] as MapColumn<*>).ncol() shouldBe item.snippet.thumbnails.default.ncol()
    }

    @Test
    fun `union`(){
        val merged = item.union(item)
        merged.nrow() shouldBe item.nrow() * 2
        val group = merged.snippet
        group.nrow() shouldBe item.snippet.nrow() * 2
        group.columnNames() shouldBe item.snippet.columnNames()
    }

    @Test
    fun `select with rename`(){
        val selected = item.select { snippet.thumbnails.default.url into "default" and snippet.thumbnails.maxres.url("maxres") }
        selected.columnNames() shouldBe listOf("default", "maxres")
        selected["default"].toList() shouldBe item.snippet.thumbnails.default.url.toList()
        selected["maxres"].toList() shouldBe item.snippet.thumbnails.maxres.url.toList()
    }

    @Test
    fun `aggregate by column`(){

        val res = typed.aggregate { items }.with {
            minBy { snippet.publishedAt }.snippet into "earliest"
        }

        res.ncol() shouldBe typed.ncol() + 1
        res.getColumnIndex("earliest") shouldBe typed.getColumnIndex("items") + 1

        val expected = typed.items.map { it.snippet.minBy { publishedAt } }.toList()
        res["earliest"].toList() shouldBe expected
    }
}