package br.com.simpli.ws

import br.com.simpli.model.PageCollection
import com.amazonaws.auth.AWS4Signer
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.auth.PropertiesCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.util.ClassLoaderHelper.getResourceAsStream
import com.amazonaws.util.json.Jackson
import java.io.InputStreamReader
import java.net.URI
import org.apache.http.HttpHost
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortBuilder
import org.elasticsearch.search.sort.SortBuilders

/**
 *
 * @author JoaoLippi
 */
class AwsAES : RestHighLevelClient {

    companion object {
        fun getMapping(file: String): String {
            val stream = InputStreamReader(getResourceAsStream(file))
            return stream.readText().also { stream.close() }
        }
    }

    @PublishedApi internal val mapper = Jackson.getObjectMapper()

    constructor(endpoint: String, region: String, credentialsFileName: String = "/AwsCredentials.properties") :
            this(endpoint, Regions.fromName(region.toLowerCase().replace('_', '-')), credentialsFileName)

    @JvmOverloads
    constructor(endpoint: String, region: Regions = Regions.US_EAST_1, credentialsFileName: String = "/AwsCredentials.properties") : super(
        RestClient.builder(HttpHost(URI(endpoint).host, URI(endpoint).port, URI(endpoint).scheme))
            .setHttpClientConfigCallback {
                it.addInterceptorLast(
                    AWSRequestSigningApacheInterceptor(
                        "es", AWS4Signer().apply {
                            serviceName = "es"
                            regionName = region.getName()
                        }, try {
                            val properties = getResourceAsStream(credentialsFileName)
                            AWSStaticCredentialsProvider(PropertiesCredentials(properties)).credentials
                        } catch (e: Exception) {
                            DefaultAWSCredentialsProviderChain().credentials
                        }
                    )
                )
            }
    )

    fun index(index: String) {
        this.indices().create(CreateIndexRequest(index), RequestOptions.DEFAULT)
    }

    fun delete(index: String) {
        this.indices().delete(DeleteIndexRequest(index), RequestOptions.DEFAULT)
    }

    fun <T : Any> index(index: String, id: String, obj: T) {
        this.index(IndexRequest(index).id(id).source(obj.toJSON(), XContentType.JSON), RequestOptions.DEFAULT)
    }

    fun <T : Any> update(index: String, id: String, obj: T) {
        this.update(UpdateRequest(index, id).doc(obj.toJSON(), XContentType.JSON), RequestOptions.DEFAULT)
    }

    fun delete(index: String, id: String) {
        this.delete(DeleteRequest(index, id), RequestOptions.DEFAULT)
    }

    fun <T : Any> bulkIndex(index: String, map: Map<String, T>) {
        this.bulk(
                BulkRequest().apply {
                    map.forEach { add(IndexRequest(index).id(it.key).source(it.value.toJSON(), XContentType.JSON)) }
                }, RequestOptions.DEFAULT
        )
    }

    fun <T : Any> bulkUpdate(index: String, map: Map<String, T>) {
        this.bulk(
                BulkRequest().apply {
                    map.forEach { add(UpdateRequest(index, it.key).doc(it.value.toJSON(), XContentType.JSON)) }
                }, RequestOptions.DEFAULT
        )
    }

    fun bulkDelete(index: String, ids: List<String>) {
        this.bulk(
                BulkRequest().apply {
                    ids.forEach { add(DeleteRequest(index, it)) }
                }, RequestOptions.DEFAULT
        )
    }

    @JvmOverloads
    inline fun <reified T : Any> search(
        index: String,
        query: QueryBuilder,
        sort: SortBuilder<*>,
        limit: Int = 10,
        page: Int = 0
    ): PageCollection<T> {
        return search(index, listOf(query), listOf(sort), limit, page)
    }

    @JvmOverloads
    inline fun <reified T : Any> search(
        index: String,
        query: QueryBuilder,
        limit: Int = 10,
        page: Int = 0
    ): PageCollection<T> {
        return search(index, listOf(query), limit = limit, page = page)
    }

    @JvmOverloads
    inline fun <reified T : Any> search(
        index: String,
        sort: SortBuilder<*>,
        limit: Int = 10,
        page: Int = 0
    ): PageCollection<T> {
        return search(index, sorts = listOf(sort), limit = limit, page = page)
    }

    @JvmOverloads
    inline fun <reified T : Any> search(
        index: String,
        queries: List<QueryBuilder>,
        sort: SortBuilder<*>,
        limit: Int = 10,
        page: Int = 0
    ): PageCollection<T> {
        return search(index, queries, listOf(sort), limit, page)
    }

    @JvmOverloads
    inline fun <reified T : Any> search(
        index: String,
        query: QueryBuilder,
        sorts: List<SortBuilder<*>>,
        limit: Int = 10,
        page: Int = 0
    ): PageCollection<T> {
        return search(index, listOf(query), sorts, limit, page)
    }

    @JvmOverloads
    inline fun <reified T : Any> search(
        index: String,
        queries: List<QueryBuilder> = listOf(QueryBuilders.matchAllQuery()),
        sorts: List<SortBuilder<*>> = listOf(SortBuilders.fieldSort("_id")),
        limit: Int = 10,
        page: Int = 0
    ): PageCollection<T> {
        val search = this.search(
            SearchRequest(index)
                .source(
                    SearchSourceBuilder()
                        .from(limit * page)
                        .size(limit)
                        .apply {
                            queries.forEach {
                                query(it)
                            }
                        }
                        .apply {
                            sorts.forEach {
                                sort(it)
                            }
                        }
                ), RequestOptions.DEFAULT
        )

        return PageCollection(search.hits.map {
            mapper.readValue(it.sourceAsString, T::class.java)
        }, search.hits.totalHits.value.toInt())
    }

    private fun <T : Any> T.toJSON(): String {
        return mapper.writeValueAsString(this)
    }
}
