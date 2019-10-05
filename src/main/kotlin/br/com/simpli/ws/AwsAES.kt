package br.com.sharity.temp

import com.amazonaws.auth.*
import com.amazonaws.regions.Regions
import com.amazonaws.util.ClassLoaderHelper.getResourceAsStream
import com.google.gson.GsonBuilder
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder
import java.net.URI

/**
 *
 * @author JoaoLippi
 */
class AwsAES : RestHighLevelClient {

    constructor(endpoint: String, region: String, credentialsFileName: String = "/AwsCredentials.properties")
            : this(endpoint, Regions.fromName(region.toLowerCase().replace('_','-')), credentialsFileName)

    @JvmOverloads
    constructor(endpoint: String, region: Regions = Regions.US_EAST_1, credentialsFileName: String = "/AwsCredentials.properties"): super(
            RestClient.builder(HttpHost(URI(endpoint).host, URI(endpoint).port, URI(endpoint).scheme))
                    .setHttpClientConfigCallback {
                        it.addInterceptorLast(
                                AWSRequestSigningApacheInterceptor("es", AWS4Signer().also {
                                    it.serviceName = "es"
                                    it.regionName = region.getName()
                                }, try {
                                    val properties = getResourceAsStream(credentialsFileName)
                                    AWSStaticCredentialsProvider(PropertiesCredentials(properties)).credentials
                                } catch (e: Exception) {
                                    DefaultAWSCredentialsProviderChain().credentials
                                })
                        )
                    }
    )

    fun <T: Any> index(index: String, id: String, obj: T) {
        this.index(IndexRequest(index).id(id).source(obj.toJSON(), XContentType.JSON), RequestOptions.DEFAULT)
    }

    fun <T: Any> update(index: String, id: String, obj: T) {
        this.update(UpdateRequest(index, id).doc(obj.toJSON(), XContentType.JSON), RequestOptions.DEFAULT)
    }

    fun delete(index: String, id: String) {
        this.delete(DeleteRequest(index, id), RequestOptions.DEFAULT)
    }

    fun <T: Any> bulkIndex(index: String, map: Map<String, T>) {
        this.bulk(
                BulkRequest().apply {
                    map.forEach { add(IndexRequest(index).id(it.key).source(it.value.toJSON(), XContentType.JSON)) }
                } ,RequestOptions.DEFAULT
        )
    }

    fun <T: Any> bulkUpdate(index: String, map: Map<String, T>) {
        this.bulk(
                BulkRequest().apply {
                    map.forEach { add(UpdateRequest(index, it.key).doc(it.value.toJSON(), XContentType.JSON)) }
                } ,RequestOptions.DEFAULT
        )
    }

    fun bulkDelete(index: String, ids: List<String>) {
        this.bulk(
                BulkRequest().apply {
                    ids.forEach { add(DeleteRequest(index, it)) }
                } ,RequestOptions.DEFAULT
        )
    }

    inline fun <reified T: Any> search(index: String, query: QueryBuilder, limit: Int = 10, page: Int = 0): List<T> {
        return this.search(
                SearchRequest(index)
                        .source(
                                SearchSourceBuilder()
                                        .query(query)
                                        .from(limit * page)
                                        .size(limit)
                        )
                , RequestOptions.DEFAULT)
                .hits.map {
            GsonBuilder().create().fromJson(it.sourceAsString, T::class.java)
        }
    }

    private fun <T: Any> T.toJSON(): String {
        return GsonBuilder().create().toJson(this, this.javaClass)
    }

}
