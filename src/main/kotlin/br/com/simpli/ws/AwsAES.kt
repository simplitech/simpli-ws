package br.com.simpli.ws

import com.amazonaws.auth.*
import com.amazonaws.http.HttpMethodName
import com.amazonaws.regions.Regions
import com.amazonaws.util.ClassLoaderHelper.getResourceAsStream
import com.google.gson.GsonBuilder
import org.apache.http.HttpHost
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.elasticsearch.client.Request
import org.elasticsearch.client.RestClient
import java.io.IOException
import java.net.URI
import java.util.logging.Level
import java.util.logging.Logger

/**
 *
 * @author JoaoLippi
 */
class AwsAES {
    private val SERVICE_NAME = "es"

    private var credentials: AWSCredentials? = null
    private var aesClient: RestClient? = null
    private val region: String

    constructor(endpoint: String, credentialsFileName: String = "/AwsCredentials.properties", region: String = "US_EAST_1")
        : this(endpoint, credentialsFileName, Regions.fromName(region.toLowerCase().replace('_','-')))

    @JvmOverloads
    constructor(endpoint: String, credentialsFileName: String? = "/AwsCredentials.properties", region: Regions = Regions.US_EAST_1) {
        this.region = region.getName()

        try {
            credentials = try {
                val properties = getResourceAsStream(credentialsFileName)
                AWSStaticCredentialsProvider(PropertiesCredentials(properties)).credentials
            } catch (e: Exception) {
                DefaultAWSCredentialsProviderChain().credentials
            }?.also { credentials ->
                val signer = AWS4Signer().also {
                    it.serviceName = SERVICE_NAME
                    it.regionName = this.region
                }

                val uri = endpoint.toURI()

                aesClient = RestClient.builder(HttpHost(uri.host, uri.port, uri.scheme))
                    .setHttpClientConfigCallback {
                        it.addInterceptorLast(
                            AWSRequestSigningApacheInterceptor(SERVICE_NAME, signer, credentials)
                        )
                    }
                    .build()
            }

        } catch (e: IOException) {
            Logger.getLogger(AwsAES::class.java.name).log(Level.SEVERE, null, e)
        }
    }

    fun execute(request: ElasticRequest) {
        execute(request.build())
    }

    fun bulk(requests: List<ElasticRequest>) {
        execute(requests.buildAsBulk())
    }

    fun bulk(vararg requests: ElasticRequest){
        execute(requests.toList().buildAsBulk())
    }

    private fun execute(req: Request) {
        aesClient?.performRequest(req)
    }

    abstract class ElasticRequest {
        abstract val index: String
        abstract val id: String
        protected abstract val method: HttpMethodName
        internal abstract val action: Action

        internal open fun build(): Request {
            return Request(method.name, "/$index/_doc/$id")
        }

        internal open fun toBulkString(): String {
            return "{\"${action.getName()}\": {\"_index\": \"$index\", \"_id\": \"$id\" } }\n"
        }
    }

    class IndexRequest<T: Any>(override val index: String, override val id: String, val obj: T) : ElasticRequest() {
        override val method = HttpMethodName.PUT
        override val action = Action.INDEX
        private val type = obj.javaClass

        private val jsonString = GsonBuilder().create().toJson(obj, type)

        override fun build(): Request {
            return super.build().apply {
                entity = NStringEntity(jsonString, ContentType.APPLICATION_JSON)
            }
        }

        override fun toBulkString(): String {
            return "${super.toBulkString()}$jsonString\n"
        }
    }

    class DeleteRequest(override val index: String, override val id: String) : ElasticRequest() {
        override val method = HttpMethodName.DELETE
        override val action = Action.DELETE
    }

    fun List<ElasticRequest>.buildAsBulk(): Request {
        val strBuilder = StringBuilder()
        for (req in this) {
            strBuilder.append(req.toBulkString())
        }

        return Request(HttpMethodName.POST.name, "/_bulk").apply {
            entity = NStringEntity(strBuilder.toString(), ContentType.APPLICATION_JSON)
        }
    }

    internal enum class Action {
        INDEX, DELETE;

        fun getName(): String {
            return this.name.toLowerCase()
        }
    }

    private fun String.toURI(): URI {
        val match = Regex("^(?:(\\w*?)://)?(.*?)(?::(\\d+))?(/.*?)?(?:\\?(.*?))?(?:#(.*?))?\$").matchEntire(this)

        val scheme = match?.groupValues?.get(1)?.nullIfEmpty() ?: "https"
        val endpoint = match?.groupValues?.get(2)
        val port = match?.groupValues?.get(3)?.toIntOrNull() ?: -1
        val path = match?.groupValues?.get(4)?.nullIfEmpty()
        val query = match?.groupValues?.get(5)?.nullIfEmpty()
        val fragment = match?.groupValues?.get(6)?.nullIfEmpty()

        return URI(scheme, null, endpoint, port, path, query, fragment)
    }

    private fun String.nullIfEmpty(): String? {
        if (this.isEmpty()) {
            return null
        }

        return this
    }

}
