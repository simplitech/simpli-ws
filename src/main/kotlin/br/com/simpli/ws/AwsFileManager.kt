package br.com.simpli.ws

import com.amazonaws.HttpMethod
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.auth.PropertiesCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.ec2.util.S3UploadPolicy
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest
import com.amazonaws.util.ClassLoaderHelper
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.*

/**
 * @author JoaoLippi
 */
class AwsFileManager {

    private val bucketName: String
    private val provider: AWSCredentialsProvider
    private val client: AmazonS3
    private val region: Regions
    private val expireInHours: Long = 24

    constructor(bucketName: String, region: String, credentialsFileName: String = "/AwsCredentials.properties") :
            this(bucketName, Regions.fromName(region.toLowerCase().replace('_', '-')), credentialsFileName)

    @JvmOverloads
    constructor(bucketName: String, region: Regions = Regions.US_EAST_1, credentialsFileName: String = "/AwsCredentials.properties") {
        this.bucketName = bucketName
        this.region = region

        provider = try {
            AWSStaticCredentialsProvider(PropertiesCredentials(ClassLoaderHelper.getResourceAsStream(credentialsFileName)))
        } catch (e: Exception) {
            DefaultAWSCredentialsProviderChain()
        }

        client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(provider)
                .withRegion(region)
                .build()
    }

    @JvmOverloads
    fun initiateMultipartUpload(folder: String? = null, filename: String): String {
        return client.initiateMultipartUpload(
                InitiateMultipartUploadRequest(bucketName, getPath(folder, filename))
        ).uploadId
    }

    @JvmOverloads
    fun getPresignedUrl(folder: String? = null, filename: String, contentType: String? = null, httpMethod: HttpMethod = HttpMethod.PUT): String {
        return getPresigned(folder, filename, contentType, httpMethod)
    }

    @JvmOverloads
    fun getUploadCredentials(folder: String? = null, filename: String): Array<String> {
        val policy = S3UploadPolicy(provider.credentials.awsAccessKeyId, provider.credentials.awsSecretKey, bucketName, getPath(folder, filename), 10)
        return arrayOf(policy.policyString, policy.policySignature)
    }

    @JvmOverloads
    fun upload(folder: String? = null, filename: String, input: InputStream): String {
        val path = getPath(folder, filename)
        val endpoint = getEndpoint()

        client.putObject(PutObjectRequest(bucketName, path, input, ObjectMetadata()))
        return "$endpoint$bucketName/$path"
    }

    @JvmOverloads
    fun upload(folder: String? = null, filename: String, file: ByteArray): String {
        return upload(folder, filename, ByteArrayInputStream(file))
    }

    @JvmOverloads
    fun listFiles(folder: String? = null): List<String> {
        return (folder?.run { client.listObjects(bucketName, this) } ?: client.listObjects(bucketName))
                .objectSummaries
                .filter { it.size > 0 }
                .map { it.key }
    }

    private fun getPresigned(folder: String?, filename: String, contentType: String?, httpMethod: HttpMethod): String {
        return client.generatePresignedUrl(
                GeneratePresignedUrlRequest(bucketName, getPath(folder, filename)).also {
                    it.method = httpMethod
                    it.expiration = getExpiration()
                    contentType?.apply {
                        it.contentType = this
                    }
                }
        ).toExternalForm()
    }

    private fun getPath(vararg prefixes: String?): String {
        return prefixes.filterNotNull().joinToString("/")
    }

    private fun getExpiration(): Date {
        return Date.from(LocalDateTime.now().plusHours(expireInHours).atZone( ZoneId.systemDefault()).toInstant())
    }

    private fun getEndpoint() = "https://s3-${region.getName()}.amazonaws.com/"
}
