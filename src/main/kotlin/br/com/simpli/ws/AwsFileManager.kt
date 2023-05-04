package br.com.simpli.ws

import br.com.simpli.util.resolveCredentials
import br.com.simpli.util.resolveRegion
import com.amazonaws.HttpMethod
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.ec2.util.S3UploadPolicy
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
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
    private val region: Regions
    private val expireInHours: Long = 24
    
    val client: AmazonS3

    constructor(bucketName: String, region: String, credentialsFileName: String) :
            this(bucketName, null, region, credentialsFileName)

    constructor(bucketName: String, region: Regions, credentialsFileName: String) :
            this(bucketName, region, null, credentialsFileName)

    constructor(bucketName: String, region: String) :
            this(bucketName, null, region, null)

    constructor(bucketName: String, region: Regions) :
            this(bucketName, region, null, null)

    constructor(bucketName: String):
            this(bucketName, null, null, null)

    private constructor(bucketName: String, regionEnum: Regions?, regionString: String?, credentialsFileName: String?) {
        this.bucketName = bucketName
        this.region = resolveRegion(regionEnum, regionString)

        provider = resolveCredentials(credentialsFileName)

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
    fun move(folder: String? = null, filename: String, newFolder: String? = null, newFilename: String): String {
        val path = getPath(folder, filename)
        val newPath = getPath(newFolder, newFilename)

        val endpoint = getEndpoint()

        client.copyObject(bucketName, path, bucketName, newPath)
        client.deleteObject(bucketName, path)

        return "$endpoint$bucketName/$newPath"
    }

    @JvmOverloads
    @Deprecated("Renamed", replaceWith = ReplaceWith("list(folder)"))
    fun listFiles(folder: String? = null): List<String> {
        return list(folder)
    }

    @JvmOverloads
    fun list(folder: String? = null): List<String> {
        return (folder?.run { client.listObjects(bucketName, this) } ?: client.listObjects(bucketName))
            .objectSummaries
            .filter { it.size > 0 }
            .map { it.key }
    }

    @JvmOverloads
    fun delete(folder: String? = null, filename: String) {
        client.deleteObject(bucketName, getPath(folder, filename))
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
