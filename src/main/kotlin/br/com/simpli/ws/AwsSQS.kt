package br.com.wowtalents.util

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.auth.PropertiesCredentials
import com.amazonaws.regions.DefaultAwsRegionProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.amazonaws.services.sqs.model.Message
import com.amazonaws.util.ClassLoaderHelper

class AwsSQS {
    private val client: AmazonSQS
    private val endpoint: String

    constructor(endpoint: String, region: String, credentialsFileName: String) :
        this(endpoint, null, region, credentialsFileName)

    constructor(endpoint: String, region: Regions, credentialsFileName: String) :
        this(endpoint, region, null, credentialsFileName)

    constructor(endpoint: String, region: String) :
        this(endpoint, null, region, null)

    constructor(endpoint: String, region: Regions) :
        this(endpoint, region, null, null)

    constructor(endpoint: String):
        this(endpoint, null, null, null)

    private constructor(endpoint: String, regionEnum: Regions?, regionString: String?, credentialsFileName: String?) {
        this.endpoint = endpoint
        this.client = AmazonSQSClientBuilder
            .standard()
            .withRegion(resolveRegion(regionEnum, regionString))
            .withCredentials(resolveCredentials(credentialsFileName))
            .build()
    }

    fun send(message: Any) {
        client.sendMessage(endpoint, message.toString())
    }

    fun read(action: (Message) -> Unit) = client.receiveMessage(endpoint).messages.forEach(action)

    fun readAndDelete(action: (Message) -> Unit) = client.receiveMessage(endpoint).messages.forEach {
        action(it)
        client.deleteMessage(endpoint, it.receiptHandle)
    }

    private fun resolveRegion(enum: Regions?, string: String?): Regions {
        // Tries to resolve given Region enum first, then string, then default provider
        return enum ?: string?.run {
            try {
                Regions.fromName(this.toLowerCase().replace('_', '-'))
            } catch (e: IllegalArgumentException) {
                null
            }
        } ?: Regions.fromName(DefaultAwsRegionProviderChain().region)
    }

    private fun resolveCredentials(path: String?): AWSCredentialsProvider {
        // If path is given, tries to get from path first
        return path?.run {
            val pathAdjusted = if (!startsWith('/')) "/$this" else this

            try {
                val properties = ClassLoaderHelper.getResourceAsStream(pathAdjusted)
                AWSStaticCredentialsProvider(PropertiesCredentials(properties))
            } catch (e: Exception) {
                null
            }
        } ?: DefaultAWSCredentialsProviderChain()
    }
}