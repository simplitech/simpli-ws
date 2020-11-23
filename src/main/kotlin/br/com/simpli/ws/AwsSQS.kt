package br.com.simpli.ws

import br.com.simpli.util.resolveCredentials
import br.com.simpli.util.resolveRegion
import com.amazonaws.regions.Regions
import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.amazonaws.services.sqs.model.Message

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

    fun readAndDelete(action: (Message) -> Boolean) = client.receiveMessage(endpoint).messages.forEach {
        if (action(it)) client.deleteMessage(endpoint, it.receiptHandle)
    }
}