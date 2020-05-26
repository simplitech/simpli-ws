package br.com.simpli.ws

import br.com.simpli.util.resolveCredentials
import br.com.simpli.util.resolveRegion
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.simpleemail.AmazonSimpleEmailService
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder
import com.amazonaws.services.simpleemail.model.SendEmailRequest

/**
 *
 * @author ricardoprado
 */
class AwsSES  {

    private val provider: AWSCredentialsProvider
    private var sesClient: AmazonSimpleEmailService

    constructor(region: String, credentialsFileName: String) :
            this(null, region, credentialsFileName)

    constructor(region: Regions, credentialsFileName: String):
            this(region, null, credentialsFileName)

    constructor(region: String) :
            this(null, region, null)

    constructor(region: Regions) :
            this(region, null, null)

    constructor() :
            this(null, null, null)

    private constructor(regionEnum: Regions?, regionString: String?, credentialsFileName: String?) {
        provider = resolveCredentials(credentialsFileName)
        sesClient = buildClient(resolveRegion(regionEnum, regionString).getName())
    }


    fun setRegion(region: Regions) {
        setRegion(region.getName())
    }

    fun setRegion(region: String) {
        sesClient = buildClient(region)
    }

    fun sendEmail(request: SendEmailRequest) {
        sesClient.sendEmail(request)
    }

    private fun buildClient(region: String): AmazonSimpleEmailService {
        return AmazonSimpleEmailServiceClientBuilder.standard()
            .withRegion(region)
            .withCredentials(provider)
            .build()
    }
}
