package br.com.simpli.ws

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.auth.PropertiesCredentials
import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
import com.amazonaws.services.simpleemail.AmazonSimpleEmailService
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder
import com.amazonaws.services.simpleemail.model.SendEmailRequest
import com.amazonaws.util.ClassLoaderHelper.getResourceAsStream

/**
 *
 * @author ricardoprado
 */
class AwsSES  {

    private val provider: AWSCredentialsProvider
    private var sesClient: AmazonSimpleEmailService

    constructor(region: String, credentialsFileName: String = "/AwsCredentials.properties") :
            this(Regions.fromName(region.toLowerCase().replace('_', '-')), credentialsFileName)

    @JvmOverloads
    constructor(region: Regions = Regions.US_EAST_1, credentialsFileName: String = "/AwsCredentials.properties") {

        provider = try {
            AWSStaticCredentialsProvider(PropertiesCredentials(getResourceAsStream(credentialsFileName)))
        } catch (e: Exception) {
            DefaultAWSCredentialsProviderChain()
        }

        sesClient = AmazonSimpleEmailServiceClientBuilder.standard()
            .withRegion(region)
            .withCredentials(provider)
            .build()
    }


    fun setRegion(region: Region) {
        sesClient = AmazonSimpleEmailServiceClientBuilder.standard()
            .withRegion(region.name)
            .withCredentials(provider)
            .build()
    }

    fun sendEmail(request: SendEmailRequest) {
        sesClient.sendEmail(request)
    }
}
