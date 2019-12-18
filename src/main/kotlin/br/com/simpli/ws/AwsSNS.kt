package br.com.simpli.ws

import com.amazonaws.auth.PropertiesCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.services.sns.model.PublishRequest
import br.com.simpli.tools.Validator
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.sns.AmazonSNS
import com.amazonaws.services.sns.AmazonSNSClientBuilder
import com.amazonaws.util.ClassLoaderHelper.getResourceAsStream

/**
 *
 * @author ricardoprado
 */
class AwsSNS {

    private val provider: AWSCredentialsProvider
    private var snsClient: AmazonSNS

    constructor(region: String, credentialsFileName: String)
            : this(Regions.fromName(region.toLowerCase().replace('_', '-')), credentialsFileName)

    @JvmOverloads
    constructor(region: Regions = Regions.US_EAST_1, credentialsFileName: String) {

        provider = try {
            AWSStaticCredentialsProvider(PropertiesCredentials(getResourceAsStream(credentialsFileName)))
        } catch (e: Exception) {
            DefaultAWSCredentialsProviderChain()
        }

        snsClient = AmazonSNSClientBuilder.standard()
                .withRegion(region)
                .withCredentials(provider)
                .build()
    }

    @Deprecated("Method in portuguese has been deprecated.", ReplaceWith("sendSMS(recipient, message)"))
    fun enviarSMS(destinatario: String, mensagem: String) = sendSMS(destinatario, mensagem)

    fun sendSMS(recipient: String, message: String) {
        if (!Validator.isValidPhoneNumber(recipient)) {
            return
        }

        val publishRequest = PublishRequest()
                .withMessage(message)
                .withPhoneNumber(recipient)
        snsClient.publish(publishRequest)
    }

}
