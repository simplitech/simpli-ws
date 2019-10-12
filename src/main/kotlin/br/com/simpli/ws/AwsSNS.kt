package br.com.simpli.ws

import com.amazonaws.auth.PropertiesCredentials
import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
import com.amazonaws.services.sns.AmazonSNSClient
import com.amazonaws.services.sns.model.PublishRequest
import br.com.simpli.tools.Validator
import com.amazonaws.services.sns.AmazonSNS
import com.amazonaws.services.sns.AmazonSNSClientBuilder
import java.io.IOException
import java.util.logging.Level
import java.util.logging.Logger

/**
 *
 * @author ricardoprado
 */
class AwsSNS {

    private var credentials: PropertiesCredentials? = null
    private var snsClient: AmazonSNS? = null

    constructor() {
        try {
            snsClient = AmazonSNSClientBuilder.standard().build()
        } catch(e: Exception) {
            try {
                val properties = AwsFileManager::class.java.getResourceAsStream("/AwsCredentials.properties")
                credentials = PropertiesCredentials(properties)
                snsClient = AmazonSNSClient(credentials)
                snsClient!!.setRegion(Region.getRegion(Regions.US_EAST_1))
            } catch (ex: IOException) {
                Logger.getLogger(AwsSNS::class.java.name).log(Level.SEVERE, null, ex)
            }
        }
    }

    constructor(credentialsFileName: String) {
        try {
            val properties = AwsFileManager::class.java!!.getResourceAsStream(credentialsFileName)
            credentials = PropertiesCredentials(properties)
            snsClient = AmazonSNSClient(credentials)
            snsClient!!.setRegion(Region.getRegion(Regions.US_EAST_1))
        } catch (ex: IOException) {
            Logger.getLogger(AwsSNS::class.java.name).log(Level.SEVERE, null, ex)
        }

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
        snsClient!!.publish(publishRequest)
    }

}
