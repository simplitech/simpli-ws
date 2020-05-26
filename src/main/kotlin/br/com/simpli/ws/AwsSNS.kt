package br.com.simpli.ws

import br.com.simpli.tools.Validator
import br.com.simpli.util.resolveCredentials
import br.com.simpli.util.resolveRegion
import com.amazonaws.regions.Regions
import com.amazonaws.services.sns.AmazonSNS
import com.amazonaws.services.sns.AmazonSNSClientBuilder
import com.amazonaws.services.sns.model.PublishRequest

/**
 *
 * @author ricardoprado
 */
class AwsSNS {

    private val snsClient: AmazonSNS

    constructor(region: String, credentialsFileName: String) :
            this(null, region, credentialsFileName)

    constructor(region: Regions, credentialsFileName: String) :
            this(region, null, credentialsFileName)

    constructor(region: String) :
            this(null, region, null)

    constructor(region: Regions) :
            this(region, null, null)

    constructor() :
            this(null, null, null)

    private constructor(regionEnum: Regions?, regionString: String?, credentialsFileName: String?) {
        snsClient = AmazonSNSClientBuilder.standard()
            .withRegion(resolveRegion(regionEnum, regionString))
            .withCredentials(resolveCredentials(credentialsFileName))
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
