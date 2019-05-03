package br.com.simpli.ws

import com.amazonaws.auth.PropertiesCredentials
import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
import com.amazonaws.services.simpleemail.AmazonSimpleEmailService
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClient
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder
import com.amazonaws.services.simpleemail.model.SendEmailRequest
import java.io.IOException
import java.util.logging.Level
import java.util.logging.Logger

/**
 *
 * @author ricardoprado
 */
class AwsSES {
    private var credentials: PropertiesCredentials? = null
    private var sesClient: AmazonSimpleEmailService? = null

    constructor() {
        try {
            sesClient = AmazonSimpleEmailServiceClientBuilder.standard().build()
        } catch(e: Exception) {
            try {
            val properties = AwsFileManager::class.java.getResourceAsStream("/AwsCredentials.properties")

            credentials = PropertiesCredentials(properties)
            sesClient = AmazonSimpleEmailServiceClient(credentials)
            sesClient!!.setRegion(Region.getRegion(Regions.US_WEST_2))
            } catch (ex: IOException) {
                Logger.getLogger(AwsSNS::class.java.getName()).log(Level.SEVERE, null, ex)
            }
        }
    }

    constructor(credentialsFileName: String) {
        try {
            val properties = AwsFileManager::class.java!!.getResourceAsStream(credentialsFileName)
            credentials = PropertiesCredentials(properties)
            sesClient = AmazonSimpleEmailServiceClient(credentials)
            sesClient!!.setRegion(Region.getRegion(Regions.US_EAST_1))
        } catch (ex: IOException) {
            Logger.getLogger(AwsSNS::class.java!!.getName()).log(Level.SEVERE, null, ex)
        }

    }

    fun setRegion(region: Region) {
        sesClient!!.setRegion(region)
    }

    fun sendEmail(request: SendEmailRequest) {
        sesClient!!.sendEmail(request)
    }
}
