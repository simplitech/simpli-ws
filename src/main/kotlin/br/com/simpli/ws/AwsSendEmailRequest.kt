package br.com.simpli.ws

import br.com.simpli.util.resolveRegion
import com.amazonaws.regions.Regions
import com.amazonaws.services.simpleemail.model.*
import freemarker.template.Configuration
import freemarker.template.TemplateExceptionHandler
import org.apache.logging.log4j.LogManager
import java.io.StringWriter

/**
 *
 * @author ricardoprado
 */

open class AwsSendEmailRequest {
    private val region: Regions

    protected var from = ""
    protected var name = from
    protected var to = ""
    protected var bcc = ""
    protected var ListBcc: List<String>? = null
    protected var subject = ""
    protected var host = ""
    protected var body: String = ""
    protected var attachment = ""
    protected var nameAttachment = ""
    protected var mailGroup: String? = null
    protected val tags = HashMap<String, String>()

    constructor(region: Regions) :
            this(region, null)

    constructor(region: String) :
            this(null, region)

    constructor() :
            this(null, null)

    private constructor(regionEnum: Regions?, regionString: String?) {
        this.region = resolveRegion(regionEnum, regionString)
    }


    fun send() {
        // Construct an object to contain the recipient address.
        val destination = Destination().withToAddresses(to)

        // Create the subject and body of the message.
        val subjectParam = Content().withData(subject)
        val textBody = Content().withData(body)
        val bodyParam = Body().withHtml(textBody)

        // Create a message with the specified subject and body.
        val message = Message().withSubject(subjectParam).withBody(bodyParam)

        val tagList = tags.map { MessageTag().withName(it.key).withValue(it.value) }

        // Assemble the email.
        val request = SendEmailRequest()
            .withSource(from)
            .withDestination(destination)
            .withMessage(message)
            .withConfigurationSetName(mailGroup)
            .withTags(tagList)
        AwsSES(region).sendEmail(request)
    }

    /**
     *
     * @param forClassLoader this.getClass()
     * @param model
     * @param templateFilename
     */
    fun setBodyFromTemplate(forClassLoader: Class<*>, model: Map<String, Any>, templateFilename: String) {
        try {
            val temp = getTemplateConfigInstance(forClassLoader).getTemplate(templateFilename)
            val out = StringWriter()
            temp.process(model, out)
            body = out.toString()
        } catch (ex: Exception) {
            logger.warn(ex.message, ex)
        }

    }

    companion object {
        private val logger = LogManager.getLogger(AwsSendEmailRequest::class.java)
        private var templateConfig: Configuration? = null

        /**
         *
         * @param forClassLoader this.getClass()
         * @return
         */
        fun getTemplateConfigInstance(forClassLoader: Class<*>): Configuration {

            return templateConfig ?: run{
                return Configuration(Configuration.VERSION_2_3_22).apply {
                    setClassForTemplateLoading(forClassLoader, "/mail-templates")
                    defaultEncoding = "UTF-8"
                    templateExceptionHandler = TemplateExceptionHandler.RETHROW_HANDLER
                    localizedLookup = false

                    templateConfig = this
                }
            }
        }
    }
}
