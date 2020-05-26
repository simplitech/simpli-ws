package br.com.simpli.handler

import br.com.simpli.util.resolveRegion
import com.amazonaws.regions.Regions
import com.amazonaws.services.simpleemail.model.EventType
import com.amazonaws.services.sns.message.*
import com.google.gson.GsonBuilder
import com.google.gson.JsonSyntaxException
import com.google.gson.annotations.SerializedName
import org.apache.logging.log4j.LogManager
import java.io.InputStream

class SESNotificationHandler : SnsMessageHandler {

    constructor(region: String) : this(null, region)
    constructor(region: Regions) : this(region, null)
    constructor() : this(null, null)

    private constructor(regionEnum: Regions?, regionString: String?) : super() {
        manager = SnsMessageManager(resolveRegion(regionEnum, regionString).getName())
    }

    private val manager: SnsMessageManager
    private var message: Message? = null

    companion object {
        private val logger = LogManager.getLogger(SESNotificationHandler::class.java)
        private val gson = GsonBuilder().disableHtmlEscaping().create()
    }

    override fun handle(notification: SnsNotification?) {
        notification?.also {
            message = try {
                gson.fromJson(it.message, Message::class.java)
            } catch (e: JsonSyntaxException) {
                null
            }
        }
    }

    override fun handle(notification: SnsSubscriptionConfirmation?) {
        notification?.apply {
            runCatching { confirmSubscription() }
                .onSuccess { logger.info("Subscribed to SNS topic $topicArn") }
                .onFailure {
                    logger.warn("Subscription to SNS topic $topicArn failed")
                    throw it
                }
        }
    }

    override fun handle(notification: SnsUnsubscribeConfirmation?) {
        notification?.apply {
            logger.warn("Subscription to SNS topic $topicArn canceled")
        }
    }

    override fun handle(notification: SnsUnknownMessage?) {
        notification?.apply {
            logger.warn("Unknown SNS message received from $topicArn")
        }
    }

    fun handle(stream: InputStream): Message? {
        manager.handleMessage(stream, this)

        // Reads only once
        return this.message?.also {
            this.message = null
        }
    }

    class Message {
        private val mail: Mail? = null
        private val complaint: Complaint? = null
        private val bounce: Bounce? = null

        @SerializedName("eventType")
        val type: EventType? = null

        val tags get() = mail?.tags ?: emptyMap()
        val emails get() = when(type) {
            EventType.Bounce -> {
                bounce?.bouncedRecipients?.mapNotNull { it.emailAddress }
            }
            EventType.Complaint -> {
                complaint?.complainedRecipients?.mapNotNull { it.emailAddress }
            }
            else -> null
        } ?: emptyList()
    }

    private class Mail {
        val tags: Map<String, List<String>> = emptyMap()
    }

    private class Bounce {
        val bouncedRecipients: List<Recipient> = emptyList()
    }

    private class Complaint {
        val complainedRecipients: List<Recipient> = emptyList()
    }

    private class Recipient {
        val emailAddress: String? = null
    }

}