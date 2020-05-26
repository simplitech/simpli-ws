package br.com.simpli.ws

import br.com.simpli.tools.ResourceLoader
import com.turo.pushy.apns.ApnsClient
import com.turo.pushy.apns.ApnsClientBuilder
import com.turo.pushy.apns.util.ApnsPayloadBuilder
import com.turo.pushy.apns.util.SimpleApnsPushNotification
import javapns.Push
import javapns.communication.exceptions.CommunicationException
import javapns.communication.exceptions.KeystoreException
import javapns.notification.Payload
import javapns.notification.PushNotificationPayload
import javapns.notification.PushedNotification
import javapns.test.NotificationTest
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.apache.logging.log4j.LogManager
import org.json.JSONException
import java.util.*

class PushIOS private constructor(val keystoreIOS: String, val senhaKeyStore: String, val production: Boolean?, private val legacy: Boolean): AutoCloseable {

    private val client: ApnsClient? = if (!legacy) {
        ApnsClientBuilder().run {
            if (production == true) {
                setApnsServer(ApnsClientBuilder.PRODUCTION_APNS_HOST)
            } else {
                setApnsServer(ApnsClientBuilder.DEVELOPMENT_APNS_HOST)
            }
            setClientCredentials(ResourceLoader.getStreamResource(keystoreIOS), senhaKeyStore)
            build()
        }
    } else {
        null
    }

    constructor(keystoreIOS: String, senhaKeyStore: String, production: Boolean) : this(keystoreIOS, senhaKeyStore, production, false)

    interface ICallbackInvalidTokenPush {
        fun invalidTokens(unsuccessfull: List<PushedNotification>, expections: List<PushedNotification>, responses: List<PushedNotification>)
    }

    companion object {
        private val logger = LogManager.getLogger(PushIOS::class.java)

        @JvmStatic
        @Deprecated(
                "Using javapns is deprecated",
                ReplaceWith("PushIOS(keystoreIOS, senhaKeyStore, production)", "br.com.simpli.ws.PushIOS")
        )
        fun BuildFromFiles(keystoreIOS: String, senhaKeyStore: String): PushIOS {
            val classloader = Thread.currentThread().contextClassLoader
            return PushIOS(classloader.getResource(keystoreIOS).file, senhaKeyStore, null, true)
        }
    }

    @Deprecated("Using javapns is deprecated")
    fun send(text: String, production: Boolean, vararg idTokens: String) {
        send(text, production, idTokens.toList())
    }

    @Deprecated("Using javapns is deprecated")
    fun send(text: String, production: Boolean, idTokens: List<String>) {
        send(text, production, 0, idTokens)
    }

    @Deprecated("Using javapns is deprecated")
    fun send(text: String, production: Boolean, badge: Int, idTokens: List<String>) {
        send(text, production, badge, null, idTokens)
    }

    @Deprecated("Using javapns is deprecated")
    fun send(text: String, production: Boolean, badge: Int, callback: ICallbackInvalidTokenPush?, idTokens: List<String>) {

        val payload = PushNotificationPayload.alert(text)

        try {
            payload.addBadge(badge)

            val erroneous = ArrayList<PushedNotification>()
            val responsePackages = ArrayList<PushedNotification>()
            val exceptions = ArrayList<PushedNotification>()

            val pushes = Push.payload(payload, keystoreIOS, senhaKeyStore, production, idTokens)

            callback?.let {
                for (individualPush in pushes) {
                    if (!individualPush.isSuccessful) {
                        erroneous.add(individualPush)
                    }
                    if (!individualPush.isTransmissionCompleted) {
                        erroneous.add(individualPush)
                    }
                    if (individualPush.response != null) {
                        responsePackages.add(individualPush)
                    }
                    if (individualPush.exception != null) {
                        exceptions.add(individualPush)
                    }
                }
            }

            NotificationTest.printPushedNotifications(pushes)

            callback?.invalidTokens(erroneous, exceptions, responsePackages)

        } catch (ex: JSONException) {
            logger.warn(ex.localizedMessage, ex)
        } catch (ex: CommunicationException) {
            logger.warn( ex.localizedMessage, ex)
        } catch (ex: KeystoreException) {
            logger.warn( ex.localizedMessage, ex)
        }

    }

    @Deprecated("Using javapns is deprecated")
    fun send(payload: Payload, production: Boolean, vararg idTokens: String) {
        send(payload, production, idTokens.toList())
    }

    @Deprecated("Using javapns is deprecated")
    fun send(payload: Payload, production: Boolean, idTokens: List<String>) {
        send(payload, production, 0, idTokens)
    }

    @Deprecated("Using javapns is deprecated")
    fun send(payload: Payload, production: Boolean, badge: Int, idTokens: List<String>) {
        send(payload, production, badge, null, idTokens)
    }

    @Deprecated("Using javapns is deprecated")
    fun send(payload: Payload, production: Boolean, badge: Int, callback: ICallbackInvalidTokenPush?, idTokens: List<String>) {
        try {
            val pushed = Push.payload(payload, keystoreIOS, senhaKeyStore, production, idTokens)

            if (!production) {
                NotificationTest.printPushedNotifications(pushed)
            }
        } catch (ex: CommunicationException) {
            logger.warn( ex.localizedMessage, ex)
        } catch (ex: KeystoreException) {
            logger.warn( ex.localizedMessage, ex)
        }
    }

    @JvmOverloads
    fun send(builder: (ApnsPayloadBuilder) -> ApnsPayloadBuilder, topic: String, badge: Int = 0, tokens: List<String>) {
        this.send(builder, topic, badge, *tokens.toTypedArray())
    }

    @Suppress("BlockingMethodInNonBlockingContext")
    @JvmOverloads
    fun send(builder: (ApnsPayloadBuilder) -> ApnsPayloadBuilder, topic: String, badge: Int = 0, vararg tokens: String) {
        client?.apply {
            val builderObject = ApnsPayloadBuilder().setBadgeNumber(badge)

            val payload = builder(builderObject).buildWithDefaultMaximumLength()

            runBlocking {
                tokens.map {
                    GlobalScope.async {
                        sendNotification(SimpleApnsPushNotification(it, topic, payload)).get()
                    }
                }.awaitAll()
            }

        } ?: if (legacy) throw NotSupportedException("Method not supported on legacy mode.")
    }

    @JvmOverloads
    fun send(message: String, topic: String, badge: Int = 0, tokens: List<String>) {
        this.send(message, topic, badge, *tokens.toTypedArray())
    }

    @Suppress("BlockingMethodInNonBlockingContext")
    @JvmOverloads
    fun send(message: String, topic: String, badge: Int = 0, vararg tokens: String) {
        client?.apply {
            val payload = ApnsPayloadBuilder()
                    .setAlertBody(message)
                    .setBadgeNumber(badge)
                    .buildWithDefaultMaximumLength()

            runBlocking {
                tokens.map {
                    GlobalScope.async {
                        sendNotification(SimpleApnsPushNotification(it, topic, payload)).get()
                    }
                }.awaitAll()
            }

        } ?: if (legacy) throw NotSupportedException("Method not supported on legacy mode.")
    }

    override fun close() {
        client?.close() ?: if (legacy) throw NotSupportedException("Method not supported on legacy mode.")
    }

    class NotSupportedException : Exception {
        constructor() : super()
        constructor(msg: String) : super(msg)
    }
    
}
