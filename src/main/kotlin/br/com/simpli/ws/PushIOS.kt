package br.com.simpli.ws

import javapns.Push
import javapns.communication.exceptions.CommunicationException
import javapns.communication.exceptions.KeystoreException
import javapns.notification.Payload
import java.util.ArrayList
import java.util.logging.Level
import javapns.notification.PushNotificationPayload
import javapns.notification.PushedNotification
import javapns.notification.PushedNotifications
import javapns.test.NotificationTest
import org.json.JSONException

class PushIOS(val keystoreIOS: String, val senhaKeyStore: String) {

    interface ICallbackInvalidTokenPush {
        fun invalidTokens(unsuccessfull: List<PushedNotification>, expections: List<PushedNotification>, responses: List<PushedNotification>)
    }

    companion object {
        @JvmStatic
        fun BuildFromFiles(keystoreIOS: String, senhaKeyStore: String): PushIOS {
            val classloader = Thread.currentThread().contextClassLoader
            return PushIOS(classloader.getResource(keystoreIOS).file, senhaKeyStore)
        }
    }

    fun send(text: String, production: Boolean, vararg idTokens: String) {
        send(text, production, idTokens.toList())
    }

    fun send(text: String, production: Boolean, idTokens: List<String>) {
        send(text, production, 0, idTokens)
    }

    fun send(text: String, production: Boolean, badge: Int, idTokens: List<String>) {
        send(text, production, badge, null, idTokens)
    }
    
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

            callback?.let { it.invalidTokens(erroneous, exceptions, responsePackages) }

        } catch (ex: JSONException) {
            java.util.logging.Logger.getLogger(PushIOS::class.java.getName()).log(Level.SEVERE, "Erro ao enviar push:" + ex.localizedMessage, ex)
        } catch (ex: CommunicationException) {
            java.util.logging.Logger.getLogger(PushIOS::class.java.getName()).log(Level.SEVERE, "Erro ao enviar push:" + ex.localizedMessage, ex)
        } catch (ex: KeystoreException) {
            java.util.logging.Logger.getLogger(PushIOS::class.java.getName()).log(Level.SEVERE, "Erro ao enviar push:" + ex.localizedMessage, ex)
        }

    }






    fun send(payload: Payload, production: Boolean, vararg idTokens: String) {
        send(payload, production, idTokens.toList())
    }

    fun send(payload: Payload, production: Boolean, idTokens: List<String>) {
        send(payload, production, 0, idTokens)
    }

    fun send(payload: Payload, production: Boolean, badge: Int, idTokens: List<String>) {
        send(payload, production, badge, null, idTokens)
    }

    fun send(payload: Payload, production: Boolean, badge: Int, callback: ICallbackInvalidTokenPush?, idTokens: List<String>) {
        try {
            val pushed = Push.payload(payload, keystoreIOS, senhaKeyStore, production, idTokens)

            if (!production) {
                NotificationTest.printPushedNotifications(pushed)
            }
        } catch (ex: CommunicationException) {
            java.util.logging.Logger.getLogger(PushIOS::class.java!!.getName()).log(Level.SEVERE, ex.message, ex)
        } catch (ex: KeystoreException) {
            java.util.logging.Logger.getLogger(PushIOS::class.java!!.getName()).log(Level.SEVERE, ex.message, ex)
        }
    }
    
}