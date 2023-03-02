package com.grpc.greetings

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.StatusException
import kotlinx.coroutines.* // ktlint-disable no-wildcard-imports
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import java.io.Closeable
import java.util.concurrent.TimeUnit

class GreetingClient(private val channel: ManagedChannel) : Closeable {
    private val stub = GreeterGrpcKt.GreeterCoroutineStub(channel)

    suspend fun sayHello(nameToGreet: String) = coroutineScope {
        val request = helloRequest { name = nameToGreet }
        try {
            val response = stub.sayHello(request)
            println("sayHello response: ${response.message}")
        } catch (throwable: StatusException) {
            println("error sayHello $throwable status: ${throwable.status}")
        }
    }

    suspend fun serverStreamsHello(nameToGreet: String) = coroutineScope {
        val request = helloRequest { name = nameToGreet }
        try {
            stub.serverStreamHello(request).collect {
                println("serverStreamsHello: ${it.message} response: $it")
            }
        } catch (e: StatusException) {
            println("error serverStreamsHello $e status: ${e.status}")
        }
    }

    suspend fun clientAskForHello(namesToGreet: List<String>) = coroutineScope {
        val request = namesToGreet.asFlow().onEach { delay(10) }.map { nameToGreet -> helloRequest { name = nameToGreet } }

        try {
            val response = stub.clientStreamNameToSayHello(request)
            println("clientAskForHello: ${response.message}  response: $response")
        } catch (e: StatusException) {
            println("error serverStreamsHello $e status: ${e.status}")
        }
    }

    suspend fun everyBodySaysHelloBack(names: List<String>) = coroutineScope {
        val producer = names.asFlow().onEach { delay(100) }.map { helloRequest { name = it } }

        stub.everyBodyStreamsHello(producer).collect { helloResponse ->
            println("everyBodySaysHelloBack ${helloResponse.message}, response: $helloResponse")
        }
    }

    override fun close() {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
    }
}

fun main(args: Array<String>) = runBlocking {
    val channel = ManagedChannelBuilder
        .forAddress("localhost", 8085)
        .usePlaintext()
        .executor(Dispatchers.Default.asExecutor())
        .build()

    val client = GreetingClient(channel)
    client.sayHello("josue Garcia says hello")
    client.serverStreamsHello("josue Garcia")
    client.clientAskForHello(listOf("josue", "cesar", "oscar", "alejandro", "noe"))
    client.everyBodySaysHelloBack(listOf("josue", "cesar", "oscar", "alejandro", "noe"))
}
