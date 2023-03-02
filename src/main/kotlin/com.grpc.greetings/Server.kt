package com.grpc.greetings

import io.grpc.Server
import io.grpc.ServerBuilder
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map

class GreetingsServer(private val port: Int) {
    val server: Server = ServerBuilder.forPort(port).addService(GreetingService).build()

    fun start() {
        server.start()
        println("Server started listening in $port")
        Runtime.getRuntime().addShutdownHook(
            Thread {
                println("*** shutting down gRPC server since JVM is shutting down")
                stop()
                println("*** server shut down")
            },
        )
    }

    private fun stop() {
        server.shutdown()
    }

    fun blockUntilShutdown() {
        server.awaitTermination()
    }

    private object GreetingService : GreeterGrpcKt.GreeterCoroutineImplBase() {

        override suspend fun sayHello(request: HelloRequest): HelloResponse {
            return helloResponse {
                message = "hola bienvenido ${request.name}"
            }
        }

        override fun serverStreamHello(request: HelloRequest): Flow<HelloResponse> {
            return flow {
                for (x in 0..15) {
                    delay(100)
                    emit(
                        helloResponse {
                            message = "hola bienvenido, server streams hello, ${request.name} - $x"
                        },
                    )
                }
            }
        }

        override suspend fun clientStreamNameToSayHello(requests: Flow<HelloRequest>): HelloResponse {
            val names = mutableListOf<String>()
            requests.collect {
                names.add(it.name)
            }

            return helloResponse {
                message = "hola ${names.joinToString(separator = ",")} bienvenidos todos."
            }
        }

        override fun everyBodyStreamsHello(requests: Flow<HelloRequest>): Flow<HelloResponse> {
            return requests.map { request ->
                println(request)
                helloResponse { message = "hello ${request.name} everybody is saying hi " }
            }
        }
    }
}

fun main(args: Array<String>) {
    val port = 8085
    val server = GreetingsServer(port)

    server.start()
    server.blockUntilShutdown()
}
