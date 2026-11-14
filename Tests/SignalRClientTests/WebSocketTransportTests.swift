// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import XCTest
import NIOCore
import NIOPosix
import NIOHTTP1
import NIOWebSocket
import WebSocketKit
@testable import SignalRClient

class WebSocketTransportTests: XCTestCase {
    private var logger: Logger!
    private var mockWebSocketConnection: MockWebSocketConnection!
    private var webSocketTransport: WebSocketTransport!

    override func setUp() {
        super.setUp()
        logger = Logger(logLevel: .debug, logHandler: DefaultLogHandler())
        mockWebSocketConnection = MockWebSocketConnection()
        webSocketTransport = WebSocketTransport(accessTokenFactory: nil, logger: logger, headers: [:], websocket: mockWebSocketConnection)
    }

    #if os(Linux)
    func testHubHandlerCanAwaitInvocationWithoutReorderingFollowingHandlers() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let serverSocket = group.next().makePromise(of: WebSocket.self)
        let server = try await ServerBootstrap(group: group)
            .childChannelInitializer { channel in
                let upgrader = NIOWebSocketServerUpgrader(
                    shouldUpgrade: { channel, _ in channel.eventLoop.makeSucceededFuture([:]) },
                    upgradePipelineHandler: { channel, _ in
                        WebSocket.server(on: channel) { socket in
                            socket.onText { socket, text in
                                if text.contains("\"protocol\"") {
                                    socket.send("{}\u{1e}")
                                } else if let data = text.split(separator: "\u{1e}").first?.data(using: .utf8),
                                          let message = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
                                          message["target"] as? String == "nested",
                                          let id = message["invocationId"] as? String {
                                    socket.send("{\"type\":1,\"target\":\"callback\",\"arguments\":[\"third\"]}\u{1e}{\"type\":3,\"invocationId\":\"\(id)\",\"result\":42}\u{1e}")
                                }
                            }
                            serverSocket.succeed(socket)
                        }
                    }
                )
                return channel.pipeline.configureHTTPServerPipeline(
                    withServerUpgrade: (upgraders: [upgrader], completionHandler: { _ in })
                )
            }
            .bind(host: "127.0.0.1", port: 0).get()
        let port = try XCTUnwrap(server.localAddress?.port)
        var options = HttpConnectionOptions()
        options.transport = .webSockets
        options.skipNegotiation = true
        let hub = HubConnectionBuilder()
            .withUrl(url: "http://127.0.0.1:\(port)", options: options)
            .withLogLevel(logLevel: .error)
            .build()
        let recorder = FrameOrderRecorder()
        let handled = expectation(description: "Nested invocation completes before subsequent handlers")
        handled.expectedFulfillmentCount = 3
        await hub.on("callback") { (value: String) in
            if value == "first" {
                do {
                    let result: Int = try await hub.invoke(method: "nested")
                    XCTAssertEqual(result, 42)
                } catch {
                    XCTFail("Nested invocation failed: \(error)")
                }
            }
            await recorder.append(value)
            handled.fulfill()
        }
        var failure: Error?
        do {
            try await hub.start()
            let socket = try await serverSocket.futureResult.get()
            try await socket.eventLoop.submit {
                socket.send("{\"type\":1,\"target\":\"callback\",\"arguments\":[\"first\"]}\u{1e}{\"type\":1,\"target\":\"callback\",\"arguments\":[\"second\"]}\u{1e}")
            }.get()
            await fulfillment(of: [handled], timeout: 3)
            let events = await recorder.events
            XCTAssertEqual(events, ["first", "second", "third"])
        } catch {
            failure = error
        }
        await hub.processIncomingData(.string("{\"type\":3,\"invocationId\":\"1\",\"result\":42}\u{1e}"))
        await hub.stop()
        try? await server.close().get()
        try await group.shutdownGracefully()
        if let failure { throw failure }
    }

    func testReceivePreservesWireOrderWhenHandlerSuspendsAndSocketCloses() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let serverSocket = group.next().makePromise(of: WebSocket.self)
        let server = try await ServerBootstrap(group: group)
            .childChannelInitializer { channel in
                let upgrader = NIOWebSocketServerUpgrader(
                    shouldUpgrade: { channel, _ in channel.eventLoop.makeSucceededFuture([:]) },
                    upgradePipelineHandler: { channel, _ in
                        WebSocket.server(on: channel) { socket in serverSocket.succeed(socket) }
                    }
                )
                return channel.pipeline.configureHTTPServerPipeline(
                    withServerUpgrade: (upgraders: [upgrader], completionHandler: { _ in })
                )
            }
            .bind(host: "127.0.0.1", port: 0).get()
        let port = try XCTUnwrap(server.localAddress?.port)
        let transport = WebSocketTransport(accessTokenFactory: nil, logger: logger, headers: [:])
        let recorder = FrameOrderRecorder()
        let received = expectation(description: "Messages complete in wire order before close")
        received.expectedFulfillmentCount = 4
        await transport.onReceive { data in
            switch data {
            case .string(let text):
                if text == "first" { try? await Task.sleep(nanoseconds: 100_000_000) }
                await recorder.append(text)
            case .data(let bytes):
                await recorder.append(String(decoding: bytes, as: UTF8.self))
            }
            received.fulfill()
        }
        await transport.onClose { _ in
            await recorder.append("closed")
            received.fulfill()
        }
        var failure: Error?
        do {
            try await transport.connect(url: "http://127.0.0.1:\(port)", transferFormat: .text)
            let socket = try await serverSocket.futureResult.get()
            try await socket.eventLoop.submit {
                socket.send("first")
                socket.send("second")
                var buffer = ByteBufferAllocator().buffer(capacity: 5)
                buffer.writeString("third")
                socket.send(buffer, opcode: .binary)
                socket.close(promise: nil)
            }.get()
            await fulfillment(of: [received], timeout: 5)
            let events = await recorder.events
            XCTAssertEqual(events, ["first", "second", "third", "closed"])
        } catch {
            failure = error
        }
        await transport.onClose(nil)
        try? await transport.stop(error: nil)
        try? await server.close().get()
        try await group.shutdownGracefully()
        if let failure { throw failure }
    }

    #endif

    func testConnect() async throws {
        let url = "http://example.com"
        try await webSocketTransport.connect(url: url, transferFormat: .text)
        XCTAssertTrue(mockWebSocketConnection.connectCalled)
    }

    func testSend() async throws {
        let data = StringOrData.string("test message")
        try await webSocketTransport.send(data)
        XCTAssertEqual(mockWebSocketConnection.sentData, data)
    }

    func testStop() async throws {
        try await webSocketTransport.stop(error: nil)
        XCTAssertTrue(mockWebSocketConnection.stopCalled)
    }

    func testOnReceive() async {
        let expectation = XCTestExpectation(description: "onReceive handler called")
        await webSocketTransport.onReceive { data in
            expectation.fulfill()
        }
        await mockWebSocketConnection.triggerReceive(.string("test message"))
        await fulfillment(of: [expectation], timeout: 1.0)
    }

    func testOnClose() async {
        let expectation = XCTestExpectation(description: "onClose handler called")
        await webSocketTransport.onClose { error in
            expectation.fulfill()
        }
        await mockWebSocketConnection.triggerClose(nil)
        await fulfillment(of: [expectation], timeout: 1.0)
    }

    func testConnectWithHttpUrl() async throws {
        let url = "http://example.com"
        try await webSocketTransport.connect(url: url, transferFormat: .text)
        XCTAssertTrue(mockWebSocketConnection.connectCalled)
        XCTAssertEqual(mockWebSocketConnection.request?.url?.scheme, "ws")
    }

    func testConnectWithHttpsUrl() async throws {
        let url = "https://example.com"
        try await webSocketTransport.connect(url: url, transferFormat: .text)
        XCTAssertTrue(mockWebSocketConnection.connectCalled)
        XCTAssertEqual(mockWebSocketConnection.request?.url?.scheme, "wss")
    }

    func testConnectWithHeaders() async throws {
        let headers = ["Authorization": "Bearer token"]
        webSocketTransport = WebSocketTransport(accessTokenFactory: nil, logger: logger, headers: headers, websocket: mockWebSocketConnection)
        let url = "http://example.com"
        try await webSocketTransport.connect(url: url, transferFormat: .text)
        XCTAssertTrue(mockWebSocketConnection.connectCalled)
        XCTAssertEqual(mockWebSocketConnection.request?.allHTTPHeaderFields?["Authorization"], "Bearer token")
    }

    func testConnectWithAccessToken() async throws {
        let accessTokenFactory: @Sendable () async throws -> String? = { return "test_token" }
        webSocketTransport = WebSocketTransport(accessTokenFactory: accessTokenFactory, logger: logger, headers: [:], websocket: mockWebSocketConnection)
        let url = "http://example.com"
        try await webSocketTransport.connect(url: url, transferFormat: .text)
        XCTAssertTrue(mockWebSocketConnection.connectCalled)
        XCTAssertEqual(mockWebSocketConnection.request?.value(forHTTPHeaderField: "Authorization"), "Bearer test_token")
    }
}

class MockWebSocketConnection: WebSocketTransport.WebSocketConnection {
    var connectCalled = false
    var sentData: StringOrData?
    var stopCalled = false
    var onReceiveHandler: Transport.OnReceiveHandler?
    var onCloseHandler: Transport.OnCloseHander?
    var request: URLRequest?

    func connect(request: URLRequest, transferFormat: TransferFormat) async throws {
        self.request = request
        connectCalled = true
    }

    func send(_ data: StringOrData) async throws {
        sentData = data
    }

    func stop(error: Error?) async {
        stopCalled = true
    }

    func onReceive(_ handler: Transport.OnReceiveHandler?) async {
        onReceiveHandler = handler
    }

    func onClose(_ handler: Transport.OnCloseHander?) async {
        onCloseHandler = handler
    }

    func triggerReceive(_ data: StringOrData) async {
        await onReceiveHandler?(data)
    }

    func triggerClose(_ error: Error?) async {
        await onCloseHandler?(error)
    }
}

#if os(Linux)
private actor FrameOrderRecorder {
    private(set) var events: [String] = []

    func append(_ value: String) {
        events.append(value)
    }
}

#endif
