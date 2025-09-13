// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

import Foundation
#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import WebSocketKit
import NIOCore
import NIOPosix

actor WebSocketTransport: Transport {
    private let logger: Logger
    private let accessTokenFactory: (@Sendable () async throws -> String?)?
    private let headers: [String: String]
    private let webSocketConnection: WebSocketConnection

    private var transferFormat: TransferFormat = .text

    init(accessTokenFactory: (@Sendable () async throws -> String?)?,
         logger: Logger,
         headers: [String: String],
         websocket: WebSocketConnection? = nil) {
        self.accessTokenFactory = accessTokenFactory
        self.logger = logger
        self.headers = headers
        self.webSocketConnection = websocket ?? DefaultWebSocketConnection(logger: logger)
    }

    func onReceive(_ handler: OnReceiveHandler?) async {
        await self.webSocketConnection.onReceive(handler)
    }

    func onClose(_ handler: OnCloseHander?) async {
        await self.webSocketConnection.onClose(handler)
    }

    func connect(url: String, transferFormat: TransferFormat) async throws {
        self.logger.log(level: .debug, message: "(WebSockets transport) Connecting.")

        self.transferFormat = transferFormat
        var urlComponents = URLComponents(url: URL(string: url)!, resolvingAgainstBaseURL: false)!
        if urlComponents.scheme == "http" {
            urlComponents.scheme = "ws"
        } else if urlComponents.scheme == "https" {
            urlComponents.scheme = "wss"
        }

        var request = URLRequest(url: urlComponents.url!)

        // Add headeres
        for (key, value) in headers {
            request.addValue(value, forHTTPHeaderField: key)
        }

        // Add token to header
        if let factory = accessTokenFactory, let token = try await factory() {
            request.addValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }

        // Add user-agent
        request.addValue(Utils.getUserAgent(), forHTTPHeaderField: "User-Agent")

        try await webSocketConnection.connect(request: request, transferFormat: transferFormat)
    }

    func send(_ data: StringOrData) async throws {
        try await webSocketConnection.send(data)
    }

    func stop(error: Error?) async throws {
        try await webSocketConnection.stop(error: error)
    }

    protocol WebSocketConnection {
        func connect(request: URLRequest, transferFormat: TransferFormat) async throws
        func send(_ data: StringOrData) async throws
        func stop(error: Error?) async throws
        func onReceive(_ handler: OnReceiveHandler?) async
        func onClose(_ handler: OnCloseHander?) async
    }

#if os(Linux)
    private actor DefaultWebSocketConnection: WebSocketConnection {
        private let logger: Logger
        private let openTcs: TaskCompletionSource<Void> = TaskCompletionSource()
        private let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        private var websocket: WebSocket?
        private var onReceive: OnReceiveHandler?
        private var onClose: OnCloseHander?
        
        init(
            logger: Logger
        ) {
            self.logger = logger
        }
        
        func connect(
            request: URLRequest,
            transferFormat: TransferFormat
        ) async throws {
            let (url, headers) = try websocketURLStringAndHeaders(from: request)
            let config = WebSocketClient.Configuration(maxFrameSize: Int(Int32.max) - 1)
            logger.log(level: .debug, message: "*** Test ***")
            logger.log(level: .debug, message: "(WebSockets transport) Connecting to \(url) with transfer format \(transferFormat).")
            //            try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
            do {
            try await WebSocket.connect(
                to: url,
                headers: headers,
                //                    proxy: "127.0.0.1",
                //                    proxyPort: 9090,
                configuration: config,
                on: group
            ) { [weak self] socket in
                guard let self = self else { return }
                self.websocket = socket

                logger.log(level: .debug, message: "(WebSockets transport) urlSession didOpenWithProtocol invoked. WebSocket open")
                
                Task {
                    if await self.openTcs.trySetResult(.success(())) == true {
                        self.logger.log(level: .debug, message: "(WebSockets transport) WebSocket connected")
                    }
                }
                
                socket.onText { [weak self] _, text in
                    guard let self = self else { return }
                    await self.onReceive?(.string(text))
                    //Task { await self.onReceive?(.string(text)) }
                }
                socket.onBinary { [weak self] _, buffer in
                    guard let self = self else { return }
                    let data = Data(buffer.readableBytesView)
                    await self.onReceive?(.data(data))
                    //Task { await self.onReceive?(.data(data)) }
                }
                
                socket.onClose.whenComplete { [weak self] result in
                    guard let self = self else { return }
                    logger.log(level: .debug, message: "(WebSockets transport) WebSocket closed")
                    Task {
                        switch result {
                        case .success:
                            await self.onClose?(nil)
                        case .failure(let error):
                            await self.onClose?(error)
                        }
                    }
                }
            }.get()
            //.wait()
            } catch {
                logger.log(level: .error, message: "(WebSockets transport) WebSocket connect error")
                //try? await stop(error: error)
                            if await openTcs.trySetResult(.failure(error ?? SignalRError.connectionAborted)) == true {
                //receiveTask?.cancel() // Cancel the receive task
            } else {
                //await receiveTask?.value // Wait for the receive task to complete
                await onClose?(error) // Call the close handler
            }
            }
            //                .whenComplete { result in
            //                    switch result {
            //                    case .success:
            //                        cont.resume(returning: ())
            //                    case .failure(let error):
            //                        cont.resume(throwing: error)
            //                    }
            //                }
            //            }
            
            // wait for startTcs to be completed before returning from connect
            // this is to ensure that the connection is truely established
            try await openTcs.task()
        }
        
        func send(_ data: StringOrData) async throws {
            guard let websocket = self.websocket else {
                throw SignalRError.invalidOperation("Not connected")
            }
            
            switch data {
            case .string(let s):
                try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
                    let promise = websocket.eventLoop.makePromise(of: Void.self)
                    websocket.send(s, promise: promise)
                    promise.futureResult.whenComplete { result in
                        switch result {
                        case .success: cont.resume(returning: ())
                        case .failure(let error): cont.resume(throwing: error)
                        }
                    }
                }
                
            case .data(let d):
                try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
                    var buffer = ByteBufferAllocator().buffer(capacity: d.count)
                    buffer.writeBytes(d)
                    let promise = websocket.eventLoop.makePromise(of: Void.self)
                    websocket.send(buffer, opcode: .binary, promise: promise)
                    promise.futureResult.whenComplete { result in
                        switch result {
                        case .success: cont.resume(returning: ())
                        case .failure(let error): cont.resume(throwing: error)
                        }
                    }
                }
            }
        }
        
        func stop(error: Error?) async throws {
            logger.log(level: .debug, message: "(WebSockets transport) Stopping")
            if let websocket = self.websocket {
                try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
                    let promise = websocket.eventLoop.makePromise(of: Void.self)
                    websocket.close(promise: promise)
                    promise.futureResult.whenComplete { result in
                        switch result {
                        case .success: cont.resume(returning: ())
                        case .failure(let e): cont.resume(throwing: e)
                        }
                    }
                }
            }

            if await openTcs.trySetResult(.failure(error ?? SignalRError.connectionAborted)) == true {
                //receiveTask?.cancel() // Cancel the receive task
            } else {
                //await receiveTask?.value // Wait for the receive task to complete
                await onClose?(error) // Call the close handler
            }
            
            try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
                group.shutdownGracefully { err in
                    if let err { cont.resume(throwing: err) } else { cont.resume(returning: ()) }
                }
            }
        }
        
        func onReceive(_ handler: OnReceiveHandler?) async {
            onReceive = handler
        }
        
        func onClose(_ handler: OnCloseHander?) async {
            onClose = handler
        }
        
        private func websocketURLStringAndHeaders(from request: URLRequest) throws -> (String, HTTPHeaders) {
            guard var comps = URLComponents(url: try urlOrThrow(request), resolvingAgainstBaseURL: false) else {
                throw URLError(.badURL)
            }
            
            switch comps.scheme?.lowercased() {
            case "http":  comps.scheme = "ws"
            case "https": comps.scheme = "wss"
            case "ws", "wss": break
            default: throw URLError(.unsupportedURL)
            }
            
            if comps.path.isEmpty { comps.path = "/" }
            
            var headers = HTTPHeaders()
            let hopHeaders: Set<String> = [
                "upgrade","connection","sec-websocket-version","sec-websocket-key","host"
            ]
            for (k, v) in (request.allHTTPHeaderFields ?? [:]) {
                if !hopHeaders.contains(k.lowercased()) {
                    headers.add(name: k, value: v)
                }
            }
            
            if headers.first(name: "Accept") == nil {
                headers.add(name: "Accept", value: "*/*")
            }
            if headers.first(name: "Accept-Encoding") == nil {
                headers.add(name: "Accept-Encoding", value: "gzip, deflate")
            }
            if headers.first(name: "Accept-Language") == nil {
                let acceptLanguage = Locale.preferredLanguages.enumerated()
                    .map { i, lang in i == 0 ? lang : "\(lang);q=\(String(format: "%.1f", 1.0 - Double(i) * 0.1))" }
                    .joined(separator: ", ")
                headers.add(name: "Accept-Language", value: acceptLanguage)
            }
            
            let existingCookie = headers.first(name: "Cookie")
            if let cookieValue = mergedCookieHeader(for: comps.url!, existing: existingCookie) {
                if existingCookie != nil {
                    headers.replaceOrAdd(name: "Cookie", value: cookieValue)
                } else {
                    headers.add(name: "Cookie", value: cookieValue)
                }
            }
            
            guard let urlString = comps.string else { throw URLError(.badURL) }
            return (urlString, headers)
        }
        private func urlOrThrow(_ request: URLRequest) throws -> URL {
            if let url = request.url { return url }
            throw URLError(.badURL)
        }
        
        private func mergedCookieHeader(for url: URL, existing: String?) -> String? {
            let stored = HTTPCookieStorage.shared.cookies(for: url) ?? []
            let storageHeader = HTTPCookie.requestHeaderFields(with: stored)["Cookie"]
            
            switch (existing?.trimmingCharacters(in: .whitespacesAndNewlines), storageHeader) {
            case let (e?, s?) where !e.isEmpty && !s.isEmpty:
                return e == s ? e : "\(e); \(s)"
            case let (e?, _) where !e.isEmpty:
                return e
            case let (_, s?):
                return s
            default:
                return nil
            }
        }
    }

#else

        private actor DefaultWebSocketConnection: NSObject, WebSocketConnection, URLSessionWebSocketDelegate {
            private let logger: Logger
            private let openTcs: TaskCompletionSource<Void> = TaskCompletionSource()

            private var urlSession: URLSession?
            private var websocket: URLSessionWebSocketTask?
            private var receiveTask: Task<Void, Never>?
            private var onReceive: OnReceiveHandler?
            private var onClose: OnCloseHander?

            private var closed: Bool = false

            init(logger: Logger) {
                self.logger = logger
            }

            func connect(request: URLRequest, transferFormat: TransferFormat) async throws {
                urlSession = URLSession(configuration: .default, delegate: self, delegateQueue: OperationQueue())
                websocket = urlSession!.webSocketTask(with: request)

                guard websocket != nil else {
                    throw SignalRError.failedToStartConnection("(WebSockets transport) WebSocket is nil")
                }

                websocket!.resume() // connect but it won't throw even failure

                receiveTask = Task { [weak self] in
                    guard let self = self else { return }
                    await receiveMessage()
                }

                // wait for startTcs to be completed before returning from connect
                // this is to ensure that the connection is truely established
                try await openTcs.task()
            }

            func send(_ data: StringOrData) async throws {
                guard let ws = self.websocket, ws.state == .running else {
                    throw SignalRError.invalidOperation("(WebSockets transport) Cannot send until the transport is connected")
                }

                switch data {
                case .string(let str):
                    try await ws.send(URLSessionWebSocketTask.Message.string(str))
                case .data(let data):
                    try await ws.send(URLSessionWebSocketTask.Message.data(data))
                }
            }

            func stop(error: Error?) async {
                if closed {
                    return
                }
                closed = true

                urlSession?.finishTasksAndInvalidate() // Prevent new task from being created
                websocket?.cancel() // Close the current connection

                if await openTcs.trySetResult(.failure(error ?? SignalRError.connectionAborted)) == true {
                    receiveTask?.cancel() // Cancel the receive task
                } else {
                    await receiveTask?.value // Wait for the receive task to complete
                    await onClose?(error) // Call the close handler
                }
            }

            func onReceive(_ handler: OnReceiveHandler?) async {
                onReceive = handler
            }

            func onClose(_ handler: OnCloseHander?) async {
                onClose = handler
            }

            nonisolated func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: (any Error)?) {
                logger.log(level: .debug, message: "(WebSockets transport) URLSession didCompleteWithError: \(String(describing: error))")

                Task {
                    await stop(error: error)
                }
            }

            // When receive websocket close message?
            nonisolated func urlSession(_ session: URLSession, webSocketTask: URLSessionWebSocketTask, didCloseWith closeCode: URLSessionWebSocketTask.CloseCode, reason: Data?) {
                logger.log(level: .debug, message: "(WebSockets transport) URLSession didCloseWith: \(closeCode)")

                Task {
                    await stop(error: nil)
                }
            }

            nonisolated func urlSession(_ session: URLSession, webSocketTask: URLSessionWebSocketTask, didOpenWithProtocol protocol: String?) {
                logger.log(level: .debug, message: "(WebSockets transport) urlSession didOpenWithProtocol invoked. WebSocket open")

                Task {
                    if await openTcs.trySetResult(.success(())) == true {
                        logger.log(level: .debug, message: "(WebSockets transport) WebSocket connected")
                    }
                }
            }

            private func receiveMessage() async {
                guard let websocket: URLSessionWebSocketTask = websocket else {
                    logger.log(level: .error, message: "(WebSockets transport) WebSocket is nil")
                    return
                }

                do {
                    while !Task.isCancelled {
                        let message = try await websocket.receive()

                        switch message {
                        case .string(let text):
                            logger.log(level: .debug, message: "(WebSockets transport) Received message: \(text)")
                            await onReceive?(.string(text))
                        case .data(let data):
                            await onReceive?(.data(data))
                        }
                    }
                } catch {
                    logger.log(level: .debug, message: "Websocket receive error : \(error)")
                }
            }
        }
    #endif
}
