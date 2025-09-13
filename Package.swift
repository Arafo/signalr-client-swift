// swift-tools-version: 5.10
import PackageDescription

let package = Package(
    name: "SignalRClient",
    platforms: [
        .macOS(.v11),
        .iOS(.v14)
    ],
    products: [
        .library(name: "SignalRClient", targets: ["SignalRClient"])
    ],
    dependencies: [
        .package(url: "https://github.com/vapor/websocket-kit.git", from: "2.16.1"),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.86.0")
    ],
    targets: [
        .target(
            name: "SignalRClient",
            dependencies: [
                .product(name: "WebSocketKit", package: "websocket-kit"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio")
            ]
        ),
        .testTarget(
            name: "SignalRClientTests", dependencies: ["SignalRClient"],
            swiftSettings: [
                //                .enableExperimentalFeature("StrictConcurrency")
            ]
        ),
        .testTarget(
            name: "SignalRClientIntegrationTests", dependencies: ["SignalRClient"]
        ),
    ]
)
