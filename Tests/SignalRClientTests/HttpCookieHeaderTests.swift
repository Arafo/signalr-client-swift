// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

import XCTest
@testable import SignalRClient

final class HttpCookieHeaderTests: XCTestCase {
    private let negotiateUrl = URL(string: "https://example.com/negotiate")!

    func testNegotiateCookieHeaderReturnsNilWithoutSetCookie() {
        let response = HttpResponse(statusCode: 200, headers: ["Content-Type": "application/json"])

        XCTAssertNil(HttpCookieHeader.negotiateCookieHeader(from: response, url: negotiateUrl))
    }

    func testNegotiateCookieHeaderReadsSeparateSetCookieHeaders() throws {
        let response = HttpResponse(statusCode: 200, headers: [
            "Set-Cookie": "AWSALB=alb-value; Expires=Sun, 31 May 2026 20:10:08 GMT; Path=/",
            "set-cookie": "AWSALBCORS=cors-value; Expires=Sun, 31 May 2026 20:10:08 GMT; Path=/; SameSite=None; Secure"
        ])

        let pairs = try XCTUnwrap(HttpCookieHeader.negotiateCookieHeader(from: response, url: negotiateUrl))
            .components(separatedBy: "; ")

        XCTAssertEqual(Set(pairs), Set(["AWSALB=alb-value", "AWSALBCORS=cors-value"]))
    }

    func testNegotiateCookieHeaderReadsCombinedSetCookieHeaderWithExpiresComma() {
        let response = HttpResponse(statusCode: 200, headers: [
            "Set-Cookie": "AWSALB=alb-value; Expires=Sun, 31 May 2026 20:10:08 GMT; Path=/; SameSite=None; Secure, AWSALBCORS=cors-value; Expires=Sun, 31 May 2026 20:10:08 GMT; Path=/; SameSite=None; Secure"
        ])

        XCTAssertEqual(
            HttpCookieHeader.negotiateCookieHeader(from: response, url: negotiateUrl),
            "AWSALB=alb-value; AWSALBCORS=cors-value"
        )
    }

    func testMergeCookieHeaderAppendsToExistingCookieHeaderCaseInsensitively() {
        let headers = HttpCookieHeader.mergeCookieHeader("AWSALB=alb-value", into: ["cookie": "session=existing"])

        XCTAssertEqual(headers["cookie"], "session=existing; AWSALB=alb-value")
        XCTAssertNil(headers["Cookie"])
    }

    func testMergeCookieHeaderDoesNotDuplicateExistingCookiePair() {
        let headers = HttpCookieHeader.mergeCookieHeader("AWSALB=alb-value", into: ["Cookie": "session=existing; AWSALB=alb-value"])

        XCTAssertEqual(headers["Cookie"], "session=existing; AWSALB=alb-value")
    }

    func testMergeCookieHeaderReplacesStaleValueForSameCookieName() {
        let headers = HttpCookieHeader.mergeCookieHeader("AWSALB=new-value", into: ["Cookie": "session=existing; AWSALB=old-value"])

        XCTAssertEqual(headers["Cookie"], "session=existing; AWSALB=new-value")
    }
}
