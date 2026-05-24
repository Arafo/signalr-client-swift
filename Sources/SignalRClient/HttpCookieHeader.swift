// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

import Foundation

enum HttpCookieHeader {
    static func negotiateCookieHeader(from response: HttpResponse, url: URL) -> String? {
        let setCookieValues = response.headers
            .filter { $0.key.caseInsensitiveCompare("Set-Cookie") == .orderedSame }
            .map { $0.value }

        guard !setCookieValues.isEmpty else {
            return nil
        }

        let cookies = HTTPCookie.cookies(
            withResponseHeaderFields: ["Set-Cookie": setCookieValues.joined(separator: ", ")],
            for: url
        )

        guard !cookies.isEmpty else {
            return nil
        }

        return HTTPCookie.requestHeaderFields(with: cookies)["Cookie"]
    }

    static func mergeCookieHeader(_ cookieHeader: String, into headers: [String: String]?) -> [String: String] {
        let trimmedCookieHeader = cookieHeader.trimmingCharacters(in: .whitespacesAndNewlines)
        var mergedHeaders = headers ?? [:]

        guard !trimmedCookieHeader.isEmpty else {
            return mergedHeaders
        }

        if let existingCookieKey = mergedHeaders.keys.first(where: { $0.caseInsensitiveCompare("Cookie") == .orderedSame }) {
            let existingCookieHeader = (mergedHeaders[existingCookieKey] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            mergedHeaders[existingCookieKey] = mergeCookieHeaderValues(existingCookieHeader, trimmedCookieHeader)
        } else {
            mergedHeaders["Cookie"] = trimmedCookieHeader
        }

        return mergedHeaders
    }

    private static func mergeCookieHeaderValues(_ existing: String, _ additional: String) -> String {
        var mergedPairs = cookieHeaderPairs(existing)

        for pair in cookieHeaderPairs(additional) {
            let name = cookieName(from: pair)
            if let existingIndex = mergedPairs.firstIndex(where: { cookieName(from: $0) == name }) {
                mergedPairs[existingIndex] = pair
            } else {
                mergedPairs.append(pair)
            }
        }

        return mergedPairs.joined(separator: "; ")
    }

    private static func cookieHeaderPairs(_ cookieHeader: String) -> [String] {
        cookieHeader
            .split(separator: ";", omittingEmptySubsequences: true)
            .map { String($0).trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
    }

    private static func cookieName(from pair: String) -> Substring {
        pair[..<(pair.firstIndex(of: "=") ?? pair.endIndex)]
    }
}
