#!/usr/bin/env python3
"""Exercise the real Better Auth, Envoy and Axum stack over HTTP."""

import http.cookiejar
import json
import os
import urllib.error
import urllib.request
import uuid


def browser():
    return urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar())
    )


def request(client, path, status=200, body=None, headers=None):
    base = f"http://localhost:{os.environ.get('PGDERIVE_HTTP_PORT', '8080')}"
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Origin": base, "Content-Type": "application/json", **(headers or {})}
    query = urllib.request.Request(base + path, data=data, headers=headers)
    try:
        result = client.open(query, timeout=10)
    except urllib.error.HTTPError as error:
        result = error

    with result:
        payload = result.read()
        if result.code != status:
            raise RuntimeError(f"{path}: expected HTTP {status}, got {result.code}: {payload!r}")
        return json.loads(payload) if status == 200 else None


def register(client, suffix):
    credentials = {
        "email": f"{suffix}@example.com",
        "password": f"local-smoke-{suffix}",
    }
    request(client, "/api/auth/sign-up/email", body={"name": suffix, **credentials})
    request(client, "/api/auth/sign-out", body={})
    return request(client, "/api/auth/sign-in/email", body=credentials)["user"]["id"]


def main():
    owner = browser()
    outsider = browser()
    suffix = uuid.uuid4().hex
    owner_id = register(owner, f"owner-{suffix}")
    outsider_id = register(outsider, f"outsider-{suffix}")
    org = request(
        owner, "/api/auth/organization/create", body={"name": "Smoke test", "slug": suffix}
    )
    path = f"/api/v1/organizations/{org['id']}"
    owner_headers = {"x-pgderive-user-id": owner_id, "x-pgderive-organization-id": org["id"]}

    request(browser(), path, status=401, headers=owner_headers)
    request(outsider, path, status=403, headers=owner_headers)
    request(browser(), "/internal/check" + path, status=404)
    result = request(owner, path, headers={"x-pgderive-user-id": outsider_id})
    expected = {"organization_id": org["id"], "user_id": owner_id}
    if result != expected:
        raise RuntimeError(f"Envoy returned unexpected identity: {result!r}")

    request(owner, "/api/auth/sign-out", body={})
    request(owner, path, status=401)
    print("Verified signed-in member, nonmember, forged headers and signed-out session.")


if __name__ == "__main__":
    main()
