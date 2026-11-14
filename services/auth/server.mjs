import { createServer } from "node:http";
import { fromNodeHeaders, toNodeHandler } from "better-auth/node";
import { auth } from "./auth.mjs";

const authHandler = toNodeHandler(auth);
const organizationCheck = /^\/internal\/check\/api\/v1\/organizations\/([a-zA-Z0-9_-]+)$/;

async function checkOrganization(request, response, organizationId) {
  const headers = fromNodeHeaders(request.headers);
  const identity = await auth.api.getSession({ headers });
  if (!identity) {
    response.writeHead(401).end();
    return;
  }

  const organizations = await auth.api.listOrganizations({ headers });
  if (!organizations.some(({ id }) => id === organizationId)) {
    response.writeHead(403).end();
    return;
  }

  response.writeHead(200, {
    "x-pgderive-user-id": identity.user.id,
    "x-pgderive-organization-id": organizationId,
  }).end();
}

async function handle(request, response) {
  const path = request.url?.split("?", 1)[0];
  if (path?.startsWith("/api/auth/")) {
    await authHandler(request, response);
    return;
  }

  const match = path?.match(organizationCheck);
  if (request.method === "GET" && match) {
    await checkOrganization(request, response, match[1]);
    return;
  }

  response.writeHead(404).end();
}

createServer((request, response) => {
  handle(request, response).catch((error) => {
    console.error("Auth request failed", error);
    if (response.headersSent) {
      response.destroy();
    } else {
      response.writeHead(500).end();
    }
  });
}).listen(3000, "0.0.0.0");
