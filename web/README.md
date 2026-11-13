# Pgderive landing page

Run these commands from the repository root with a supported Node.js version.

```sh
npm ci --prefix web
npm run dev --prefix web
```

Use `npm run build --prefix web` to build the production assets into `web/dist/`. Use `npm run preview --prefix web` to preview that build locally.

## Cloudflare deployment

Confirm that Wrangler is using the personal Cloudflare account with `cd web && npx wrangler whoami`. From the repository root, run `npm run deploy --prefix web` to build and deploy the assets with [Cloudflare Workers Static Assets](https://developers.cloudflare.com/workers/static-assets/).

To use `pgderive.com`, add the domain to the personal Cloudflare account and set the Cloudflare-assigned nameservers at Porkbun. Once the zone is active, Wrangler attaches `pgderive.com` as a Custom Domain for the `pgderive` Worker on deploy. [Nameserver setup](https://developers.cloudflare.com/dns/nameservers/update-nameservers/) · [Worker Custom Domains](https://developers.cloudflare.com/workers/configuration/routing/custom-domains/)

## Early-access waitlist

The landing page stores signup emails in the `pgderive-early-access` D1 database. Apply the migration before deploying the signup form for the first time.

```sh
cd web
npx wrangler d1 migrations apply pgderive-early-access --remote
```

Use the Cloudflare D1 dashboard to access the `early_access` table when sending launch updates or invitations. The form records signups for later outreach.
