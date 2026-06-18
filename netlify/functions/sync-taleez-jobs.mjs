// netlify/functions/sync-taleez-jobs.js
//
// Keep the Webflow CMS in sync with the ACTIVE jobs from Taleez.
//   - active job not in Webflow  -> create
//   - active job already there   -> update if it changed
//   - all active jobs            -> published
//   - anything else in Webflow   -> deleted
//
// Single pass, idempotent (safe to re-run). Scheduled in netlify.toml:
//   [functions."sync-taleez-jobs"]
//     schedule = "@hourly"

const WF_BASE = "https://api.webflow.com/v2";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function toIsoFromUnixSeconds(sec) {
  return sec ? new Date(sec * 1000).toISOString() : null;
}

function slugify(str) {
  return (str || "")
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)+/g, "");
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

// Compare managed fields so we only update jobs that actually changed.
// "last-seen-at" is ignored because it changes every run.
function norm(v) {
  return v === null || v === undefined || v === "" ? "" : JSON.stringify(v);
}
function hasChanged(mapped, existing = {}) {
  for (const key of Object.keys(mapped)) {
    if (key === "last-seen-at") continue;
    if (norm(mapped[key]) !== norm(existing[key])) return true;
  }
  return false;
}

// -------------------- Webflow --------------------
async function wfFetch(path, { method = "GET", body } = {}) {
  const res = await fetch(`${WF_BASE}${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    throw new Error(`Webflow ${method} ${path} failed: ${res.status} ${await res.text()}`);
  }
  return res.status === 204 ? null : res.json();
}

async function wfListAllItems(collectionId) {
  const all = [];
  let offset = 0;
  while (true) {
    const data = await wfFetch(
      `/collections/${collectionId}/items?limit=100&offset=${offset}`
    );
    const items = data?.items || [];
    all.push(...items);
    if (items.length < 100) break;
    offset += 100;
    await sleep(120);
  }
  return all;
}

async function wfCreate(collectionId, items) {
  return wfFetch(`/collections/${collectionId}/items`, { method: "POST", body: { items } });
}
async function wfUpdate(collectionId, items) {
  return wfFetch(`/collections/${collectionId}/items`, { method: "PATCH", body: { items } });
}
async function wfPublish(collectionId, itemIds) {
  if (itemIds.length)
    await wfFetch(`/collections/${collectionId}/items/publish`, { method: "POST", body: { itemIds } });
}
async function wfDelete(collectionId, itemIds) {
  if (!itemIds.length) return;
  // NOTE: unlike publish (which takes `itemIds`), the delete endpoints
  // expect `{ items: [{ id }, ...] }`.
  const items = itemIds.map((id) => ({ id }));
  // remove from the live site, then from the CMS
  await wfFetch(`/collections/${collectionId}/items/live`, { method: "DELETE", body: { items } });
  await wfFetch(`/collections/${collectionId}/items`, { method: "DELETE", body: { items } });
}

// -------------------- Taleez --------------------
async function taleezFetchActiveJobs() {
  const base = process.env.TALEEZ_BASE_URL || "https://api.taleez.com";
  const path = process.env.TALEEZ_OFFERS_PATH || "/0/jobs";
  const secret = process.env.TALEEZ_API_SECRET;
  if (!secret) throw new Error("Missing TALEEZ_API_SECRET");

  // Only pull published jobs from Taleez (not the whole history).
  // Set TALEEZ_STATUS_FILTER="" to disable.
  const status =
    process.env.TALEEZ_STATUS_FILTER === undefined ? "PUBLISHED" : process.env.TALEEZ_STATUS_FILTER;

  const all = [];
  let page = 0;
  while (true) {
    const url = new URL(`${base}${path}`);
    url.searchParams.set("page", String(page));
    url.searchParams.set("pageSize", "200");
    url.searchParams.set("withDetails", "true");
    if (status) url.searchParams.set("status", status);

    const res = await fetch(url.toString(), {
      headers: { "X-taleez-api-secret": secret, "Content-Type": "application/json" },
    });
    if (!res.ok) throw new Error(`Taleez fetch failed: ${res.status} ${await res.text()}`);

    const data = await res.json();
    const items = Array.isArray(data) ? data : data.list || [];
    all.push(...items);

    if (typeof data?.hasMore === "boolean" ? !data.hasMore : items.length < 200) break;
    page += 1;
    await sleep(100);
  }
  return all;
}

// -------------------- Mapping --------------------
// Webflow field slugs must match your schema EXACTLY (kebab-case).
function mapJob(job, nowIso) {
  const taleezId = String(job.id);
  const title = job.label || `Job ${taleezId}`;

  return {
    name: title,
    slug: `${slugify(title)}-${taleezId}`.slice(0, 240),

    "taleez-id": taleezId,
    "taleez-token": job.token || "",

    status: job.currentStatus || "",
    visibility: job.visibility || "",

    contract: job.contract || "",
    "contract-length-value": job.contractLength != null ? String(job.contractLength) : "",
    "contract-length-unit": job.contractLengthTimeUnit || "",
    "full-time": !!job.fullTime,
    "work-hours": job.workHours ?? null,
    remote: !!job.remote,

    country: job.country || "",
    city: job.city || "",
    "postal-code": job.postalCode ? String(job.postalCode) : "",
    "location-full": [job.city, job.postalCode, job.country].filter(Boolean).join(", "),
    lat: typeof job.lat === "number" ? job.lat : null,
    lng: typeof job.lng === "number" ? job.lng : null,

    "company-label": job.companyLabel || "",
    "company-website": job.website || "",
    "company-logo-url": job.logo || "",
    "company-banner-url": job.banner || "",
    "company-description": job.companyDescription || "",

    "job-description": job.jobDescription || "",
    "profile-description": job.profileDescription || "",

    "offer-url": job.url || "",
    "apply-url": job.urlApplying || "",

    "tags-text": Array.isArray(job.tags) ? job.tags.join(", ") : "",

    "created-at": toIsoFromUnixSeconds(job.dateCreation),
    "first-publish-at": toIsoFromUnixSeconds(job.dateFirstPublish),
    "last-publish-at": toIsoFromUnixSeconds(job.dateLastPublish),

    "is-active": true,
    "last-seen-at": nowIso,
  };
}

// A job counts as active if it's published AND publicly visible.
function isActive(job) {
  const visible = job.visibility === "PUBLIC" || job.visibility === "INTERNAL_AND_PUBLIC";
  return visible && job.currentStatus === "PUBLISHED";
}

// -------------------- Handler --------------------
export default async function handler() {
  try {
    const collectionId = process.env.WEBFLOW_COLLECTION_ID;

    if (!process.env.WEBFLOW_TOKEN) {
      throw new Error("Missing WEBFLOW_TOKEN");
    }

    if (!collectionId) {
      throw new Error("Missing WEBFLOW_COLLECTION_ID");
    }

    const nowIso = new Date().toISOString();
    const minExpected = Number(process.env.MIN_EXPECTED_COUNT || "0");

    // 1) Active jobs from Taleez
    const jobs = (await taleezFetchActiveJobs()).filter(isActive);

    if (minExpected && jobs.length < minExpected) {
      throw new Error(
        `Safety stop: only ${jobs.length} active jobs (< ${minExpected}).`
      );
    }

    // 2) What's already in Webflow
    const existing = await wfListAllItems(collectionId);
    const byTaleezId = new Map();

    for (const item of existing) {
      const tId = item?.fieldData?.["taleez-id"];
      if (tId) byTaleezId.set(String(tId), item);
    }

    // 3) Decide create/update
    const seen = new Set();
    const toCreate = [];
    const toUpdate = [];
    const activeIds = [];

    for (const job of jobs) {
      const tId = String(job.id);
      seen.add(tId);

      const fields = mapJob(job, nowIso);
      const current = byTaleezId.get(tId);

      if (!current) {
        toCreate.push({ fieldData: fields });
      } else {
        activeIds.push(current.id);

        if (hasChanged(fields, current.fieldData)) {
          toUpdate.push({
            id: current.id,
            fieldData: fields,
          });
        }
      }
    }

    // 4) Create
    let created = 0;

    for (const batch of chunk(toCreate, 100)) {
      const res = await wfCreate(collectionId, batch);

      for (const item of res?.items || []) {
        if (item?.id) activeIds.push(item.id);
      }

      created += batch.length;
      await sleep(200);
    }

    // 5) Update
    let updated = 0;

    for (const batch of chunk(toUpdate, 100)) {
      await wfUpdate(collectionId, batch);
      updated += batch.length;
      await sleep(200);
    }

    // 6) Publish
    for (const batch of chunk([...new Set(activeIds)], 100)) {
      await wfPublish(collectionId, batch);
      await sleep(200);
    }

    // 7) Delete stale items
    const stale = existing
      .filter((item) => {
        const tId = String(item?.fieldData?.["taleez-id"] || "");
        return tId && !seen.has(tId);
      })
      .map((item) => item.id);

    let deleted = 0;

    for (const batch of chunk(stale, 100)) {
      await wfDelete(collectionId, batch);
      deleted += batch.length;
      await sleep(200);
    }

    console.log(
      JSON.stringify({
        event: "taleez-sync-completed",
        ok: true,
        activeJobs: jobs.length,
        created,
        updated,
        deleted,
        runAt: nowIso,
      })
    );

    // Scheduled functions should return undefined.
    return;
  } catch (err) {
    console.error("SYNC ERROR:", err?.stack || err);

    // Throwing marks the scheduled invocation as failed in Netlify.
    throw err;
  }
}

export const config = {
  schedule: "@hourly",
};