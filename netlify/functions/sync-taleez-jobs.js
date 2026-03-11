// netlify/functions/sync-taleez-jobs.js

const WF_BASE = "https://api.webflow.com/v2";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function toIsoFromUnixSeconds(sec) {
  if (!sec) return null;
  return new Date(sec * 1000).toISOString();
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

// -------------------- Webflow (Data API v2) --------------------
async function wfFetch(path, { method = "GET", body } = {}) {
  const token = process.env.WEBFLOW_TOKEN;
  const res = await fetch(`${WF_BASE}${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Webflow ${method} ${path} failed: ${res.status} ${text}`);
  }

  if (res.status === 204) return null;
  return await res.json();
}

async function wfListAllItems(collectionId) {
  const all = [];
  const limit = 100;
  let offset = 0;

  while (true) {
    const data = await wfFetch(
      `/collections/${collectionId}/items?limit=${limit}&offset=${offset}`
    );
    const items = data?.items || [];
    all.push(...items);
    if (items.length < limit) break;
    offset += limit;
    await sleep(150);
  }
  return all;
}

async function wfBulkCreate(collectionId, items) {
  // Bulk create staged items
  return wfFetch(`/collections/${collectionId}/items`, {
    method: "POST",
    body: { items },
  });
}

async function wfBulkUpdate(collectionId, items) {
  // Bulk update staged items
  return wfFetch(`/collections/${collectionId}/items`, {
    method: "PATCH",
    body: { items },
  });
}

async function wfPublish(collectionId, itemIds) {
  if (!itemIds.length) return;
  return wfFetch(`/collections/${collectionId}/items/publish`, {
    method: "POST",
    body: { itemIds },
  });
}

async function wfUnpublish(collectionId, itemIds) {
  if (!itemIds.length) return;
  return wfFetch(`/collections/${collectionId}/items/live`, {
    method: "DELETE",
    body: { itemIds },
  });
}

// -------------------- Taleez --------------------
async function taleezFetchAllJobs() {
  const base = process.env.TALEEZ_BASE_URL || "https://api.taleez.com";
  const path = process.env.TALEEZ_OFFERS_PATH || "/0/jobs";
  const secret = process.env.TALEEZ_API_SECRET;

  if (!secret) throw new Error("Missing TALEEZ_API_SECRET");

  const pageSize = 200;
  let page = 0;
  const all = [];

  while (true) {
    const url = new URL(`${base}${path}`);
    url.searchParams.set("page", String(page));
    url.searchParams.set("pageSize", String(pageSize));
    // If you want richer list payloads, you can try:
    // url.searchParams.set("withDetails", "true");

    const res = await fetch(url.toString(), {
      headers: {
        "X-taleez-api-secret": secret,
        "Content-Type": "application/json",
      },
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Taleez fetch failed: ${res.status} ${text}`);
    }

    const data = await res.json();

    // Taleez JobList: { hasMore: boolean, list: [] }
    const items = Array.isArray(data) ? data : (data.list || []);
    all.push(...items);

    if (typeof data?.hasMore === "boolean") {
      if (!data.hasMore) break;
    } else {
      if (items.length < pageSize) break;
    }

    page += 1;
    await sleep(100);
  }

  return all;
}

// -------------------- Mapping --------------------
// IMPORTANT: Use Webflow field slugs EXACTLY as in your schema (kebab-case).
function mapJobToWebflow(job, nowIso) {
  const taleezId = String(job.id);
  const title = job.label || `Job ${taleezId}`;
  const slug = `${slugify(title)}-${taleezId}`.slice(0, 240);

  // refine later once you confirm Taleez statuses
const ACTIVE_VISIBILITY = new Set(["PUBLIC", "INTERNAL_AND_PUBLIC"]);
const isActive =
  ACTIVE_VISIBILITY.has(job.visibility) &&
  job.currentStatus === "PUBLISHED";


  return {
    fieldData: {
      // Webflow reserved:
      name: title,
      slug,

      // Identity:
      "taleez-id": taleezId,
      "taleez-token": job.token || "",

      // Status:
      status: job.currentStatus || "",
      visibility: job.visibility || "",

      // Job:
      contract: job.contract || "",
      "contract-length-value":
        job.contractLength != null ? String(job.contractLength) : "",
      "contract-length-unit": job.contractLengthTimeUnit || "",
      "full-time": !!job.fullTime,
      "work-hours": job.workHours ?? null,
      remote: !!job.remote,

      // Location:
      country: job.country || "",
      city: job.city || "",
      "postal-code": job.postalCode ? String(job.postalCode) : "",
      "location-full": [job.city, job.postalCode, job.country]
        .filter(Boolean)
        .join(", "),
lat: typeof job.lat === "number" ? job.lat : null,
lng: typeof job.lng === "number" ? job.lng : null,

      // Company:
      "company-label": job.companyLabel || "",
      "company-website": job.website || "",
      "company-logo-url": job.logo || "",
      "company-banner-url": job.banner || "",
      "company-description": job.companyDescription || "",

      // Content:
      "job-description": job.jobDescription || "",
      "profile-description": job.profileDescription || "",

      // Links:
      "offer-url": job.url || "",
      "apply-url": job.urlApplying || "",

      // Tags:
      "tags-text": Array.isArray(job.tags) ? job.tags.join(", ") : "",

      // Dates:
      "created-at": toIsoFromUnixSeconds(job.dateCreation),
      "first-publish-at": toIsoFromUnixSeconds(job.dateFirstPublish),
      "last-publish-at": toIsoFromUnixSeconds(job.dateLastPublish),

      // Sync fields:
      "is-active": isActive,
      // NOTE: your schema has last-seen-at as PlainText, so store ISO as string
      "last-seen-at": nowIso,
    },
  };
}

// -------------------- Handler --------------------
export async function handler(event) {
  try {
    const collectionId = process.env.WEBFLOW_COLLECTION_ID;
    const token = process.env.WEBFLOW_TOKEN;

    if (!token) throw new Error("Missing WEBFLOW_TOKEN");
    if (!collectionId) throw new Error("Missing WEBFLOW_COLLECTION_ID");

    const minExpected = Number(process.env.MIN_EXPECTED_COUNT || "0");
    const nowIso = new Date().toISOString();

    const isScheduled = event?.headers?.["x-nf-scheduled"] === "true";

    // 1) Taleez jobs
    const jobs = await taleezFetchAllJobs();

    // Guardrail
    if (minExpected && jobs.length < minExpected) {
      throw new Error(
        `Safety stop: Taleez returned ${jobs.length} jobs (< ${minExpected}).`
      );
    }

    // 2) Existing Webflow items
    const existing = await wfListAllItems(collectionId);

    // Map: taleez-id -> webflow item id
    const existingByTaleez = new Map();
    for (const item of existing) {
      const tId = item?.fieldData?.["taleez-id"];
      if (tId) existingByTaleez.set(String(tId), item.id);
    }

    // 3) Prepare create/update and publish list
    const seen = new Set();
    const toCreate = [];
    const toUpdate = [];
    const publishIds = [];

    for (const job of jobs) {
      const tId = String(job.id);
      seen.add(tId);

      const mapped = mapJobToWebflow(job, nowIso);
      const existingId = existingByTaleez.get(tId);

      const active = !!mapped.fieldData["is-active"];

      if (existingId) {
        toUpdate.push({ id: existingId, ...mapped });
        if (active) publishIds.push(existingId);
      } else {
        toCreate.push(mapped);
      }
    }

    // 4) Create staged items
    const createdIdsToPublish = [];
    for (const batch of chunk(toCreate, 100)) {
      const created = await wfBulkCreate(collectionId, batch);

      // Webflow sometimes returns created items with ids
      const createdItems = created?.items || [];
      for (const it of createdItems) {
        if (it?.fieldData?.["is-active"]) createdIdsToPublish.push(it.id);
      }
      await sleep(250);
    }

    // 5) Update staged items
    for (const batch of chunk(toUpdate, 100)) {
      await wfBulkUpdate(collectionId, batch);
      await sleep(250);
    }

    // 6) Publish active items
  // Re-list items so we can publish newly created ones too
// Re-list items so we can publish newly created items too
const afterWrite = await wfListAllItems(collectionId);

// Publish everything that is active (simple + guaranteed)
const idsToPublish = [];
for (const item of afterWrite) {
  if (item?.fieldData?.["is-active"] === true) {
    idsToPublish.push(item.id);
  }
}
await wfPublishInBatches(collectionId, idsToPublish);

    // 7) Soft delete: mark inactive + unpublish items missing from Taleez list
    const missingIds = [];
    const missingUpdates = [];

    for (const item of existing) {
      const tId = String(item?.fieldData?.["taleez-id"] || "");
      if (!tId) continue;

      if (!seen.has(tId)) {
        missingIds.push(item.id);
        missingUpdates.push({
          id: item.id,
          fieldData: { "is-active": false },
        });
      }
    }

    for (const batch of chunk(missingUpdates, 100)) {
      await wfBulkUpdate(collectionId, batch);
      await sleep(250);
    }

    for (const batch of chunk(missingIds, 100)) {
      await wfUnpublish(collectionId, batch);
      await sleep(250);
    }

    return {
      statusCode: 200,
      body: JSON.stringify({
        ok: true,
        scheduled: isScheduled,
        taleezJobs: jobs.length,
        webflowExisting: existing.length,
        created: toCreate.length,
        updated: toUpdate.length,
        published: uniquePublishIds.length,
        unpublished: missingIds.length,
        runAt: nowIso,
      }),
    };
  } catch (err) {
    console.error("SYNC ERROR:", err?.stack || err);
    return {
      statusCode: 500,
      body: JSON.stringify({
        ok: false,
        error: String(err?.message || err),
      }),
    };
  }
}

async function wfPublishInBatches(collectionId, itemIds) {
  const unique = [...new Set(itemIds)];
  for (const batch of chunk(unique, 100)) {
    await wfPublish(collectionId, batch);
    await sleep(250);
  }
}