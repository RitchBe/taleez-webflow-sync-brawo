// netlify/functions/sync-taleez-jobs.js
// Scheduled by netlify.toml (no @netlify/functions dependency)

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
  return wfFetch(`/collections/${collectionId}/items/bulk`, {
    method: "POST",
    body: { items },
  });
}

async function wfBulkUpdate(collectionId, items) {
  // Bulk update staged items (up to 100)
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
  // Unpublish live items
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
  if (!path) throw new Error("Missing TALEEZ_OFFERS_PATH");

  const pageSize = 200; // reduce number of requests
  let page = 0;
  const all = [];

  while (true) {
    const url = new URL(`${base}${path}`);
    url.searchParams.set("page", String(page));
    url.searchParams.set("pageSize", String(pageSize));

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
    const items = Array.isArray(data) ? data : (data.items || []);

    all.push(...items);

    if (items.length < pageSize) break;
    page += 1;
    await sleep(100);
  }

  return all;
}

// -------------------- Mapping --------------------
// IMPORTANT: fieldData keys must match your Webflow CMS field slugs.
function mapJobToWebflow(job, nowIso) {
  const taleezId = String(job.id);
  const title = job.label || `Job ${taleezId}`;
  const slug = `${slugify(title)}-${taleezId}`.slice(0, 240);

  // Adjust once you confirm Taleez "open" statuses in your account.
  const isActive =
    job.visibility === "PUBLIC" &&
    job.currentStatus !== "DONE";

  return {
    fieldData: {
      // Webflow reserved:
      name: title,
      slug,

      // Identity:
      taleez_id: taleezId,
      taleez_token: job.token || "",

      // Status:
      status: job.currentStatus || "",
      visibility: job.visibility || "",

      // Job:
      contract: job.contract || "",
      contract_length_value: job.contractLength ?? null,
      contract_length_unit: job.contractLengthTimeUnit || "",
      full_time: !!job.fullTime,
      work_hours: job.workHours ?? null,
      remote: !!job.remote,

      // Location:
      country: job.country || "",
      city: job.city || "",
      postal_code: job.postalCode ? String(job.postalCode) : "",
      location_full: [job.city, job.postalCode, job.country]
        .filter(Boolean)
        .join(", "),
      lat: job.lat ?? null,
      lng: job.lng ?? null,

      // Company:
      company_label: job.companyLabel || "",
      company_website: job.website || "",
      company_logo_url: job.logo || "",
      company_banner_url: job.banner || "",

      // Links:
      offer_url: job.url || "",
      apply_url: job.urlApplying || "",

      // Content:
      job_description: job.jobDescription || "",
      profile_description: job.profileDescription || "",
      company_description: job.companyDescription || "",

      // Tags:
      tags_text: Array.isArray(job.tags) ? job.tags.join(", ") : "",

      // Dates:
      created_at: toIsoFromUnixSeconds(job.dateCreation),
      first_publish_at: toIsoFromUnixSeconds(job.dateFirstPublish),
      last_publish_at: toIsoFromUnixSeconds(job.dateLastPublish),

      // Sync fields:
      is_active: isActive,
      last_seen_at: nowIso,
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

    // (Optional) detect scheduled run
    const isScheduled = event?.headers?.["x-nf-scheduled"] === "true";

    // 1) Fetch all jobs from Taleez
    const jobs = await taleezFetchAllJobs();

    // Safety guardrail: prevent accidental mass-unpublish on API outage
    if (minExpected && jobs.length < minExpected) {
      throw new Error(
        `Safety stop: Taleez returned ${jobs.length} jobs (< ${minExpected}).`
      );
    }

    // 2) Load all existing Webflow items (staged)
    const existing = await wfListAllItems(collectionId);

    // Build lookup: taleez_id -> webflow item id
    const existingByTaleez = new Map();
    for (const item of existing) {
      const tId = item?.fieldData?.taleez_id;
      if (tId) existingByTaleez.set(String(tId), item.id);
    }

    // 3) Compute create/update batches and publish list
    const seen = new Set();
    const toCreate = [];
    const toUpdate = [];
    const publishIds = [];

    for (const job of jobs) {
      const tId = String(job.id);
      seen.add(tId);

      const mapped = mapJobToWebflow(job, nowIso);
      const existingId = existingByTaleez.get(tId);

      if (existingId) {
        toUpdate.push({ id: existingId, ...mapped });
        if (mapped.fieldData.is_active) publishIds.push(existingId);
      } else {
        toCreate.push(mapped);
      }
    }

    // 4) Create (staged)
    const createdIdsToPublish = [];
    for (const batch of chunk(toCreate, 100)) {
      const created = await wfBulkCreate(collectionId, batch);

      // Webflow may return created item ids; collect if present
      const createdItems = created?.items || [];
      for (const it of createdItems) {
        if (it?.fieldData?.is_active) createdIdsToPublish.push(it.id);
      }

      await sleep(250);
    }

    // 5) Update (staged)
    for (const batch of chunk(toUpdate, 100)) {
      await wfBulkUpdate(collectionId, batch);
      await sleep(250);
    }

    // 6) Publish active items (staged -> live)
    const uniquePublishIds = [...new Set([...publishIds, ...createdIdsToPublish])];
    await wfPublish(collectionId, uniquePublishIds);

    // 7) Soft-delete missing items: mark inactive + unpublish live
    const missingIds = [];
    const missingUpdates = [];

    for (const item of existing) {
      const tId = String(item?.fieldData?.taleez_id || "");
      if (!tId) continue;

      if (!seen.has(tId)) {
        missingIds.push(item.id);
        missingUpdates.push({
          id: item.id,
          fieldData: { is_active: false },
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