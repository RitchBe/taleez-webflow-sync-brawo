import { schedule } from "@netlify/functions";

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

// Bulk create staged items
async function wfBulkCreate(collectionId, items) {
  // Webflow bulk create endpoint for staged items:
  return wfFetch(`/collections/${collectionId}/items/bulk`, {
    method: "POST",
    body: { items },
  });
}

// Bulk update staged items
async function wfBulkUpdate(collectionId, items) {
  return wfFetch(`/collections/${collectionId}/items`, {
    method: "PATCH",
    body: { items },
  });
}

// Publish staged items to live
async function wfPublish(collectionId, itemIds) {
  if (!itemIds.length) return;
  return wfFetch(`/collections/${collectionId}/items/publish`, {
    method: "POST",
    body: { itemIds },
  });
}

// Unpublish live items
async function wfUnpublish(collectionId, itemIds) {
  if (!itemIds.length) return;
  return wfFetch(`/collections/${collectionId}/items/live`, {
    method: "DELETE",
    body: { itemIds },
  });
}

// --- Taleez ---
async function taleezFetchAllJobs() {
  const base = process.env.TALEEZ_BASE_URL || "https://api.taleez.com";
  const path = process.env.TALEEZ_OFFERS_PATH || "/0/jobs";
  const secret = process.env.TALEEZ_API_SECRET;

  const pageSize = 200; // try big to reduce requests (adjust if Taleez caps it)
  let page = 0;
  let all = [];

  while (true) {
    const url = new URL(`${base}${path}`);
    url.searchParams.set("page", String(page));
    url.searchParams.set("pageSize", String(pageSize));

    const res = await fetch(url.toString(), {
      headers: { "X-taleez-api-secret": secret },
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Taleez fetch failed: ${res.status} ${text}`);
    }

    const data = await res.json();

    // handle both shapes: array or {items:[]}
    const items = Array.isArray(data) ? data : (data.items || []);
    all.push(...items);

    // stop condition: last page
    if (items.length < pageSize) break;

    page += 1;
    await sleep(100);
  }

  return all;
}

// ---- mapping ----
// IMPORTANT: these keys must match your Webflow field slugs.
// I’m using the slugs we discussed; adjust if you named differently.
function mapJobToWebflow(job, nowIso) {
  const taleezId = String(job.id);
  const title = job.label || `Job ${taleezId}`;
  const slug = `${slugify(title)}-${taleezId}`.slice(0, 240);

  // You should refine this once you confirm real "open" statuses.
  const isActive =
    job.visibility === "PUBLIC" &&
    job.currentStatus !== "DONE";

  return {
    fieldData: {
      name: title,
      slug,

      taleez_id: taleezId,
      taleez_token: job.token || "",

      status: job.currentStatus || "",
      visibility: job.visibility || "",

      contract: job.contract || "",
      contract_length_value: job.contractLength ?? null,
      contract_length_unit: job.contractLengthTimeUnit || "",

      full_time: !!job.fullTime,
      work_hours: job.workHours ?? null,
      remote: !!job.remote,

      country: job.country || "",
      city: job.city || "",
      postal_code: job.postalCode ? String(job.postalCode) : "",
      location_full: [job.city, job.postalCode, job.country].filter(Boolean).join(", "),

      lat: job.lat ?? null,
      lng: job.lng ?? null,

      company_label: job.companyLabel || "",
      company_website: job.website || "",
      company_logo_url: job.logo || "",
      company_banner_url: job.banner || "",

      offer_url: job.url || "",
      apply_url: job.urlApplying || "",

      job_description: job.jobDescription || "",
      profile_description: job.profileDescription || "",
      company_description: job.companyDescription || "",

      tags_text: Array.isArray(job.tags) ? job.tags.join(", ") : "",

      created_at: toIsoFromUnixSeconds(job.dateCreation),
      first_publish_at: toIsoFromUnixSeconds(job.dateFirstPublish),
      last_publish_at: toIsoFromUnixSeconds(job.dateLastPublish),

      is_active: isActive,
      last_seen_at: nowIso,
    },
  };
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

export const handler = schedule("*/30 * * * *", async () => {
  const collectionId = process.env.WEBFLOW_COLLECTION_ID;
  const minExpected = Number(process.env.MIN_EXPECTED_COUNT || "0");
  const nowIso = new Date().toISOString();

  // 1) fetch Taleez jobs
  const jobs = await taleezFetchAllJobs();

  // safety guardrail
  if (minExpected && jobs.length < minExpected) {
    throw new Error(
      `Safety stop: Taleez returned ${jobs.length} jobs (< ${minExpected}).`
    );
  }

  // 2) load existing Webflow items and map taleez_id -> item.id
  const existing = await wfListAllItems(collectionId);
  const existingByTaleez = new Map();
  for (const item of existing) {
    const tId = item?.fieldData?.taleez_id;
    if (tId) existingByTaleez.set(String(tId), item.id);
  }

  // 3) prepare upserts
  const seen = new Set();
  const toCreate = [];
  const toUpdate = [];
  const toPublish = [];

  for (const job of jobs) {
    const tId = String(job.id);
    seen.add(tId);

    const mapped = mapJobToWebflow(job, nowIso);
    const existingId = existingByTaleez.get(tId);

    if (existingId) {
      toUpdate.push({ id: existingId, ...mapped });
      if (mapped.fieldData.is_active) toPublish.push(existingId);
    } else {
      toCreate.push(mapped);
    }
  }

  // 4) write to Webflow (staged)
  // Create first
  let createdIdsToPublish = [];
  for (const batch of chunk(toCreate, 100)) {
    const created = await wfBulkCreate(collectionId, batch);
    // The response shape can vary; safest is: refetch later if needed.
    // If Webflow returns created items with ids, collect them:
    const createdItems = created?.items || [];
    for (const it of createdItems) {
      // publish only if active
      if (it?.fieldData?.is_active) createdIdsToPublish.push(it.id);
    }
    await sleep(250);
  }

  // Updates
  for (const batch of chunk(toUpdate, 100)) {
    await wfBulkUpdate(collectionId, batch);
    await sleep(250);
  }

  // 5) publish active items
  await wfPublish(collectionId, [...new Set([...toPublish, ...createdIdsToPublish])]);

  // 6) soft-delete missing (unpublish + mark inactive)
  const missingIds = [];
  const missingUpdates = [];

  for (const item of existing) {
    const tId = String(item?.fieldData?.taleez_id || "");
    if (!tId) continue;

    if (!seen.has(tId)) {
      missingIds.push(item.id);
      missingUpdates.push({
        id: item.id,
        fieldData: {
          is_active: false,
        },
      });
    }
  }

  // mark inactive
  for (const batch of chunk(missingUpdates, 100)) {
    await wfBulkUpdate(collectionId, batch);
    await sleep(250);
  }

  // unpublish live
  for (const batch of chunk(missingIds, 100)) {
    await wfUnpublish(collectionId, batch);
    await sleep(250);
  }

  return {
    statusCode: 200,
    body: JSON.stringify({
      ok: true,
      taleezJobs: jobs.length,
      webflowExisting: existing.length,
      created: toCreate.length,
      updated: toUpdate.length,
      published: [...new Set([...toPublish, ...createdIdsToPublish])].length,
      unpublished: missingIds.length,
      runAt: nowIso,
    }),
  };
});