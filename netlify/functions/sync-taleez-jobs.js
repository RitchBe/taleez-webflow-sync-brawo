// netlify/functions/sync-taleez-jobs.js
//
// SCHEDULED function, free-tier friendly. Hard 30s ceiling, so this is built to:
//   1. do only the work that actually changed (skip unchanged jobs entirely),
//   2. publish brand-new / changed / not-yet-live items only,
//   3. stay idempotent + RESUMABLE under a soft time budget — if a run can't
//      finish, it does what it can and the next run continues. No mid-run crash.
//
// Schedule lives in netlify.toml:
//   [functions."sync-taleez-jobs"]
//     schedule = "@hourly"

const WF_BASE = "https://api.webflow.com/v2";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Stop starting new work ~7s before the 30s wall, to leave room to return.
const SOFT_BUDGET_MS = 23000;

// Fields this sync owns. Used for change detection. "last-seen-at" is excluded
// because it changes every run and would make everything look "changed".
const VOLATILE_FIELDS = new Set(["last-seen-at"]);

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

// Normalize a field value so "", null, undefined and missing keys all compare
// equal, and everything else compares by JSON value.
function norm(v) {
  return v === null || v === undefined || v === "" ? "" : JSON.stringify(v);
}

// True if the managed fields differ between what we'd write and what's stored.
function fieldsChanged(mappedFieldData, existingFieldData = {}) {
  for (const key of Object.keys(mappedFieldData)) {
    if (VOLATILE_FIELDS.has(key)) continue;
    if (norm(mappedFieldData[key]) !== norm(existingFieldData[key])) return true;
  }
  return false;
}

// An item needs publishing if it's never been published or is still a draft.
function needsPublish(item) {
  return !item?.lastPublished || item?.isDraft === true;
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
    await sleep(120);
  }
  return all;
}

async function wfBulkCreate(collectionId, items) {
  return wfFetch(`/collections/${collectionId}/items`, {
    method: "POST",
    body: { items },
  });
}

async function wfBulkUpdate(collectionId, items) {
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

async function wfDeleteItems(collectionId, itemIds) {
  if (!itemIds.length) return;
  return wfFetch(`/collections/${collectionId}/items`, {
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
    url.searchParams.set("withDetails", "true");

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

  const ACTIVE_VISIBILITY = new Set(["PUBLIC", "INTERNAL_AND_PUBLIC"]);
  const isActive =
    ACTIVE_VISIBILITY.has(job.visibility) && job.currentStatus === "PUBLISHED";

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

// -------------------- Sync logic --------------------
const syncHandler = async (event) => {
  const startedAt = Date.now();
  const deadline = startedAt + SOFT_BUDGET_MS;
  const haveTime = () => Date.now() < deadline;

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

    // Guardrail: refuse to proceed (and possibly delete) on a suspicious fetch.
    if (minExpected && jobs.length < minExpected) {
      throw new Error(
        `Safety stop: Taleez returned ${jobs.length} jobs (< ${minExpected}).`
      );
    }

    // 2) Existing Webflow items (single full pass)
    const existing = await wfListAllItems(collectionId);
    const existingByTaleez = new Map();
    for (const item of existing) {
      const tId = item?.fieldData?.["taleez-id"];
      if (tId) existingByTaleez.set(String(tId), item);
    }

    // 3) Build the plan in memory (NO api calls — always completes).
    //    `seen` is the complete set of currently-active Taleez jobs, so the
    //    delete step below is safe even if writes get truncated by the budget.
    const seen = new Set();
    const toCreate = [];
    const toUpdate = [];
    const publishIds = new Set();

    for (const job of jobs) {
      const tId = String(job.id);
      const mapped = mapJobToWebflow(job, nowIso);
      const active = mapped.fieldData["is-active"] === true;

      if (!active) continue; // ignore inactive jobs entirely
      seen.add(tId);

      const existingItem = existingByTaleez.get(tId);

      if (!existingItem) {
        toCreate.push(mapped); // new -> create + publish
      } else if (fieldsChanged(mapped.fieldData, existingItem.fieldData)) {
        toUpdate.push({ id: existingItem.id, ...mapped }); // changed -> update + publish
        publishIds.add(existingItem.id);
      } else if (needsPublish(existingItem)) {
        // unchanged, but not live yet (e.g. created by a previous truncated run)
        publishIds.add(existingItem.id);
      }
      // else: unchanged AND already live -> do nothing
    }

    // 4) Create new items (budgeted). Collect their IDs to publish.
    let partial = false;
    let created = 0;
    for (const batch of chunk(toCreate, 100)) {
      if (!haveTime()) { partial = true; break; }
      const res = await wfBulkCreate(collectionId, batch);
      for (const it of res?.items || []) {
        if (it?.id) publishIds.add(it.id);
      }
      created += batch.length;
      await sleep(200);
    }

    // 5) Update changed items (budgeted)
    let updated = 0;
    if (!partial) {
      for (const batch of chunk(toUpdate, 100)) {
        if (!haveTime()) { partial = true; break; }
        await wfBulkUpdate(collectionId, batch);
        updated += batch.length;
        await sleep(200);
      }
    }

    // 6) Publish everything that's new / changed / not-yet-live (budgeted)
    let published = 0;
    const publishList = [...publishIds];
    for (const batch of chunk(publishList, 100)) {
      if (!haveTime()) { partial = true; break; }
      await wfPublish(collectionId, batch);
      published += batch.length;
      await sleep(200);
    }

    // 7) Remove items that are no longer in Taleez's active list.
    //    Safe because `seen` is complete. Only runs if we still have budget;
    //    otherwise it's deferred to the next run (deletes are idempotent).
    const missingIds = [];
    for (const item of existing) {
      const tId = String(item?.fieldData?.["taleez-id"] || "");
      if (tId && !seen.has(tId)) missingIds.push(item.id);
    }

    let deleted = 0;
    if (!partial && missingIds.length) {
      for (const batch of chunk(missingIds, 100)) {
        if (!haveTime()) { partial = true; break; }
        await wfUnpublish(collectionId, batch); // remove from live
        await wfDeleteItems(collectionId, batch); // remove from CMS
        deleted += batch.length;
        await sleep(200);
      }
    } else if (missingIds.length) {
      partial = true; // had deletions to do but ran out of budget
    }

    return {
      statusCode: 200,
      body: JSON.stringify({
        ok: true,
        partial, // true => some work deferred to the next scheduled run
        scheduled: isScheduled,
        taleezJobs: jobs.length,
        webflowExisting: existing.length,
        created,
        updated,
        published,
        deleted,
        skippedUnchanged:
          seen.size - created - updated, // active jobs that needed no write
        elapsedMs: Date.now() - startedAt,
        runAt: nowIso,
      }),
    };
  } catch (err) {
    console.error("SYNC ERROR:", err?.stack || err);
    return {
      statusCode: 500,
      body: JSON.stringify({ ok: false, error: String(err?.message || err) }),
    };
  }
};

// Scheduling is configured in netlify.toml (no npm dependency needed).
export const handler = syncHandler;