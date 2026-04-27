/**
 * CASCADE ENGINE — Multi-Cross-Conversion Core
 *
 * THE PLATFORM'S PRIMARY DIFFERENTIATOR.
 *
 * Whenever ANY source links new data to an existing person or incident,
 * this engine fires every applicable enricher in parallel, cross-examines
 * results, and repeats until either:
 *   - No new fields are filled (convergence)
 *   - Identity confidence reaches 95+
 *   - 3 iterations elapsed (cost guardrail)
 *
 * This makes the platform smarter than any single-source vendor.
 *
 * Public functions:
 *   enqueueCascade(db, personId, trigger) — drop a record into cascade_queue
 *   processCascadeQueue(db, opts) — drain the queue, fires cascades
 *   runCascadeForPerson(db, personId) — direct synchronous cascade
 */
const { v4: uuidv4 } = require('uuid');
const { reportError } = require('./_errors');
const { logChange } = require('./changelog');
const { crossExamine } = require('../enrich/cross-exam');
const { deepEnrichPerson } = require('../enrich/deep');
const trestle = require('../enrich/trestle');

let _queueTableEnsured = false;
async function ensureQueueTable(db) {
  if (_queueTableEnsured) return;
  try {
    await db.raw(`
      CREATE TABLE IF NOT EXISTS cascade_queue (
        id BIGSERIAL PRIMARY KEY,
        person_id UUID,
        incident_id UUID,
        trigger_source VARCHAR(80) NOT NULL,
        trigger_field VARCHAR(80),
        trigger_value TEXT,
        priority INTEGER DEFAULT 5,
        attempts INTEGER DEFAULT 0,
        status VARCHAR(20) DEFAULT 'pending',
        last_error TEXT,
        confidence_before INTEGER,
        confidence_after INTEGER,
        fields_filled INTEGER DEFAULT 0,
        sources_fired TEXT[],
        enqueued_at TIMESTAMPTZ DEFAULT NOW(),
        started_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ
      );
      CREATE INDEX IF NOT EXISTS idx_cascade_queue_status ON cascade_queue(status, priority DESC, enqueued_at);
      CREATE INDEX IF NOT EXISTS idx_cascade_queue_person ON cascade_queue(person_id);
    `);
    _queueTableEnsured = true;
  } catch (e) { console.error('cascade_queue table:', e.message); }
}

/**
 * Add a person to the cascade queue.
 * Should be called whenever any source inserts/updates a person record.
 */
async function enqueueCascade(db, optsOrPersonId, triggerSource, triggerField, triggerValue) {
  // Backward-compat: accept (db, personId, 'trigger') OR (db, {person_id, ...})
  let opts;
  if (typeof optsOrPersonId === 'string') {
    opts = { person_id: optsOrPersonId, trigger_source: triggerSource || 'unknown', trigger_field: triggerField, trigger_value: triggerValue };
  } else {
    opts = optsOrPersonId || {};
  }
  const { person_id, incident_id, trigger_source = 'unknown', trigger_field, trigger_value, priority = 5 } = opts;
  await ensureQueueTable(db);
  // Skip if already pending for this person within last hour (dedup)
  if (person_id) {
    const recent = await db('cascade_queue')
      .where('person_id', person_id)
      .where('status', 'pending')
      .where('enqueued_at', '>', new Date(Date.now() - 3600000))
      .first();
    if (recent) return { skipped: 'already_queued', queue_id: recent.id };
  }
  const [row] = await db('cascade_queue').insert({
    person_id, incident_id,
    trigger_source: String(trigger_source || 'unknown').substring(0, 80),
    trigger_field: trigger_field ? String(trigger_field).substring(0, 80) : null,
    trigger_value: trigger_value ? String(trigger_value).substring(0, 500) : null,
    priority,
    enqueued_at: new Date()
  }).returning('id');
  return { enqueued: true, queue_id: row.id || row };
}

/**
 * Run the full cascade for one person:
 *   1. Cross-examine current state
 *   2. If incomplete, fire deep-enrich (Trestle + PDL + Hunter + Tracerfy + SearchBug + NumVerify)
 *   3. Cross-examine again
 *   4. Repeat up to 3 iterations
 */
async function runCascadeForPerson(db, personId, opts = {}) {
  const maxIter = opts.maxIter || 3;
  const log = { person_id: personId, iterations: [], total_fields_filled: 0, sources_fired: new Set() };

  let person = await db('persons').where('id', personId).first();
  if (!person) return { error: 'person_not_found' };

  // Initial cross-exam
  let initialExam = await crossExamine(db, person);
  log.confidence_before = initialExam?.identity_confidence || 0;

  for (let iter = 0; iter < maxIter; iter++) {
    const iterLog = { iteration: iter + 1 };

    // Re-load person (may have been updated by previous iteration)
    person = await db('persons').where('id', personId).first();

    // Skip if confidence already maxed
    const exam = await crossExamine(db, person);
    if (exam?.identity_confidence >= 95) {
      iterLog.skipped = 'confidence_maxed';
      log.iterations.push(iterLog);
      break;
    }

    // Run deep enrich chain
    let deepResult;
    try {
      deepResult = await deepEnrichPerson(person, db);
    } catch (e) {
      iterLog.error = e.message;
      log.iterations.push(iterLog);
      break;
    }

    if (!deepResult || !deepResult.ok) {
      iterLog.skipped = 'no_new_data';
      log.iterations.push(iterLog);
      break;
    }

    // Property records lookup (FREE) — when address is known but full_name isn't
    if (person.address && !person.full_name && !deepResult.merged_fields.full_name) {
      try {
        const pr = require('../enrich/property-records');
        if (pr.lookupOwner) {
          const owner = await pr.lookupOwner({ address: person.address, city: person.city, state: person.state });
          if (owner?.owner_name) {
            deepResult.merged_fields.full_name = owner.owner_name;
            deepResult.sources_used = (deepResult.sources_used || []).concat('property_records');
          }
        }
      } catch (_) {}
    }

    // Apply merged fields (only fill empties — never overwrite higher-confidence)
    const update = { updated_at: new Date() };
    let filled = 0;
    const updateable = ['phone','email','address','city','state','zip','employer','occupation','linkedin_url','age','phone_verified','phone_carrier','phone_line_type','phone_secondary','full_name','first_name','last_name'];
    for (const f of updateable) {
      if (deepResult.merged_fields[f] && !person[f]) {
        update[f] = deepResult.merged_fields[f];
        filled++;
      }
    }

    if (filled > 0) {
      await db('persons').where('id', personId).update(update);
      log.total_fields_filled += filled;

      // Log to enrichment_logs
      for (const f of updateable) {
        if (update[f]) {
          try {
            await db('enrichment_logs').insert({
              person_id: personId,
              incident_id: person.incident_id,
              field_name: f,
              old_value: person[f] || null,
              new_value: String(update[f]).substring(0, 500),
              confidence: deepResult.confidence,
              verified: false,
              created_at: new Date()
            });
          } catch (_) {}
        }
      }
    }

    iterLog.fields_filled = filled;
    iterLog.sources = deepResult.sources_used || [];
    for (const s of (deepResult.sources_used || [])) log.sources_fired.add(s);
    iterLog.cost_estimate = deepResult.cost_estimate_usd || 0;
    log.iterations.push(iterLog);

    // If we filled nothing this iteration, stop (convergence)
    if (filled === 0) break;
  }

  // Final cross-exam
  person = await db('persons').where('id', personId).first();
  const finalExam = await crossExamine(db, person);
  log.confidence_after = finalExam?.identity_confidence || 0;
  log.confidence_delta = log.confidence_after - log.confidence_before;
  log.sources_fired = [...log.sources_fired];

  return log;
}

/**
 * Process the cascade queue — drain pending rows up to a budget.
 * Each cascade can take 5-30s; cap at 4-5 per invocation to fit Vercel timeout.
 */
async function processCascadeQueue(db, opts = {}) {
  await ensureQueueTable(db);
  const maxJobs = opts.maxJobs || 4;
  const startTime = Date.now();
  const TIME_BUDGET = 50000;

  const pending = await db('cascade_queue')
    .where('status', 'pending')
    .where('attempts', '<', 3)
    .orderBy('priority', 'desc')
    .orderBy('enqueued_at', 'asc')
    .limit(maxJobs);

  const results = [];
  for (const job of pending) {
    if (Date.now() - startTime > TIME_BUDGET) break;
    try {
      // Mark started
      await db('cascade_queue').where('id', job.id).update({
        status: 'running', started_at: new Date(),
        attempts: db.raw('attempts + 1')
      });

      let log = null;
      if (job.person_id) {
        log = await runCascadeForPerson(db, job.person_id);
      }

      // Mark completed
      await db('cascade_queue').where('id', job.id).update({
        status: 'completed',
        completed_at: new Date(),
        confidence_before: log?.confidence_before,
        confidence_after: log?.confidence_after,
        fields_filled: log?.total_fields_filled || 0,
        sources_fired: log?.sources_fired || []
      });
      results.push({ id: job.id, person_id: job.person_id, success: true, ...log });
    } catch (e) {
      await db('cascade_queue').where('id', job.id).update({
        status: 'pending',
        last_error: String(e.message).substring(0, 500)
      });
      results.push({ id: job.id, success: false, error: e.message });
      await reportError(db, 'cascade', job.person_id, e.message);
    }
  }

  return {
    processed: results.length,
    successful: results.filter(r => r.success).length,
    total_fields_filled: results.reduce((s, r) => s + (r.total_fields_filled || 0), 0),
    avg_confidence_delta: results.length ?
      Math.round(results.reduce((s, r) => s + (r.confidence_delta || 0), 0) / results.length) : 0,
    results
  };
}

module.exports = {
  enqueueCascade,
  processCascadeQueue,
  runCascadeForPerson,
  ensureQueueTable
};
