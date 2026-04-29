/**
 * Predictive Caller-ID Speed-Dial Card — Phase 44B
 * GET /api/v1/dashboard/caller-id?secret=ingest-now&phone=<E164>
 * GET /api/v1/dashboard/caller-id?secret=ingest-now&person_id=<uuid>
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const repCallBriefHandler = require('./rep-call-brief');

const SECRET = 'ingest-now';

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

function normalizePhone(p) {
  if (!p) return null;
  const digits = String(p).replace(/\D/g, '');
  if (digits.length === 10) return '+1' + digits;
  if (digits.length === 11 && digits.startsWith('1')) return '+' + digits;
  return digits ? '+' + digits : null;
}

function daysSince(date) {
  if (!date) return null;
  const t = new Date(date).getTime();
  if (Number.isNaN(t)) return null;
  return Math.floor((Date.now() - t) / 86400000);
}

async function findPersonByPhone(db, phone) {
  const e164 = normalizePhone(phone);
  const last10 = (e164 || phone || '').replace(/\D/g, '').slice(-10);
  let p = await db('persons').where('phone', e164).orderBy('identity_confidence', 'desc').first().catch(() => null);
  if (!p && last10) {
    p = await db('persons').whereRaw("REGEXP_REPLACE(phone, '[^0-9]', '', 'g') LIKE ?", ['%' + last10])
      .orderBy('identity_confidence', 'desc').first().catch(() => null);
  }
  return p;
}

async function gatherCard(db, person) {
  if (!person) return null;
  const card = {
    person_id: person.id, full_name: person.full_name, phone: person.phone, email: person.email,
    role: person.role, age: person.age, city: person.city, state: person.state,
    identity_confidence: person.identity_confidence,
    injury_severity: person.injury_severity,
    has_attorney: person.has_attorney === true ? 'YES — represented' :
                  person.has_attorney === false ? 'NO — unrepresented' : 'unknown',
    contact_quality: person.contact_quality,
    phone_recently_swapped: !!person.phone_recently_swapped,
    phone_is_voip: !!person.phone_is_voip
  };
  if (person.incident_id) {
    try {
      const inc = await db('incidents').where('id', person.incident_id).first();
      if (inc) {
        card.incident = {
          id: inc.id, headline: inc.headline, city: inc.city, state: inc.state,
          accident_at: inc.accident_at || inc.created_at,
          severity: inc.severity, fatal: !!inc.fatal,
          qualification_state: inc.qualification_state, lead_score: inc.lead_score
        };
        card.days_since_incident = daysSince(inc.accident_at || inc.created_at);
      }
    } catch (_) {}
  }
  try {
    const fam = await db('persons')
      .where('incident_id', person.incident_id).whereNot('id', person.id)
      .orderBy('identity_confidence', 'desc').limit(6);
    card.family = (fam || []).map(f => ({
      name: f.full_name, relationship: f.relationship || f.role,
      phone: f.phone, email: f.email
    }));
  } catch (_) { card.family = []; }
  const days = card.days_since_incident;
  if (card.incident?.fatal && days != null && days < 7) {
    card.empathy_flag = 'RECENT_FATALITY';
    card.note_suggestion = 'If recent fatality (<7d), lead with empathy not legal questions. Open with condolences and offer to call back at a better time.';
  } else if (card.has_attorney === 'YES — represented') {
    card.empathy_flag = 'ALREADY_RETAINED';
    card.note_suggestion = 'Family is already represented — do not solicit; offer second-opinion consultation only.';
  } else if (card.phone_recently_swapped) {
    card.empathy_flag = 'PHONE_SIM_SWAPPED';
    card.note_suggestion = 'Phone shows recent SIM swap. Verify identity before sharing case details.';
  } else if (card.phone_is_voip) {
    card.empathy_flag = 'VOIP_NUMBER';
    card.note_suggestion = 'VoIP number — likely not the primary mobile. Ask for a callback number.';
  } else {
    card.empathy_flag = 'STANDARD';
    card.note_suggestion = 'Lead with: confirm name, mention you saw the news, ask if everyone is ok, then pivot to legal.';
  }
  return card;
}

async function attachRepBrief(db, person, card) {
  try {
    const fakeReq = {
      method: 'GET',
      query: { secret: SECRET, person_id: person.id },
      headers: { 'x-cron-secret': SECRET }
    };
    const fakeRes = {
      _payload: null, _status: 200,
      status(c) { this._status = c; return this; },
      json(p) { this._payload = p; return this; },
      setHeader() {}, end() {}
    };
    const fn = (typeof repCallBriefHandler === 'function')
      ? repCallBriefHandler
      : (repCallBriefHandler.handler || repCallBriefHandler.default);
    if (fn) {
      await fn(fakeReq, fakeRes);
      if (fakeRes._payload) card.rep_brief = fakeRes._payload.brief || fakeRes._payload;
    }
  } catch (e) {
    card.rep_brief_error = e.message;
  }
  return card;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  const db = getDb();
  try {
    let person = null;
    if (req.query?.person_id) {
      person = await db('persons').where('id', req.query.person_id).first();
    } else if (req.query?.phone) {
      person = await findPersonByPhone(db, req.query.phone);
    } else {
      return res.status(400).json({ error: 'phone or person_id required' });
    }
    if (!person) {
      return res.status(404).json({
        success: false, error: 'no_match',
        searched: req.query?.phone ? { phone: req.query.phone } : { person_id: req.query?.person_id }
      });
    }
    let card = await gatherCard(db, person);
    card = await attachRepBrief(db, person, card);
    return res.json({ success: true, card, timestamp: new Date().toISOString() });
  } catch (e) {
    try { await reportError(db, 'caller-id', null, e.message); } catch (_) {}
    return res.status(500).json({ success: false, error: e.message });
  }
};
module.exports.gatherCard = gatherCard;
module.exports.findPersonByPhone = findPersonByPhone;
