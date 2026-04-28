/**
 * REP QUICK LINKS — Phase 40 Module 3 part B
 *
 * For a given person_id, returns pre-populated search URLs across every
 * paid + free + DIY tool a rep might want to consult manually.
 *
 * GET /api/v1/dashboard/rep-quick-links?secret=ingest-now&person_id=<uuid>
 *
 * Returns: { person, links: { apollo, pdl, fastpeoplesearch, ... } }
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');

const SECRET = 'ingest-now';

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

const enc = (s) => encodeURIComponent(String(s || '').trim());
const slug = (s) => String(s || '').trim().toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');

function buildLinks(p) {
  const fullName = p.full_name || [p.first_name, p.last_name].filter(Boolean).join(' ');
  const first = p.first_name || (fullName.split(/\s+/)[0] || '');
  const last  = p.last_name  || (fullName.split(/\s+/).slice(-1)[0] || '');
  const city  = p.city || '';
  const state = (p.state || '').toUpperCase();
  const stateLow = state.toLowerCase();
  const phone = p.phone ? p.phone.replace(/\D+/g, '') : '';
  const email = p.email || '';

  const links = {};

  if (fullName) {
    // ── B2B / paid ──
    links.apollo = `https://app.apollo.io/#/people?q=${enc(fullName)}${city ? `&qOrganizationLocations[]=${enc(city)}` : ''}`;
    links.pdl    = `https://dashboard.peopledatalabs.com/search?name=${enc(fullName)}`;

    // ── Free/freemium people-search ──
    if (last && state) {
      links.fastpeoplesearch = `https://www.fastpeoplesearch.com/name/${slug(first + '-' + last)}_${stateLow}`;
    } else if (last) {
      links.fastpeoplesearch = `https://www.fastpeoplesearch.com/name/${slug(first + '-' + last)}`;
    }
    if (first && last) {
      links.thatsthem = `https://thatsthem.com/name/${slug(first)}-${slug(last)}`;
      links.truepeoplesearch = `https://www.truepeoplesearch.com/results?name=${enc(first + ' ' + last)}${state ? '&citystatezip=' + enc(state) : ''}`;
      links.spokeo = `https://www.spokeo.com/${slug(first + '-' + last)}`;
      links.beenverified = `https://www.beenverified.com/people/${slug(first + '-' + last)}/`;
      links.whitepages = `https://www.whitepages.com/name/${slug(first + '-' + last)}${state ? '/' + state : ''}`;
      links.peoplefinders = `https://www.peoplefinders.com/people/${slug(first + '-' + last)}`;
    }

    // ── Google CSE pre-canned queries ──
    links.cse_obit   = `https://www.google.com/search?q=${enc(`"${fullName}" obituary ${city} ${state}`)}`;
    links.cse_linkedin = `https://www.google.com/search?q=${enc(`"${fullName}" site:linkedin.com/in ${city} ${state}`)}`;
    links.cse_facebook = `https://www.google.com/search?q=${enc(`"${fullName}" site:facebook.com ${city} ${state}`)}`;
    links.cse_news = `https://www.google.com/search?q=${enc(`"${fullName}" accident OR crash ${city} ${state}`)}&tbm=nws`;
    links.cse_funeral = `https://www.google.com/search?q=${enc(`"${fullName}" "survived by" ${city} ${state}`)}`;
    links.legacy = `https://www.legacy.com/us/obituaries/search?firstName=${enc(first)}&lastName=${enc(last)}${state ? '&location=' + enc(state) : ''}`;
    links.dignitymemorial = `https://www.dignitymemorial.com/obituaries?searchType=name&firstName=${enc(first)}&lastName=${enc(last)}`;

    // ── Public-records ──
    if (state === 'AZ') {
      links.maricopa_property = `https://mcassessor.maricopa.gov/mcs.php?q=${enc(fullName)}`;
    }
    if (state === 'TX') {
      links.harris_property  = `https://www.hcad.org/quick-search/?searchType=name&searchValue=${enc(fullName)}`;
      links.travis_property  = `https://search.tcadcentral.org/Search/Result?keywords=${enc(fullName)}`;
      links.tx_court = `https://search.txcourts.gov/Search.aspx?search=${enc(fullName)}`;
    }
    if (state === 'IL') {
      links.cook_property = `https://assessor.cookcountyil.gov/Search?searchterm=${enc(fullName)}`;
    }
    if (state === 'GA') {
      links.fulton_property = `https://iaspublicaccess.fultoncountyga.gov/search/CommonSearch.aspx?mode=OWNER&owner=${enc(fullName)}`;
    }
    if (state === 'CA') {
      links.la_property = `https://portal.assessor.lacounty.gov/parceldetail/?owner=${enc(fullName)}`;
    }
    if (state === 'FL') {
      links.fl_voter = `https://registration.elections.myflorida.com/CheckVoterStatus`;
    }

    // ── Court / litigation ──
    links.courtlistener = `https://www.courtlistener.com/?q=${enc(fullName)}&type=r`;
    links.unicourt = `https://unicourt.com/search?q=${enc(fullName)}`;
    if (state) {
      links.state_court = `https://www.google.com/search?q=${enc(`${fullName} ${city} ${state} site:.gov court records`)}`;
    }

    // ── Social ──
    links.facebook_search = `https://www.facebook.com/search/people/?q=${enc(fullName + ' ' + city)}`;
    links.linkedin_search = `https://www.linkedin.com/search/results/people/?keywords=${enc(fullName + ' ' + city)}`;
    links.x_search = `https://x.com/search?q=${enc(fullName)}&f=user`;
    links.instagram_search = `https://www.google.com/search?q=${enc(`"${fullName}" site:instagram.com`)}`;
  }

  // ── Phone lookups ──
  if (phone) {
    links.phone_truecaller = `https://www.truecaller.com/search/us/${enc(phone)}`;
    links.phone_whitepages = `https://www.whitepages.com/phone/${enc(phone)}`;
    links.phone_thatsthem  = `https://thatsthem.com/phone/${enc(phone.replace(/^1/, '').replace(/(\d{3})(\d{3})(\d{4})/, '$1-$2-$3'))}`;
    links.phone_google = `https://www.google.com/search?q=${enc(phone)}`;
  }

  // ── Email lookups ──
  if (email) {
    links.email_hunter = `https://hunter.io/email-finder?domain=${enc((email.split('@')[1] || ''))}`;
    links.email_emailrep = `https://emailrep.io/${enc(email)}`;
    links.email_haveibeen = `https://haveibeenpwned.com/account/${enc(email)}`;
    links.email_google = `https://www.google.com/search?q=${enc(`"${email}"`)}`;
  }

  // ── Address lookups ──
  if (p.address) {
    links.address_thatsthem = `https://thatsthem.com/address/${enc(p.address)}`;
    links.address_zillow = `https://www.zillow.com/homes/${enc(p.address + ' ' + (city || '') + ' ' + state)}_rb/`;
    links.address_google = `https://www.google.com/search?q=${enc(`"${p.address}" ${city} ${state}`)}`;
  }

  return links;
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  const db = getDb();
  try {
    const personId = req.query?.person_id;
    let person = null;
    if (personId) {
      person = await db('persons').where('id', personId).first();
      if (!person) return res.status(404).json({ error: 'person_not_found' });
    } else if (req.query?.victim_name) {
      person = {
        full_name: req.query.victim_name,
        first_name: (String(req.query.victim_name).split(/\s+/)[0] || ''),
        last_name:  (String(req.query.victim_name).split(/\s+/).slice(-1)[0] || ''),
        city: req.query.city || null,
        state: req.query.state || null,
        phone: req.query.phone || null,
        email: req.query.email || null,
        address: req.query.address || null
      };
    } else {
      return res.status(400).json({ error: 'person_id or victim_name required' });
    }

    const links = buildLinks(person);
    return res.json({
      success: true,
      person: {
        id: person.id || null,
        full_name: person.full_name,
        city: person.city,
        state: person.state,
        phone: person.phone || null,
        email: person.email || null,
        address: person.address || null
      },
      links,
      link_count: Object.keys(links).length,
      timestamp: new Date().toISOString()
    });
  } catch (e) {
    await reportError(db, 'dashboard-rep-quick-links', null, e.message);
    res.status(500).json({ success: false, error: e.message });
  }
}

module.exports = handler;
module.exports.handler = handler;
module.exports.buildLinks = buildLinks;
