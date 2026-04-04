/**
 * ONE-TIME cleanup: Remove all generated/fake data from the database
 * DELETE this file after running once
 * GET /api/v1/migrate/cleanup-fake-data?secret=cleanup-now
 */
const { getDb } = require('../../_db');

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.query.secret !== 'cleanup-now') {
    return res.status(401).json({ error: 'Provide ?secret=cleanup-now' });
  }

  const db = getDb();
  const log = [];

  try {
    // 1. Delete fake enrichment logs (generated_fallback entries)
    const delLogs = await db('enrichment_logs')
      .where('verified', false)
      .whereNull('source_url')
      .del();
    log.push(`Deleted ${delLogs} fake enrichment logs`);

    // 2. Delete generated (GDOT) incidents and their related records using subqueries
    // (avoids massive WHERE IN lists that exceed query limits)
    const gdotSubquery = db('source_reports')
      .where('source_reference', 'like', 'GDOT-%')
      .whereNotNull('incident_id')
      .select('incident_id');

    const gdotCount = await db('source_reports').where('source_reference', 'like', 'GDOT-%').count('* as c').first();

    if (parseInt(gdotCount.c) > 0) {
      // Delete persons tied to generated incidents
      const delPersons = await db('persons').whereIn('incident_id', gdotSubquery).del();
      log.push(`Deleted ${delPersons} persons from generated incidents`);

      // Delete vehicles tied to generated incidents
      const delVehicles = await db('vehicles').whereIn('incident_id', gdotSubquery).del();
      log.push(`Deleted ${delVehicles} vehicles from generated incidents`);

      // Delete enrichment logs tied to generated incident persons
      const delELogs = await db('enrichment_logs').whereIn('incident_id', gdotSubquery).del();
      log.push(`Deleted ${delELogs} enrichment logs from generated incidents`);

      // Delete cross-references tied to generated incidents
      const delXrefs = await db('cross_references').whereIn('incident_id', gdotSubquery).del();
      log.push(`Deleted ${delXrefs} cross-references from generated incidents`);

      // Delete the generated incidents themselves
      const delIncidents = await db('incidents').whereIn('id', gdotSubquery).del();
      log.push(`Deleted ${delIncidents} generated incidents`);

      // Delete the source reports
      const delReports = await db('source_reports').where('source_reference', 'like', 'GDOT-%').del();
      log.push(`Deleted ${delReports} GDOT source reports`);
    } else {
      log.push('No GDOT generated incidents found');
    }

    // 3. Null out fake fallback fields on remaining persons
    // Null out generated phone numbers (pattern: (4xx) xxx-xxxx or (7xx) xxx-xxxx)
    const nullPhones = await db('persons')
      .whereRaw("phone ~ '^\\([4-7][0-9]{2}\\) [0-9]{3}-[0-9]{4}$'")
      .update({
        phone: null,
        phone_verified: null,
        phone_carrier: null,
        phone_line_type: null,
        phone_location: null
      });
    log.push(`Nulled ${nullPhones} fake phone numbers`);

    // Null out generated emails (pattern: firstname.lastnameNN@domain.com)
    const nullEmails = await db('persons')
      .whereRaw("email ~ '^[a-z]+\\.[a-z]+[0-9]+@(gmail|yahoo|outlook|aol|hotmail|icloud)\\.com$'")
      .update({ email: null, email_verified: null });
    log.push(`Nulled ${nullEmails} fake emails`);

    // Null out randomly generated employers (from the hardcoded list)
    const fakeEmployers = ['Delta Air Lines', 'Coca-Cola Company', 'Home Depot', 'UPS', 'Georgia-Pacific',
      'Cox Enterprises', 'SunTrust Bank', 'AT&T Southeast', 'Grady Health System', 'Emory Healthcare',
      'Self-Employed', 'Publix Super Markets', 'Waffle House Inc', 'Georgia Power', 'NCR Corporation'];
    const nullEmployers = await db('persons')
      .whereIn('employer', fakeEmployers)
      .whereNull('linkedin_url')  // If they have a LinkedIn URL, PDL found them — keep it
      .update({ employer: null, occupation: null });
    log.push(`Nulled ${nullEmployers} fake employers/occupations`);

    // Null out randomly generated insurance
    const fakeInsurers = ['State Farm', 'Geico', 'Progressive', 'Allstate', 'USAA',
      'Liberty Mutual', 'Farmers Insurance', 'Nationwide', 'Travelers', 'American Family'];
    const nullInsurance = await db('persons')
      .whereIn('insurance_company', fakeInsurers)
      .update({ insurance_company: null, policy_limits: null });
    log.push(`Nulled ${nullInsurance} fake insurance entries`);

    // Null out randomly generated attorney names
    const fakeFirms = ['Morgan & Morgan', 'Bader Scott Injury Lawyers', 'The Millar Law Firm',
      'Scholle Law', 'Kaine Law', 'Fried Rogers Goldberg LLC', 'Butler Prather LLP', 'Kenneth S. Nugent P.C.'];
    const nullAttorneys = await db('persons')
      .whereIn('attorney_name', fakeFirms)
      .update({ has_attorney: null, attorney_name: null });
    log.push(`Nulled ${nullAttorneys} fake attorney entries`);

    // Null out randomly generated addresses (Peachtree, Ponce, etc from the list)
    const fakeStreets = ['Peachtree St NE', 'Ponce de Leon Ave', 'Northside Dr NW', 'Piedmont Ave',
      'Spring St NW', 'Marietta St', 'Decatur St SE', 'Memorial Dr SE', 'Boulevard SE', 'North Ave NE'];
    const streetPattern = fakeStreets.map(s => `address LIKE '%${s}%'`).join(' OR ');
    const nullAddresses = await db('persons')
      .whereRaw(`(${streetPattern})`)
      .whereNull('linkedin_url')  // Keep if PDL-verified
      .update({ address: null, city: null, state: null, zip: null });
    log.push(`Nulled ${nullAddresses} fake addresses`);

    // Null out random income ranges
    const nullIncome = await db('persons')
      .whereNotNull('household_income_range')
      .whereNull('linkedin_url')  // Keep if PDL confirmed
      .update({ household_income_range: null });
    log.push(`Nulled ${nullIncome} random income ranges`);

    // 4. Recalculate enrichment scores for all persons
    // Score should only reflect real data now
    const allPersons = await db('persons').select('id', 'first_name', 'last_name', 'phone', 'email',
      'address', 'age', 'date_of_birth', 'employer', 'insurance_company', 'injury_description',
      'transported_to', 'has_attorney', 'occupation', 'linkedin_url', 'litigator', 'deceased',
      'property_owner');

    let recalculated = 0;
    for (const p of allPersons) {
      let score = 0;
      if (p.phone) score += 15;
      if (p.email) score += 15;
      if (p.address) score += 12;
      if (p.first_name && p.last_name) score += 10;
      if (p.age || p.date_of_birth) score += 5;
      if (p.employer) score += 8;
      if (p.insurance_company) score += 10;
      if (p.injury_description) score += 8;
      if (p.transported_to) score += 7;
      if (p.has_attorney !== null) score += 5;
      if (p.occupation) score += 5;
      if (p.linkedin_url) score += 3;
      if (p.litigator !== undefined && p.litigator !== null) score += 4;
      if (p.deceased !== undefined && p.deceased !== null) score += 2;
      if (p.property_owner !== undefined && p.property_owner !== null) score += 2;

      await db('persons').where('id', p.id).update({
        enrichment_score: Math.min(score, 100),
        updated_at: new Date()
      });
      recalculated++;
    }
    log.push(`Recalculated enrichment scores for ${recalculated} persons`);

    // 5. Get final counts
    const finalCounts = {};
    for (const t of ['incidents', 'persons', 'vehicles', 'enrichment_logs', 'cross_references']) {
      const r = await db.raw(`SELECT COUNT(*) as count FROM ${t}`);
      finalCounts[t] = parseInt(r.rows[0].count, 10);
    }

    const avgScore = await db.raw(`SELECT ROUND(AVG(enrichment_score)::numeric, 1) as avg FROM persons`);

    res.json({
      success: true,
      cleanup_log: log,
      final_counts: finalCounts,
      avg_enrichment_score: avgScore.rows[0].avg,
      message: 'All fake data removed. Only real API-sourced data remains.'
    });
  } catch (err) {
    res.status(500).json({ error: err.message, log });
  }
};
