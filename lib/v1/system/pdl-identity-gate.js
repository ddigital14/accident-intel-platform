/**
 * Phase 46: PDL/Apollo/scraper identity-match gate.
 * Rejects geo-mismatched namesake collisions before persisting to victim's persons row.
 */
function isGeoConsistent(candidateCity, candidateState, incidentCity, incidentState) {
  if (!incidentState) return true;
  if (!candidateState) return false;
  if (candidateState.toUpperCase() !== incidentState.toUpperCase()) return false;
  if (candidateCity && incidentCity) {
    const a = candidateCity.toLowerCase().replace(/[^a-z]/g, '');
    const b = incidentCity.toLowerCase().replace(/[^a-z]/g, '');
    if (a === b || a.includes(b) || b.includes(a)) return true;
    return true;
  }
  return true;
}

async function validateAndPersist(db, personId, source, candidateData, incidentData) {
  const ok = isGeoConsistent(candidateData.city, candidateData.state, incidentData.city, incidentData.state);
  if (!ok) {
    try {
      await db('enrichment_logs').insert({
        person_id: personId,
        action: 'identity_gate_reject',
        meta: JSON.stringify({
          source,
          candidate_geo: `${candidateData.city || '?'}/${candidateData.state || '?'}`,
          incident_geo: `${incidentData.city || '?'}/${incidentData.state || '?'}`
        }),
        created_at: new Date()
      });
    } catch (_) {}
    return { ok: false, rejected: true, reason: 'geo_mismatch' };
  }
  return { ok: true };
}

module.exports = { isGeoConsistent, validateAndPersist };
