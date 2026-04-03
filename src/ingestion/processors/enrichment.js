/**
 * AI ENRICHMENT PROCESSOR
 * Uses OpenAI GPT to:
 *   - Generate consolidated descriptions from multiple sources
 *   - Classify incident severity and type with more accuracy
 *   - Extract structured data from unstructured text
 *   - Score lead quality for rep prioritization
 */

const db = require('../../config/database');
const { logger } = require('../../utils/logger');

async function runEnrichmentCycle(io) {
  // Find incidents that have multiple sources but haven't been AI-enriched
  const incidents = await db('incidents')
    .where('source_count', '>=', 2)
    .where(function () {
      this.whereNull('ai_analysis').orWhereRaw("ai_analysis->>'enriched_at' IS NULL");
    })
    .where('confidence_score', '<', 80)
    .orderBy('priority', 'asc')
    .limit(10);

  if (incidents.length === 0) return;
  logger.info(`Enrichment: processing ${incidents.length} incidents`);

  for (const incident of incidents) {
    try {
      await enrichIncident(incident, io);
    } catch (err) {
      logger.error(`Enrichment failed for ${incident.id}:`, err.message);
    }
  }
}

async function enrichIncident(incident, io) {
  // Gather all source reports
  const sources = await db('source_reports')
    .leftJoin('data_sources', 'source_reports.data_source_id', 'data_sources.id')
    .where('source_reports.incident_id', incident.id)
    .select('source_reports.parsed_data', 'source_reports.source_type', 'data_sources.name as source_name');

  const persons = await db('persons').where('incident_id', incident.id);
  const vehicles = await db('vehicles').where('incident_id', incident.id);

  // Build AI prompt
  const sourceTexts = sources.map(s => {
    const parsed = typeof s.parsed_data === 'string' ? JSON.parse(s.parsed_data) : s.parsed_data;
    return `[${s.source_name || s.source_type}]: ${parsed?.description || JSON.stringify(parsed).substring(0, 500)}`;
  }).join('\n');

  const prompt = `You are analyzing accident incident data from multiple sources to create a consolidated intelligence report.

INCIDENT DATA FROM ${sources.length} SOURCES:
${sourceTexts}

PERSONS INVOLVED: ${persons.length}
${persons.map(p => `- ${p.full_name || 'Unknown'} (${p.role}) - Injured: ${p.is_injured} - Insurance: ${p.insurance_company || 'Unknown'}`).join('\n')}

VEHICLES: ${vehicles.length}
${vehicles.map(v => `- ${v.year || ''} ${v.make || ''} ${v.model || ''} - Commercial: ${v.is_commercial}`).join('\n')}

Generate a JSON response with:
{
  "consolidated_description": "A 2-3 sentence factual summary of what happened, combining all source information",
  "incident_type": "car_accident|motorcycle_accident|truck_accident|work_accident|pedestrian|bicycle|slip_fall|other",
  "severity": "fatal|critical|serious|moderate|minor",
  "confidence_score": 0-100,
  "lead_quality_score": 0-100,
  "lead_quality_reasons": ["reason1", "reason2"],
  "key_facts": ["fact1", "fact2", "fact3"],
  "recommended_priority": 1-10,
  "flags": ["any notable flags like commercial vehicle, multi-party, possible DUI, etc"],
  "estimated_claim_value": "low|medium|high|very_high",
  "claim_value_factors": ["factor1", "factor2"]
}`;

  try {
    const OpenAI = require('openai');
    const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

    if (!process.env.OPENAI_API_KEY) {
      // Fallback: rule-based enrichment without AI
      await ruleBasedEnrichment(incident, sources, persons, vehicles);
      return;
    }

    const completion = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [{ role: 'user', content: prompt }],
      response_format: { type: 'json_object' },
      temperature: 0.2,
      max_tokens: 800
    });

    const analysis = JSON.parse(completion.choices[0].message.content);

    // Update incident with AI analysis
    await db('incidents').where({ id: incident.id }).update({
      description: analysis.consolidated_description || incident.description,
      incident_type: analysis.incident_type || incident.incident_type,
      severity: analysis.severity || incident.severity,
      confidence_score: Math.min(100, Math.max(incident.confidence_score, analysis.confidence_score || 60)),
      priority: analysis.recommended_priority || incident.priority,
      ai_analysis: JSON.stringify({
        ...analysis,
        enriched_at: new Date().toISOString(),
        model: 'gpt-4o-mini',
        source_count: sources.length
      }),
      tags: [...(incident.tags || []), ...(analysis.flags || [])].filter((v, i, a) => a.indexOf(v) === i)
    });

    logger.info(`Enriched incident ${incident.id}: confidence=${analysis.confidence_score}, lead_quality=${analysis.lead_quality_score}`);

    // Emit update via WebSocket
    if (io) {
      io.to('all-incidents').emit('incident:enriched', { id: incident.id, analysis });
    }

  } catch (err) {
    logger.error('AI enrichment error:', err.message);
    await ruleBasedEnrichment(incident, sources, persons, vehicles);
  }
}

async function ruleBasedEnrichment(incident, sources, persons, vehicles) {
  let confidence = Math.min(100, 30 + (sources.length * 15));
  let leadScore = 30;

  // Boost for injured persons without attorneys
  const injuredNoAttorney = persons.filter(p => p.is_injured && !p.has_attorney).length;
  leadScore += injuredNoAttorney * 20;

  // Boost for known insurance
  const insuredCount = persons.filter(p => p.insurance_company).length;
  leadScore += insuredCount * 10;

  // Boost for commercial vehicles
  if (vehicles.some(v => v.is_commercial)) leadScore += 25;

  // Boost for multiple vehicles
  if (vehicles.length >= 3) leadScore += 15;

  // Boost for high severity
  if (['fatal', 'critical'].includes(incident.severity)) leadScore += 20;
  if (incident.severity === 'serious') leadScore += 10;

  leadScore = Math.min(100, leadScore);

  await db('incidents').where({ id: incident.id }).update({
    confidence_score: confidence,
    ai_analysis: JSON.stringify({
      lead_quality_score: leadScore,
      enriched_at: new Date().toISOString(),
      model: 'rule_based',
      source_count: sources.length,
      injured_no_attorney: injuredNoAttorney,
      insured_count: insuredCount,
      has_commercial: vehicles.some(v => v.is_commercial)
    })
  });
}

module.exports = { runEnrichmentCycle, enrichIncident };
