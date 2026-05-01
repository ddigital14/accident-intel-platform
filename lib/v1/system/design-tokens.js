/**
 * Phase 52: ACC Design System tokens — canonical JSON.
 *
 * GET /api/v1/system/design-tokens?secret=ingest-now
 *
 * Returns the authoritative theme palette so internal pages, embeds,
 * email templates, browser extensions, and 3rd-party integrations all
 * pull the same values. Mirrors frontend/src/theme.css.
 */
const { getDb } = require('../../_db');
const { trackApiCall } = require('./cost');
const { bumpCounter } = require('./_cei_telemetry');

const TOKENS = {
  version: '1.0.0',
  phase: 52,
  generated_at: null,
  colors: {
    red:           '#EF4444',
    red_dark:      '#B91C1C',
    red_bg:        '#FEE2E2',
    charcoal:      '#1F2937',
    slate:         '#475569',
    slate_light:   '#94A3B8',
    border:        '#E2E8F0',
    divider:       '#CBD5E1',
    bg:            '#FFFFFF',
    bg_soft:       '#F8FAFC',
    indigo:        '#6366F1',
    indigo_soft:   '#C7D2FE',
    dds_orange:    '#FF6600',
    success:       '#10B981',
    warning:       '#F59E0B'
  },
  typography: {
    display: "'Space Grotesk', system-ui, -apple-system, 'Helvetica Neue', sans-serif",
    body:    "'Inter', system-ui, -apple-system, sans-serif",
    mono:    "'JetBrains Mono', ui-monospace, 'SF Mono', monospace",
    sizes: {
      header_brand: '18px', header_subtitle: '10px',
      lead_name: '22px', tab_label: '14px',
      button: '14px', badge: '11px', meta: '12px', helper: '11px'
    },
    weights: { regular: 400, medium: 500, semibold: 600, bold: 700 }
  },
  shadows: {
    sm: '0 1px 3px rgba(31,41,55,0.06)',
    md: '0 4px 12px rgba(31,41,55,0.08)',
    lg: '0 12px 32px rgba(31,41,55,0.12)'
  },
  radius:    { sm: '4px', md: '8px', lg: '12px' },
  motion:    { ease: 'cubic-bezier(0.4, 0, 0.2, 1)', duration_fast: '150ms', duration: '200ms', duration_slow: '320ms' },
  logo: {
    full_url:     '/logo.svg',
    mark_url:     '/logomark.svg',
    full_viewbox: '0 0 680 460',
    mark_viewbox: '0 0 240 240'
  },
  badges: {
    qualified: { color: '#B91C1C', bg: '#FEE2E2', border: '#FECACA', dot: '#EF4444' },
    pending:   { color: '#475569', bg: '#F8FAFC', border: '#E2E8F0', dot: '#94A3B8' },
    verified:  { color: '#4338CA', bg: '#C7D2FE', border: '#A5B4FC', dot: '#6366F1' },
    validated: { color: '#047857', bg: '#D1FAE5', border: '#6EE7B7', dot: '#10B981' }
  },
  buttons: {
    primary:   { bg: '#1F2937', text: '#FFFFFF', hover_bg: '#334155' },
    secondary: { bg: '#FFFFFF', text: '#1F2937', border: '#E2E8F0', hover_bg: '#F8FAFC' },
    danger:    { bg: '#EF4444', text: '#FFFFFF', hover_bg: '#B91C1C' },
    ghost:     { bg: 'transparent', text: '#1F2937', hover_bg: '#F8FAFC' }
  },
  voice: {
    tone: 'confident, intelligent, slightly cinematic, with personality but professional',
    short_tagline: 'Accident Command Center',
    parent_tagline: 'by Donovan Digital Solutions'
  }
};

const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 'public, max-age=300, s-maxage=300');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  const t0 = Date.now();
  let db = null;
  try { db = getDb(); } catch (_) {}

  try {
    const out = { ...TOKENS, generated_at: new Date().toISOString() };
    if (db) {
      await trackApiCall(db, 'design-tokens', 'fetch', 0, 0, true).catch(() => {});
      await bumpCounter(db, 'design-tokens', true, Date.now() - t0).catch(() => {});
    }
    return res.status(200).json({ success: true, tokens: out });
  } catch (e) {
    if (db) await bumpCounter(db, 'design-tokens', false, Date.now() - t0).catch(() => {});
    return res.status(500).json({ error: e.message });
  }
};
