/**
 * Helper: set Vercel edge cache headers on read-only public endpoints.
 * Usage at the top of a handler:
 *   require('./_edge_cache').apply(res, { sMaxAge: 60, swr: 300 });
 */
function apply(res, { sMaxAge = 60, swr = 300 } = {}) {
  if (!res || typeof res.setHeader !== 'function') return;
  res.setHeader('Cache-Control', `public, s-maxage=${sMaxAge}, stale-while-revalidate=${swr}`);
  res.setHeader('CDN-Cache-Control', `s-maxage=${sMaxAge}`);
  res.setHeader('Vercel-CDN-Cache-Control', `s-maxage=${sMaxAge}`);
}
module.exports = { apply };
