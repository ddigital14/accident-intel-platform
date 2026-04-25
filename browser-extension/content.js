// AIP Lead Claimer — content script
// Adds a "Claim" button to every QualifiedLeadRow on the dashboard.

const API_BASE = 'https://accident-intel-platform.vercel.app/api/v1';

function getToken() {
  return localStorage.getItem('aip_token');
}

async function claimLead(incidentId) {
  const token = getToken();
  if (!token) {
    alert('Please log in to AIP first');
    return false;
  }
  try {
    const resp = await fetch(`${API_BASE}/incidents/${incidentId}/assign`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ assign_to_me: true })
    });
    return resp.ok;
  } catch (e) {
    console.error('AIP: claim failed', e);
    return false;
  }
}

function injectClaimButtons() {
  // Find all incident rows that don't have a button yet
  const rows = document.querySelectorAll('.incident-row:not([data-aip-claimed])');
  for (const row of rows) {
    row.setAttribute('data-aip-claimed', '1');
    const btn = document.createElement('button');
    btn.textContent = '⚡ Claim';
    btn.className = 'aip-claim-btn';
    btn.onclick = async (e) => {
      e.stopPropagation();
      btn.textContent = '⏳ Claiming...';
      btn.disabled = true;
      // Try to find incident id from a known attribute or React fiber
      const incId = row.getAttribute('data-incident-id') ||
                    row.querySelector('[data-incident-id]')?.getAttribute('data-incident-id');
      if (!incId) {
        btn.textContent = '❌ No ID';
        return;
      }
      const ok = await claimLead(incId);
      btn.textContent = ok ? '✓ Claimed' : '✗ Failed';
      btn.disabled = !ok;
      if (ok) btn.style.background = '#34d399';
    };
    row.appendChild(btn);
  }
}

// Watch for new rows (dashboard auto-refreshes)
const observer = new MutationObserver(injectClaimButtons);
observer.observe(document.body, { childList: true, subtree: true });
injectClaimButtons();
