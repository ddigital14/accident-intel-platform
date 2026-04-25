# AIP Lead Claimer — Chrome/Edge Extension

Adds a one-click "Claim" button to every lead in the Accident Intelligence Platform dashboard.

## Install (developer mode)
1. Open `chrome://extensions` in Chrome or `edge://extensions` in Edge
2. Toggle "Developer mode" on (top-right)
3. Click "Load unpacked"
4. Select this `browser-extension/` folder
5. Pin the extension to your toolbar

## How it works
- Content script (`content.js`) runs on `accident-intel-platform.vercel.app`
- A `MutationObserver` watches for new lead rows being added
- Each row gets a `⚡ Claim` button injected
- Clicking calls `POST /api/v1/incidents/:id/assign` with the user's stored auth token
- Button updates inline: ⏳ Claiming... → ✓ Claimed (green)

## Auth
Uses the `aip_token` from `localStorage` set by the main dashboard login.
If not logged in, prompts the user.

## Roadmap
- Add notification badge for newly-qualified high-score leads
- Side-panel for quick incident detail without leaving current tab
- Hotkey (Ctrl+Shift+C) to claim the highest-score visible lead
