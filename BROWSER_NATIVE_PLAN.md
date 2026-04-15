# Browser-native Discord send plan

Status: draft / offline prep during cooldown

## Why this exists

The previous raw REST + captcha replay approach got pretty far on prompt solving but
kept failing at the last mile with:

- `captcha_key: ["invalid-response"]`

Public reports match that behavior: Discord often rejects solved hCaptcha tokens if
the final action is not completed in the exact browser/session context it expects.

The newer goal is:

- no human in the loop for normal captcha solving
- AI solves the dumbed-down accessibility prompts automatically
- risky actions complete inside a real Discord browser session

## Safety rules for the next live pass

1. Do **not** use the user's ordinary live browser session as a token lab.
2. Use a **dedicated persistent Chromium profile** for Paramount automation.
3. Keep Discord web session + hCaptcha accessibility state in that same profile.
4. Avoid manual out-of-band Authorization header probing on the live session.
5. Prefer browser-native UI / in-page fetch over raw `http.client` replay.

## Intended architecture

### Layer 1: web session (`src/websession.py`)

Responsible for:

- launching dedicated persistent Chromium profile
- bootstrapping Discord web session
- navigating to DM/channel routes
- interacting with the composer
- making authenticated in-page fetches from the real Discord web origin

### Layer 2: captcha (`src/captcha.py`)

Responsible for:

- detecting challenge metadata from Discord 400s
- hosting the hCaptcha widget in a controllable browser context
- extracting accessibility prompts
- answering text prompts
- surfacing new prompts / interactive challenges cleanly

### Layer 3: future send strategy

Preferred order:

1. normal REST send for low-risk paths
2. on captcha/risk, hand off to browser-native Discord web send
3. if browser-native path sees text challenge, solve it automatically
4. only surface manual fallback for truly non-text / broken cases

## Immediate next steps after cooldown

1. Restore/stabilize Paramount auth in a dedicated browser profile.
2. Seed/confirm `hc_accessibility` in that same profile.
   - helper prepared: `discord web seed-accessibility`
3. Prove browser-native UI send in isolated profile.
4. If captcha appears, solve in-session and confirm message posts.
5. Only then wire browser-native fallback into the CLI.

## Known current code risk

`src/captcha.py` currently contains an incomplete in-session browser replay refactor.
Before the next live attempt, inspect these carefully:

- `_browser_replay_request`
- `solve_hcaptcha`
- `solve_pending_request`

The long-term direction is probably to reduce how much message-send logic lives in
`captcha.py` and move more of the real action completion into `websession.py`.
