import fs from 'fs';
import path from 'path';
import { execSync } from 'child_process';

// Requires Node 18+ (uses the built-in global `fetch` — no node-fetch dependency needed).

const CLIENT_ID = process.env.OSU_CLIENT_ID;
const CLIENT_SECRET = process.env.OSU_CLIENT_SECRET;
const COUNTRY = "IQ";

// --- CONFIGURATION ---
// Every ruleset we track. A player only gets checked for a given mode if
// they're actually on that mode's IQ country leaderboard — so someone ranked
// in osu! and mania gets checked in both, but not in taiko/catch.
const MODES = ["osu", "taiko", "fruits", "mania"];

const MANUAL_USER_IDS = []; // Add specific IDs here (e.g. [123456, 789101]) to always track them, in every mode, regardless of rank.

// Opt-out system. Add player IDs here that you want to completely ignore, in every mode.
const IGNORED_USER_IDS = [99999999, 88888888];

// Only keep scores set within this window. Without this, a player who hasn't
// played in months would still re-contribute the same old "recent" scores to
// every day's archive forever. That was harmless at 500 players, but across
// the full country list in 4 modes it would bloat every day's file with stale
// repeats and bury genuinely new plays. 24h keeps "today" meaning today.
const MAX_SCORE_AGE_HOURS = 24;

// --- SMART THROTTLING ---
// osu!'s current API guidance is to stay at/under ~60 requests/minute with
// exponential backoff on errors — real users have been banned for bursty
// patterns even under the documented ceiling, so this runs strictly one
// request at a time (no parallel batches) and backs off hard on any sign of
// trouble, easing back toward full speed only after a long clean streak.
const RATE_LIMIT_PER_MINUTE = Number(process.env.OSU_RPM || 55);
const BASE_INTERVAL_MS = 60000 / RATE_LIMIT_PER_MINUTE;
const MAX_INTERVAL_MS = BASE_INTERVAL_MS * 8;
const MAX_RETRIES = 5;

// GitHub-hosted runners hard-kill a job at 360 minutes with no way to extend
// it. We stop starting new work well before that so there's time to save +
// commit whatever we've collected and exit cleanly, instead of losing
// progress to a hard kill mid-request. Tune via OSU_MAX_RUNTIME_MINUTES if
// you're running this somewhere without that cap (e.g. a self-hosted runner).
const MAX_RUNTIME_MINUTES = Number(process.env.OSU_MAX_RUNTIME_MINUTES || 320);
const startTime = Date.now();
const minutesElapsed = () => (Date.now() - startTime) / 60000;
const timeBudgetExceeded = () => minutesElapsed() >= MAX_RUNTIME_MINUTES;

// Commit+push progress this often (wall-clock), so a hard kill loses at most
// one interval's worth of work instead of the whole run. Local disk saves
// happen more often than this (see CHECKPOINT_EVERY_N_PLAYERS below); this
// just controls how often those saves also get pushed to git.
const COMMIT_INTERVAL_MS = 15 * 60 * 1000;
const CHECKPOINT_EVERY_N_PLAYERS = 250;

// Helpers
function mkdirp(dir){if(!fs.existsSync(dir)) fs.mkdirSync(dir,{recursive:true});}
function today(){return new Date().toISOString().split('T')[0];}
const sleep = (ms) => new Promise(res => setTimeout(res, ms));

// --- SMART RATE LIMITER ---
// One request at a time, spaced at least `intervalMs` apart. Any 429 doubles
// the interval (up to a cap); a long streak of clean requests slowly earns
// the pace back down toward the base rate.
class RateLimiter {
  constructor(baseIntervalMs, maxIntervalMs) {
    this.baseIntervalMs = baseIntervalMs;
    this.maxIntervalMs = maxIntervalMs;
    this.intervalMs = baseIntervalMs;
    this.lastRequestAt = 0;
    this.cleanStreak = 0;
  }
  async waitForSlot() {
    const wait = this.intervalMs - (Date.now() - this.lastRequestAt);
    if (wait > 0) await sleep(wait);
    this.lastRequestAt = Date.now();
  }
  onSuccess() {
    this.cleanStreak++;
    if (this.cleanStreak % 40 === 0 && this.intervalMs > this.baseIntervalMs) {
      this.intervalMs = Math.max(this.baseIntervalMs, this.intervalMs * 0.8);
    }
  }
  onRateLimited() {
    this.cleanStreak = 0;
    this.intervalMs = Math.min(this.maxIntervalMs, this.intervalMs * 2);
  }
}
const limiter = new RateLimiter(BASE_INTERVAL_MS, MAX_INTERVAL_MS);

let accessToken = null;
async function getToken(){
  const res = await fetch('https://osu.ppy.sh/oauth/token',{
    method:'POST',headers:{'Content-Type':'application/json'},
    body: JSON.stringify({client_id:CLIENT_ID,client_secret:CLIENT_SECRET,grant_type:'client_credentials',scope:'public'})
  });
  const data = await res.json();
  if (!data.access_token) throw new Error('osu! auth failed — check OSU_CLIENT_ID / OSU_CLIENT_SECRET.');
  return data.access_token;
}

// Rate-limited GET with retry + backoff. Returns parsed JSON, or null if the
// resource doesn't exist / repeatedly fails — callers just skip and move on.
async function apiGet(url, attempt = 1) {
  await limiter.waitForSlot();

  let res;
  try {
    res = await fetch(url, { headers: { Authorization: `Bearer ${accessToken}` } });
  } catch (networkErr) {
    if (attempt >= MAX_RETRIES) { console.error(`   ⚠️ network error, giving up: ${url} (${networkErr.message})`); return null; }
    await sleep(Math.min(MAX_INTERVAL_MS * 4, 2000 * 2 ** attempt));
    return apiGet(url, attempt + 1);
  }

  if (res.status === 401) {
    accessToken = await getToken();
    if (attempt >= MAX_RETRIES) { console.error(`   ⚠️ still unauthorized after re-auth, giving up: ${url}`); return null; }
    return apiGet(url, attempt + 1);
  }

  if (res.status === 429) {
    limiter.onRateLimited();
    if (attempt >= MAX_RETRIES) { console.error(`   ⚠️ still rate-limited after ${MAX_RETRIES} tries, giving up: ${url}`); return null; }
    const retryAfter = Number(res.headers.get('retry-after'));
    const wait = retryAfter > 0 ? retryAfter * 1000 : Math.min(MAX_INTERVAL_MS * 4, 2000 * 2 ** attempt);
    console.warn(`   ⏸️  429 rate-limited — backing off ${Math.round(wait/1000)}s (attempt ${attempt}/${MAX_RETRIES}, pace now ${Math.round(limiter.intervalMs)}ms/req)`);
    await sleep(wait);
    return apiGet(url, attempt + 1);
  }

  if (res.status >= 500) {
    if (attempt >= MAX_RETRIES) { console.error(`   ⚠️ server error ${res.status}, giving up: ${url}`); return null; }
    await sleep(Math.min(MAX_INTERVAL_MS * 4, 1500 * 2 ** attempt));
    return apiGet(url, attempt + 1);
  }

  if (res.status === 404) return null;

  if (!res.ok) { console.error(`   ⚠️ unexpected status ${res.status}: ${url}`); return null; }

  limiter.onSuccess();
  try { return await res.json(); } catch { return null; }
}

// Every ranked player in one mode's IQ leaderboard — paginated until exhausted.
async function getAllCountryRankings(mode) {
  const ids = new Set();
  let page = 1;
  const SAFETY_MAX_PAGES = 500; // 25,000 players — a guard against an API quirk looping forever, not a real cap
  while (page <= SAFETY_MAX_PAGES) {
    if (timeBudgetExceeded()) { console.warn(`   ⏹️  time budget hit while paginating ${mode} rankings`); break; }
    const data = await apiGet(`https://osu.ppy.sh/api/v2/rankings/${mode}/performance?country=${COUNTRY}&cursor[page]=${page}`);
    const rows = data && Array.isArray(data.ranking) ? data.ranking : [];
    if (rows.length === 0) break;
    rows.forEach(r => ids.add(r.user.id));
    if (rows.length < 50) break; // short page = last page
    page++;
  }
  return ids;
}

// Recent scores for one user, in one specific mode, filtered to the last
// MAX_SCORE_AGE_HOURS.
async function getRecentScores(userId, mode) {
  const data = await apiGet(`https://osu.ppy.sh/api/v2/users/${userId}/scores/recent?mode=${mode}&limit=100&include_fails=0`);
  if (!Array.isArray(data)) return [];

  const cutoff = Date.now() - MAX_SCORE_AGE_HOURS * 3600 * 1000;
  const results = [];
  for (const s of data) {
    try {
      if (new Date(s.created_at).getTime() < cutoff) continue;
      if (!s.beatmapset) continue; // shouldn't happen, but don't let one odd score kill the whole batch
      results.push({
        user: s.user.username,
        user_id: s.user.id,
        country: COUNTRY,
        mode,
        score_id: s.id,
        rank: s.rank,
        accuracy: s.accuracy,
        pp: s.pp,
        mods: s.mods,
        combo: s.max_combo,
        created_at: s.created_at,
        beatmapset: {
          id: s.beatmapset.id,
          title: s.beatmapset.title,
          cover: s.beatmapset.covers.card,
          stars: s.beatmap ? s.beatmap.difficulty_rating : 0
        }
      });
    } catch { /* skip malformed entry, keep going */ }
  }
  return results;
}

// Commits + pushes whatever's currently on disk under the given paths. Used
// both as a mid-run safety net and for the final save, so a job that gets
// killed (soft budget miss, runner timeout, whatever) loses at most the
// interval since the last checkpoint — not the whole run.
let gitIdentityReady = false;
function commitCheckpoint(message, paths = ['data']) {
  try {
    if (!gitIdentityReady) {
      execSync('git config user.name "github-actions[bot]"');
      execSync('git config user.email "github-actions[bot]@users.noreply.github.com"');
      gitIdentityReady = true;
    }
    execSync(`git add ${paths.join(' ')}`);
    const status = execSync('git status --porcelain').toString().trim();
    if (!status) return;
    execSync(`git commit -m ${JSON.stringify(message)}`);
    execSync('git push');
    console.log(`   💾 committed & pushed: ${message}`);
  } catch (e) {
    console.error(`   ⚠️ commit/push failed, continuing anyway: ${e.message.split('\n')[0]}`);
  }
}

function saveResults(scores, meta) {
  const uniqueScores = Array.from(new Map(scores.map(s => [s.score_id, s])).values());
  uniqueScores.sort((a,b) => new Date(b.created_at) - new Date(a.created_at));

  const dateParts = today().split('-');
  const dir = path.join('data', dateParts[0], dateParts[1]);
  mkdirp(dir);

  fs.writeFileSync(path.join(dir, dateParts[2]+'.json'), JSON.stringify({
    date: today(),
    country: COUNTRY,
    modes: MODES,
    generated_at: new Date().toISOString(),
    complete: meta.complete,
    players_checked: meta.playersChecked,
    players_total: meta.playersTotal,
    scores: uniqueScores
  }, null, 2));

  // Update index.json
  const indexFile = path.join('data','index.json');
  let indexData = { available_dates: [] };
  if(fs.existsSync(indexFile)) indexData = JSON.parse(fs.readFileSync(indexFile, 'utf8'));
  if(!indexData.available_dates.includes(today())) {
    indexData.available_dates.push(today());
    indexData.available_dates.sort();
  }
  fs.writeFileSync(indexFile, JSON.stringify(indexData, null, 2));

  return uniqueScores.length;
}

async function main(){
  accessToken = await getToken();

  // 1. Full rankings, per mode. This part is cheap even at full scale — it's
  // the per-player recent-score lookups below that take real time.
  const modeUserIds = {};
  for (const mode of MODES) {
    console.log(`📡 Pulling full ${mode} rankings for ${COUNTRY}...`);
    const ranked = await getAllCountryRankings(mode);
    MANUAL_USER_IDS.forEach(id => ranked.add(id));
    IGNORED_USER_IDS.forEach(id => ranked.delete(id));
    modeUserIds[mode] = ranked;
    console.log(`   ✅ ${mode}: ${ranked.size} players to check`);
  }

  const playersTotal = Object.values(modeUserIds).reduce((sum, set) => sum + set.size, 0);
  if (playersTotal === 0) {
    console.warn('⚠️  No players found in any mode — check API connectivity/credentials before trusting an empty result.');
  }

  let playersChecked = 0;
  let allScores = [];
  let hitTimeBudget = false;
  let lastCommitAt = Date.now();

  // 2. Recent scores, per mode, per player — the expensive part.
  outer:
  for (const mode of MODES) {
    const userList = Array.from(modeUserIds[mode]);
    console.log(`⏳ Checking ${userList.length} ${mode} players for recent scores...`);
    for (let i = 0; i < userList.length; i++) {
      if (timeBudgetExceeded()) {
        console.warn(`⏹️  Time budget (${MAX_RUNTIME_MINUTES}m) reached — stopping early and saving what we have.`);
        hitTimeBudget = true;
        break outer;
      }

      const scores = await getRecentScores(userList[i], mode);
      allScores.push(...scores);
      playersChecked++;

      if (playersChecked % CHECKPOINT_EVERY_N_PLAYERS === 0) {
        console.log(`   …${playersChecked}/${playersTotal} players checked (${allScores.length} scores so far)`);
        saveResults(allScores, { complete: false, playersChecked, playersTotal });
        if (Date.now() - lastCommitAt >= COMMIT_INTERVAL_MS) {
          commitCheckpoint(`Checkpoint: ${playersChecked}/${playersTotal} players (${today()})`);
          lastCommitAt = Date.now();
        }
      }
    }
  }

  // 3. Final save + commit.
  const savedCount = saveResults(allScores, { complete: !hitTimeBudget, playersChecked, playersTotal });
  console.log(`🚀 Done! Archived ${savedCount} scores from ${playersChecked}/${playersTotal} players checked${hitTimeBudget ? ' (partial — time budget reached)' : ''}.`);

  // 4. Update HTML footers.
  console.log("📝 Updating HTML footers with latest timestamp and commit...");
  const commitHash = process.env.GITHUB_SHA ? process.env.GITHUB_SHA.substring(0, 7) : 'local';
  const updateTime = new Date().toISOString().replace('T', ' ').substring(0, 16) + ' UTC';
  const htmlFiles = ['Test.html', 'OIT_Document.html', 'index.html', 'rankings.html', 'videos.html', 'mapping.html', 'archive.html', 'socials.html', 'Communityy.html'];

  for (const file of htmlFiles) {
    if (fs.existsSync(file)) {
      let content = fs.readFileSync(file, 'utf8');
      if (content.includes('id="footer-commit"') && content.includes('id="footer-time"')) {
          content = content.replace(/(<span[^>]*id="footer-commit"[^>]*>).*?(<\/span>)/is, `$1${commitHash}$2`);
          content = content.replace(/(<span[^>]*id="footer-time"[^>]*>).*?(<\/span>)/is, `$1${updateTime}$2`);
          fs.writeFileSync(file, content, 'utf8');
          console.log(`   ✅ Updated footer in ${file}`);
      } else {
          console.log(`   ⚠️ Skipping ${file}: Footer IDs not found.`);
      }
    }
  }

  commitCheckpoint(`Daily fetch: ${savedCount} scores, ${playersChecked}/${playersTotal} players (${today()})`, ['data', '*.html']);
}

main().catch(err => {
  console.error('❌ Fatal error:', err);
  process.exit(1);
});
