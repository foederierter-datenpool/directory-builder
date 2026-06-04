import path from "path"
import fs from "fs"

// Static-file source:
//   - organisations_filtered.json     --> names, webseite, hierarchy
//   - organisations_all_filtered.json --> BAGFW service categories
//   - locations_filtered.json         --> addresses
// Org records share one id per org (the clean step unifies them); location
// records carry an organisation_id back-link, joined to orgs in the clean step.

const OUT_DIR = process.argv[2]
const SRC_DIR = process.argv[3]
const { plz: PLZS = [] } = JSON.parse(process.argv[4] || "{}")

const read = (f) => JSON.parse(fs.readFileSync(path.join(SRC_DIR, f), "utf8")).data

// The static files cover all of Berlin
const inPlz = (plz) => PLZS.length === 0 || PLZS.includes(String(plz))
const orgs    = read("organisations_filtered.json")
const orgsAll = read("organisations_all_filtered.json")
const keptLocations = read("locations_filtered.json").filter((l) => inPlz(l.plz))

let orgInScope = () => true
if (PLZS.length) {   // DEV scoping — delete this block when no more filters apply
    const inScope = new Set(keptLocations.map((l) => l.organisation_id))
    // Orgs with no address row (the AWO "Pflege" branch is exposed only via the
    // services file) still ride in on a service that serves a kept PLZ.
    for (const o of orgsAll)
        for (const s of o.services ?? [])
            if (inPlz(s.location?.plz)) inScope.add(o.id)
    orgInScope = (o) => inScope.has(o.id)
}

const records = [...orgs.filter(orgInScope), ...orgsAll.filter(orgInScope), ...keptLocations]

fs.mkdirSync(OUT_DIR, { recursive: true })
const outPath = path.join(OUT_DIR, "awo.json")
fs.writeFileSync(outPath, JSON.stringify(records, null, 2))
console.log(`  ${records.length} records (3 endpoints, PLZs ${PLZS.join("/") || "all"}) → ${outPath}`)
