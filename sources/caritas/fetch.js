import path from "path"
import fs from "fs"

const OUT_DIR = process.argv[2]
const URL = process.argv[3]
const { plz: PLZS = [] } = JSON.parse(process.argv[4] || "{}")

fs.mkdirSync(OUT_DIR, { recursive: true })

// One merged file, not one per PLZ: the lift step spawns a JVM per raw file, so
// at nationwide PLZ counts per-file output would mean thousands of JVM starts.
// Records are deduped by Guid (a centre can surface under several PLZ queries).
const byGuid = new Map()
for (const plz of PLZS) {
    const result = await fetch(URL, {
        method: "POST",
        headers: { "Content-Type": "application/json", "Accept": "application/json" },
        body: JSON.stringify({
            WebsiteGuid: "52c60690-787a-40ac-965c-a087c020c5f5",
            ModuleGuid:  "e38bf59d-2afb-4bc8-9d78-26618f6909af",
            Location:    plz,
        }),
    })
    const json = await result.json()
    for (const rec of json) byGuid.set(rec.Guid, rec)
    console.log(`  ${plz}: ${json.length} records (running total ${byGuid.size})`)
}

const outPath = path.join(OUT_DIR, "caritas.json")
fs.writeFileSync(outPath, JSON.stringify([...byGuid.values()], null, 2))
console.log(`  ${byGuid.size} unique records → ${outPath}`)
