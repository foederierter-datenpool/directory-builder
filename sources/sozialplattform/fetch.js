import path from "path"
import fs from "fs"

const OUT_DIR = process.argv[2]
const BASE_URL = process.argv[3]
const { plz: PLZS = [] } = JSON.parse(process.argv[4] || "{}")
const PER_PAGE = 100

const fetchPage = async (plz, page) => {
    const url = `${BASE_URL}?place=${plz}&page=${page}&itemsPerPage=${PER_PAGE}`
    const res = await fetch(url)
    if (!res.ok) throw new Error(`Page ${page}: HTTP ${res.status}`)
    const json = await res.json()
    if (json.status !== "success") throw new Error(`Page ${page}: API status ${json.status}`)
    return json.data
}

fs.mkdirSync(OUT_DIR, { recursive: true })

// One merged file, not one per PLZ: the lift step spawns a JVM per raw file, so
// per-PLZ output would multiply JVM starts. Offers are deduped by offer_id (one
// office can serve several PLZ queries).
const byId = new Map()
for (const plz of PLZS) {
    const first = await fetchPage(plz, 1)
    const totalPages = Math.ceil(first.total / PER_PAGE)
    const items = [...first.items]
    for (let page = 2; page <= totalPages; page++) {
        items.push(...(await fetchPage(plz, page)).items)
    }
    for (const it of items) byId.set(it.offer_id, it)
    console.log(`  ${plz}: ${items.length} items (${totalPages} pages), running total ${byId.size}`)
}

const outPath = path.join(OUT_DIR, "sozialplattform.json")
fs.writeFileSync(outPath, JSON.stringify([...byId.values()], null, 2))
console.log(`  ${byId.size} unique offers → ${outPath}`)
