import { sparqlSelect, storeFromTurtles } from "@foerderfunke/sem-ops-utils"
import { PATHS, sourceName, stepJournal } from "../utils.js"
import { spawnSync } from "child_process"
import path from "path"
import fs from "fs"

const ROOT = path.join(import.meta.dirname, "..")
const JAR = path.join(ROOT, "tools/sparql-anything.jar")
const abs = (p) => path.join(ROOT, p)
const NS = "https://civic-data.de/pipeline#"
const defStore = storeFromTurtles(["config/federation.ttl"].map(p => fs.readFileSync(abs(p), "utf8")))

const run = (cmd, args) => {
    const r = spawnSync(cmd, args, { stdio: "inherit" })
    if (r.status !== 0) throw new Error(`Exit ${r.status}: ${cmd} ${args.join(" ")}`)
}

// ---- Read the sources ----------------------------------------------------
// The step graph (fetch → lift per source) is the engine's own shape; config
// declares only the sources and their facts. Lift params are SPARQL Anything
// variables declared per source.

const sources = new Map()
for (const r of await sparqlSelect(`
    PREFIX : <${NS}>
    SELECT ?source ?fetchUrl ?format ?paramName ?paramValue WHERE {
        :federation :hasSource ?source .
        OPTIONAL { ?source :fetchUrl ?fetchUrl }
        OPTIONAL { ?source :format   ?format   }
        OPTIONAL { ?source :hasLiftParam [ :name ?paramName ; :value ?paramValue ] }
    } ORDER BY ?source`, [defStore])) {
    if (!sources.has(r.source)) sources.set(r.source, { fetchUrl: r.fetchUrl, format: r.format, params: [] })
    if (r.paramName) sources.get(r.source).params.push([r.paramName, r.paramValue])
}
for (const [iri, s] of sources) {
    if (!s.format) throw new Error(`${iri} declares no :format (needed to pick the lift query)`)
}

// ---- Ensure sparql-anything.jar ----------------------------------------

const SPARQL_ANYTHING_VERSION = "v1.1.0"
const VERSION_FILE = path.join(ROOT, "tools/sparql-anything.version")
const haveCurrentJar = fs.existsSync(JAR) && fs.existsSync(VERSION_FILE)
    && fs.readFileSync(VERSION_FILE, "utf8").trim() === SPARQL_ANYTHING_VERSION

if (!haveCurrentJar) {
    const url = `https://github.com/SPARQL-Anything/sparql.anything/releases/download/${SPARQL_ANYTHING_VERSION}/sparql-anything-${SPARQL_ANYTHING_VERSION}.jar`
    console.log(`Downloading sparql-anything ${SPARQL_ANYTHING_VERSION}...`)
    fs.mkdirSync(path.dirname(JAR), { recursive: true })
    const response = await fetch(url)
    if (!response.ok) throw new Error(`Failed to fetch ${url}: ${response.status}`)
    fs.writeFileSync(JAR, Buffer.from(await response.arrayBuffer()))
    fs.writeFileSync(VERSION_FILE, SPARQL_ANYTHING_VERSION)
    console.log(`Saved to ${JAR}`)
}

// ---- Run steps ----------------------------------------------------------

const PLZS = (await sparqlSelect(`
    PREFIX : <${NS}>
    SELECT ?plz WHERE { :federation :hasRunParam [ :name "plz" ; :value ?plz ] } ORDER BY ?plz`, [defStore])).map(r => r.plz)

const runStart = new Date()
const harvests = []
const journal = stepJournal()
const fetchStepOf = new Map()

for (const [iri, s] of sources) {
    const name = sourceName(iri)
    fetchStepOf.set(iri, await journal.step("fetch", { source: iri }, () => {
        const outDir = PATHS.raw(name)
        // Live sources pass their :fetchUrl; static-file sources pass the
        // absolute static dir instead. The script gets whichever applies.
        const origin = s.fetchUrl ?? abs(PATHS.staticDir(name))
        console.log(`fetch  ${s.fetchUrl ?? PATHS.staticDir(name)} (PLZs ${PLZS.join(", ")}) → ${outDir}`)
        fs.mkdirSync(abs(outDir), { recursive: true })
        run("node", [abs(PATHS.fetchScript(name)), abs(outDir), origin, PLZS.join(",")])
        harvests.push({ source: iri, time: new Date().toISOString() })
    }))
}

for (const [iri, s] of sources) {
    const name = sourceName(iri)
    await journal.step("lift", { source: iri, after: [fetchStepOf.get(iri)] }, () => {
        // TODO: directory mode spawns one JVM per file (~1s startup each).
        // Fine at small N; revisit if a source crosses ~50 items. SPARQL Anything
        // accepts VALUES ?_location { … } in the lift query, which would let one
        // invocation handle the whole batch.
        const liftQuery = PATHS.liftQuery(s.format)
        const liftOne = (location, outPath) => {
            const args = ["-jar", JAR, "-q", abs(liftQuery),
                          "-v", `location=${location}`,
                          "-f", "TTL", "-o", outPath]
            for (const [pName, value] of s.params) args.push("-v", `${pName}=${value}`)
            run("java", args)
        }
        const inAbs = abs(PATHS.raw(name))
        const outAbs = abs(PATHS.lifted(name))
        const files = fs.readdirSync(inAbs).filter(f => !f.startsWith(".")).sort()
        fs.mkdirSync(outAbs, { recursive: true })
        console.log(`lift   ${PATHS.raw(name)} (${files.length} files) → ${PATHS.lifted(name)}`)
        for (const f of files) {
            const stem = path.basename(f, path.extname(f))
            liftOne(path.join(inAbs, f), path.join(outAbs, `${stem}.ttl`))
        }
    })
}

const dt = (s) => `"${s}"^^xsd:dateTime`
const runId = "run" + runStart.toISOString().replace(/\D/g, "").slice(0, 14)
const harvestPart = harvests.length
    ? ` ;\n    :harvested\n` + harvests.map((h) => {
        const local = h.source.split("#").pop()
        return `        [ :ofSource :${local} ; prov:atTime ${dt(h.time)} ]`
    }).join(" ,\n")
    : ""

const block = `
${journal.toTurtle()}

:${runId} a :IngestRun ;
    prov:startedAtTime ${dt(runStart.toISOString())} ;
    prov:endedAtTime   ${dt(new Date().toISOString())}${harvestPart} .
`

const prefixes = `@prefix :      <${NS}> .
@prefix p-plan: <http://purl.org/net/p-plan#> .
@prefix prov:   <http://www.w3.org/ns/prov#> .
@prefix xsd:    <http://www.w3.org/2001/XMLSchema#> .
`
fs.mkdirSync(path.dirname(abs(PATHS.ingestLog)), { recursive: true })
fs.writeFileSync(abs(PATHS.ingestLog), prefixes + block)
console.log(`log:   wrote steps + IngestRun → ${PATHS.ingestLog}`)
