// All config + pipeline data the webapp reads, fetched at runtime relative to
// BASE_URL — in dev the vite middleware serves config/ and data/ from the
// repo root; on gh-pages the deploy publishes them next to the bundle.
// federation.ttl is the only fixed path: the rest of the manifest derives
// from it (match-knowledge from :matchKnowledgeGraph, cleaned files from
// :hasSource) and the PATHS conventions. A missing artifact resolves to ""
// (pages render empty). Top-level await — importing modules stay synchronous.

import { parseTtl, PATHS, sourceName } from "../../utils.js"

const NS = "https://civic-data.de/pipeline#"

const fetchText = async (path) => {
    const res = await fetch(`${import.meta.env.BASE_URL}${path}`).catch(() => null)
    return res?.ok ? res.text() : ""
}

export const federationTtl = await fetchText("config/federation.ttl")

const objectsOf = (quads, pred) => [...new Set(quads.filter((q) => q.predicate.value === `${NS}${pred}`).map((q) => q.object.value))]
const fedQuads = parseTtl(federationTtl)
const knowledgePaths = objectsOf(fedQuads, "matchKnowledgeGraph")
const cleanedPaths = objectsOf(fedQuads, "hasSource").map((iri) => PATHS.cleaned(sourceName(iri)))

const FIXED = [PATHS.ingestLog, PATHS.federateLog, PATHS.mapped, PATHS.matches, PATHS.merged, PATHS.provenance, PATHS.final]
const [fixedTexts, knowledgeTexts, cleanedTexts] = await Promise.all([
    Promise.all(FIXED.map(fetchText)),
    Promise.all(knowledgePaths.map(fetchText)),
    Promise.all(cleanedPaths.map(fetchText)),
])

export const [ingestLogTtl, federateLogTtl, mappedTtl, matchesTtl, mergedTtl, provenanceTtl, finalTtl] = fixedTexts
export const matchKnowledgeTtl = knowledgeTexts.join("\n")
export const cleanedByPath = Object.fromEntries(cleanedPaths.map((p, i) => [p, cleanedTexts[i]]))
