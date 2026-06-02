// Helper for the Pipeline view: turn pipeline.ttl into a step graph.
// Reads:  the pipeline + federation TTL strings passed by Pipeline.jsx
// Does:   returns { nodes, edges } — Source lane-header nodes (transparent
//         fill, light-gray border) above each Fetch step, step nodes labelled
//         by their type (fetch/lift/clean/map/match/merge/resolve), and an
//         End sink so resolve's output is shown on a visible edge, plus a
//         boundary node for any visible step's :input file (e.g. the Match
//         step's match-knowledge.ttl). Load is filtered out and its edges are
//         forwarded past it. Edge labels come
//         straight from the TTL: a step's :format (uppercased) or its :output/
//         :provOutput basename(s), and :retrieval on the source→fetch edge.
//         Multiple outputs (e.g. merge's :provOutput) stack as newline lines.

import { formatFamily, localName, parseTtl } from "../../utils.js"

const NS = "https://civic-data.de/pipeline#"
const PPLAN_IS_PRECEDED_BY = "http://purl.org/net/p-plan#isPrecededBy"
const RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
const RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
const FROM_SOURCE = `${NS}fromSource`
const RETRIEVAL = `${NS}retrieval`
const FORMAT = `${NS}format`
const OUTPUT = `${NS}output`
const INPUT = `${NS}input`
const PROV_OUTPUT = `${NS}provOutput`
const STEP_TYPES = ["Fetch", "Lift", "Clean", "Load", "Map", "Match", "Merge", "Resolve"]
const HIDDEN_STEPS = new Set(["Load"])
const LANE_BORDER = "#bbb"

const basename = (path) => path.replace(/^.*\//, "")

export function loadPipeline(pipelineTtl, federationTtl) {
    const quads = parseTtl(pipelineTtl)
    const fedQuads = federationTtl ? parseTtl(federationTtl) : []

    const stepType = new Map()
    const rawEdges = []
    const inputQuads = []
    const sourceOfStep = new Map()
    const formatBySubject = new Map()
    const outputOfStep = new Map()
    const provOutputOfStep = new Map()
    const retrievalOfStep = new Map()
    for (const q of quads) {
        const p = q.predicate.value
        if (p === RDF_TYPE && q.object.value.startsWith(NS)) {
            const local = q.object.value.slice(NS.length)
            if (STEP_TYPES.includes(local)) stepType.set(q.subject.value, local)
        } else if (p === PPLAN_IS_PRECEDED_BY) rawEdges.push({ from: q.object.value, to: q.subject.value })
        else if (p === FROM_SOURCE)    sourceOfStep.set(q.subject.value, q.object.value)
        else if (p === RETRIEVAL)      retrievalOfStep.set(q.subject.value, q.object.value)
        else if (p === FORMAT)         formatBySubject.set(q.subject.value, q.object.value)
        else if (p === OUTPUT)         outputOfStep.set(q.subject.value, q.object.value)
        else if (p === PROV_OUTPUT)    provOutputOfStep.set(q.subject.value, q.object.value)
        else if (p === INPUT)          inputQuads.push([q.subject.value, q.object.value])
    }

    // Forward edges past hidden (Load) steps so clean→load→map collapses to clean→map.
    const hidden = new Set([...stepType].filter(([, t]) => HIDDEN_STEPS.has(t)).map(([iri]) => iri))
    const incoming = new Map()
    for (const e of rawEdges) {
        if (!incoming.has(e.to)) incoming.set(e.to, [])
        incoming.get(e.to).push(e.from)
    }
    const resolvePreds = (iri) => (incoming.get(iri) ?? []).flatMap((p) => hidden.has(p) ? resolvePreds(p) : [p])

    const fileLabel = (fromIri) => {
        const outs = [outputOfStep.get(fromIri), provOutputOfStep.get(fromIri)].filter(Boolean).map(basename)
        return outs.length ? outs.join("\n") : null
    }
    // A step's own :format, else its type's class-level :format (e.g.
    // :Lift :format "rdf" labels every lift edge without repeating it per step).
    const formatOf = (iri) => formatBySubject.get(iri) ?? formatBySubject.get(`${NS}${stepType.get(iri)}`)
    // Edge label = the format the step emits (its file-type IRI's short label),
    // else its output file(s); both come straight from the TTL, nothing hardcoded.
    const edgeLabel = (fromIri) => {
        const fmt = formatOf(fromIri)
        return fmt ? formatFamily(fmt) : fileLabel(fromIri)
    }

    const stepEdges = []
    for (const iri of stepType.keys()) {
        if (hidden.has(iri)) continue
        for (const from of resolvePreds(iri)) {
            stepEdges.push({ from, to: iri, value: edgeLabel(from) ?? undefined, centered: true })
        }
    }

    const sourceLabel = new Map()
    for (const q of fedQuads) {
        if (q.predicate.value === RDFS_LABEL) sourceLabel.set(q.subject.value, q.object.value)
    }

    const stepNodes = [...stepType]
        .filter(([iri]) => !hidden.has(iri))
        .map(([iri, type]) => ({ id: iri, label: type.toLowerCase(), type }))

    const laneNodes = []
    const laneEdges = []
    for (const [iri, type] of stepType) {
        if (type !== "Fetch") continue
        const sourceIri = sourceOfStep.get(iri)
        if (!sourceIri) continue
        const laneId = `lane:${sourceIri}`
        laneNodes.push({
            id: laneId,
            label: sourceLabel.get(sourceIri) ?? localName(sourceIri),
            type: "Source",
            color: "transparent",
            borderColor: LANE_BORDER,
        })
        laneEdges.push({ from: laneId, to: iri, value: retrievalOfStep.get(iri), centered: true })
    }

    // End sink so resolve's output (final.ttl) is shown on a visible edge.
    const resolveIri = [...stepType].find(([, t]) => t === "Resolve")?.[0]
    const endNodes = []
    const endEdges = []
    if (resolveIri) {
        endNodes.push({ id: "end", label: "end", type: "End", color: "transparent", borderColor: LANE_BORDER })
        endEdges.push({ from: resolveIri, to: "end", value: edgeLabel(resolveIri) ?? undefined, centered: true })
    }

    // Side inputs: a visible step may consume an :input file besides its upstream
    // step (the Match step reads config/match-knowledge.ttl). Each becomes a
    // boundary node feeding that step, labelled with the file basename. Hidden
    // (Load) steps' :input is the forwarded main flow, so it is skipped.
    const inputNodes = []
    const inputEdges = []
    for (const [iri, path] of inputQuads) {
        if (hidden.has(iri)) continue
        const inId = `input:${path}`
        inputNodes.push({ id: inId, label: "input", type: "Input", color: "transparent", borderColor: LANE_BORDER })
        inputEdges.push({ from: inId, to: iri, value: basename(path), centered: true, sideInput: true })
    }

    return {
        nodes: [...laneNodes, ...inputNodes, ...stepNodes, ...endNodes],
        edges: [...laneEdges, ...inputEdges, ...stepEdges, ...endEdges],
    }
}
