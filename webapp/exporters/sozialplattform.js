// Exporter: maps the federated directory into Sozialplattform's JSON shape via
// a SPARQL query. A bespoke output adapter (target-specific by nature).
// Declared in federation.ttl (:hasExporter "sozialplattform") and loaded by the
// webapp's Download page at runtime, like config/ and data/. Runtime modules
// can't resolve bare imports, so build() receives the helpers as a toolkit.

export const label    = "Sozialplattform JSON"
export const filename = "sozialplattform.json"
export const mime     = "application/json"

const QUERY = `
PREFIX schema: <http://schema.org/>
PREFIX dct:    <http://purl.org/dc/terms/>

SELECT ?org
       (SAMPLE(?n)   AS ?name)
       (SAMPLE(?s)   AS ?street)
       (SAMPLE(?pc)  AS ?postalCode)
       (SAMPLE(?l)   AS ?locality)
       (SAMPLE(?co)  AS ?country)
       (SAMPLE(?cat) AS ?category)
       (SAMPLE(?ph)  AS ?phone)
       (SAMPLE(?em)  AS ?email)
WHERE {
    ?org schema:name ?n .
    OPTIONAL { ?org schema:streetAddress   ?s   }
    OPTIONAL { ?org schema:postalCode      ?pc  }
    OPTIONAL { ?org schema:addressLocality ?l   }
    OPTIONAL { ?org schema:addressCountry  ?co  }
    OPTIONAL { ?org dct:subject            ?cat }
    OPTIONAL { ?org schema:telephone       ?ph  }
    OPTIONAL { ?org schema:email           ?em  }
}
GROUP BY ?org
ORDER BY ?org`

export async function build(finalTtl, { sparqlSelect, storeFromTurtles, localName }) {
    const rows = await sparqlSelect(QUERY, [storeFromTurtles([finalTtl])])
    return JSON.stringify(rows.map((r) => ({
        offer_id:                              localName(r.org),
        offer_nid:                             "",
        offer_title:                           "",
        offer_service_area:                    "",
        search_score:                          null,
        offer_external_service_type:           "",
        offer_external_id_booking:             "",
        offer_external_service_booking:        "",
        offer_external_service_consulting:     "",
        offer_external_service_link_url:       "",
        offer_external_service_link_text:      "",
        office_address_short:                  [r.country, r.locality, r.postalCode, r.street].filter(Boolean).join(", "),
        office_address: {
            langcode:                          null,
            country_code:                      r.country    ?? null,
            locality:                          r.locality   ?? null,
            postal_code:                       r.postalCode ?? null,
            address_line1:                     r.street     ?? null,
            address_line3:                     null,
        },
        office_title:                          r.name  ?? "",
        office_phone:                          r.phone ?? "",
        office_email:                          r.email ?? "",
        office_opening_hours:                  "",
        office_opening_hours_notes:            "",
        office_external_service_type:          "",
        office_external_id_booking:            "",
        office_external_service_booking:       "",
        office_external_id_consulting:         "",
        office_external_service_consulting:    "",
        offer_type:                            r.category ?? "",
        offer_chat_topic:                      "",
    })), null, 2)
}
