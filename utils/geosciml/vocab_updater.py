# utils/geosciml/vocab_updater.py
"""
GeoSciML / EarthResourceML / ICS vocabulary downloader.

Ported from Adaptive_GKE/util/geosciml_vocab_updater.py.
Changes from original:
  - Removed __main__ block (not needed as library module)
  - No other changes — function logic is identical

Downloads 40+ geological vocabulary TTL files from official sources.
Skips files that already exist (safe to call repeatedly).
"""
import os
import time

import requests


def download_geosciml_vocabularies(output_dir: str = "./data/vocabularies") -> dict:
    """
    Download GeoSciML, EarthResourceML, and ICS geological time vocabularies
    in Turtle (TTL) format.

    Skips files that already exist in output_dir — safe to call on every startup.

    Args:
        output_dir: Directory to save TTL files.

    Returns:
        dict with keys: total, successful, skipped, failed,
                        successful_downloads, skipped_downloads, failed_downloads,
                        output_dir
    """
    os.makedirs(output_dir, exist_ok=True)

    successful_downloads: list = []
    failed_downloads: list = []
    skipped_downloads: list = []
    url_mapping: dict = {}

    # ── GeoSciML vocabularies ─────────────────────────────────────────────────
    # Note: http://resource.geosciml.org/classifier/cgi/ now redirects to a
    # Nuxt SPA (cgi.vocabs.ga.gov.au) that always returns HTML regardless of
    # Accept headers.  The Prez API backend is at cgi-api.vocabs.ga.gov.au and
    # returns proper Turtle (text/anot+turtle) when queried as an object.
    geosciml_prez_base = (
        "https://cgi-api.vocabs.ga.gov.au/object"
        "?uri=http://resource.geosciml.org/classifier/cgi/"
    )
    geosciml_vocab_mappings = {
        "Alteration Type": "alterationtype",
        "Borehole Drilling Method": "boreholedrillingmethod",
        "Composition Category": "compositioncategory",
        "Compound Material Constituent Part": "compoundmaterialconstituentpartrole",
        "Consolidation Degree": "consolidationdegree",
        "Contact Type": "contacttype",
        "Convention for Strike and Dip Measurements": "conventioncode",
        "Deformation Style": "deformationstyle",
        "Description Purpose": "descriptionpurpose",
        "Event Environment": "eventenvironment",
        "Event Process": "eventprocess",
        "Fault Movement Sense": "faultmovementsense",
        "Fault Movement Type": "faultmovementtype",
        "Fault Type": "faulttype",
        "Foliation Type": "foliationtype",
        "Genetic Category": "geneticcategory",
        "Geologic Unit Morphology": "geologicunitmorphology",
        "Geologic Unit Part Role": "geologicunitpartrole",
        "Geologic Unit Type": "geologicunittype",
        "Lineation Type": "lineationtype",
        "Mapping Frame": "mappingframe",
        "Metamorphic Facies": "metamorphicfacies",
        "Metamorphic Grade": "metamorphicgrade",
        "Observation Method (Geologic Feature)": "featureobservationmethod",
        "Observation Method (Mapped Feature)": "mappedfeatureobservationmethod",
        "Orientation Determination Method": "determinationmethodorientation",
        "Particle Aspect Ratio": "particleaspectratio",
        "Particle Shape": "particleshape",
        "Particle Type": "particletype",
        "Planar Polarity Code": "planarpolaritycode",
        "Proportion Term": "proportionterm",
        "Stratigraphic Rank": "stratigraphicrank",
        "Value Qualifier": "valuequalifier",
        "Vocabulary Relation": "vocabularyrelation",
    }
    for vocab_name, uri_name in geosciml_vocab_mappings.items():
        url_mapping[vocab_name] = geosciml_prez_base + uri_name

    # ── EarthResourceML vocabularies ──────────────────────────────────────────
    # Same Prez API base applies — EarthResourceML lives under the same CGI namespace.
    earthresourceml_prez_base = geosciml_prez_base
    earthresourceml_vocab_mappings = {
        "Commodity Code": "commodity-code",
        "Earth Resource Expression": "earth-resource-expression",
        "Earth Resource Form": "earth-resource-form",
        "Earth Resource Material Role": "earth-resource-material-role",
        "Earth Resource Shape": "earth-resource-shape",
        "End Use Potential": "end-use-potential",
        "Environmental Impact": "environmental-impact",
        "Exploration Activity Type": "exploration-activity-type",
        "Exploration Result": "exploration-result",
        "Mine Status": "mine-status",
        "Mineral Occurrence Type": "mineral-occurrence-type",
        "Mining Activity": "mining-activity",
        "Processing Activity": "mining-processing-activity",
        "Raw Material Role": "raw-material-role",
        "Reporting Classification Method": "classification-method-used",
        "Reserve Assessment Category": "reserve-assessment-category",
        "Resource Assessment Category": "resource-assessment-category",
        "UNFC Code": "unfc",
        "Waste Storage": "waste-storage",
    }
    for vocab_name, uri_name in earthresourceml_vocab_mappings.items():
        url_mapping[vocab_name] = earthresourceml_prez_base + uri_name

    # ── ICS geological time vocabulary ────────────────────────────────────────
    geologic_time_vocab_mappings = {
        "International Chronostratigraphic Chart - 2020": (
            "https://vocabs.ardc.edu.au/repository/api/lda/csiro/"
            "international-chronostratigraphic-chart/"
            "geologic-time-scale-2020/collection.ttl"
        )
    }
    for vocab_name, vocab_url in geologic_time_vocab_mappings.items():
        url_mapping[vocab_name] = vocab_url

    # ── Download ──────────────────────────────────────────────────────────────
    headers = {
        "Accept": "text/turtle",
        "User-Agent": "Mozilla/5.0 (compatible; GeoSciML-Downloader/1.0)",
    }
    total_vocabularies = len(url_mapping)

    for vocab_name, vocab_url in url_mapping.items():
        safe_filename = (
            vocab_name.replace(" ", "_")
            .replace("(", "")
            .replace(")", "")
            .replace("-", "_")
            .lower()
        )
        output_file = os.path.join(output_dir, f"{safe_filename}.ttl")

        if os.path.exists(output_file):
            skipped_downloads.append(vocab_name)
            continue

        try:
            response = requests.get(
                vocab_url, headers=headers, timeout=30, allow_redirects=True
            )
            # Accept both text/turtle and text/anot+turtle (Prez annotated Turtle)
            ct = response.headers.get("Content-Type", "")
            is_turtle_ct = "turtle" in ct
            if response.status_code == 200:
                content = response.text.strip()
                if (
                    is_turtle_ct
                    or content.startswith("@prefix")
                    or content.startswith("@base")
                    or "@prefix" in content[:2000]
                    or "rdf:type" in content[:2000]
                ):
                    with open(output_file, "w", encoding="utf-8") as f:
                        f.write(content)
                    successful_downloads.append(vocab_name)
                else:
                    failed_downloads.append((vocab_name, "Invalid TTL format"))
            else:
                error_msg = f"HTTP {response.status_code}"
                failed_downloads.append((vocab_name, error_msg))

            time.sleep(0.5)  # Be respectful to servers

        except Exception as exc:
            failed_downloads.append((vocab_name, str(exc)))

    return {
        "total": total_vocabularies,
        "successful": len(successful_downloads),
        "skipped": len(skipped_downloads),
        "failed": len(failed_downloads),
        "successful_downloads": successful_downloads,
        "skipped_downloads": skipped_downloads,
        "failed_downloads": failed_downloads,
        "output_dir": os.path.abspath(output_dir),
    }
