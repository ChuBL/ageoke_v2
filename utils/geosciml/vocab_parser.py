# utils/geosciml/vocab_parser.py
"""
GeoSciML / ICS TTL vocabulary parser.

Ported from Adaptive_GKE/util/geosciml_vocab_parser.py.
Changes from original:
  - Removed sys.path.insert hack (lines 5–8 of original)
  - LLM initialized via utils.settings instead of os.getenv() calls
  - load_dotenv() removed (settings handles .env loading)

Core parsing logic (extract_ttl_members) is identical to the original.
"""
import asyncio
import glob
import os
import re
from typing import Dict, List, Optional


def extract_ttl_members(file_path: str) -> Optional[Dict[str, List[str]]]:
    """
    Parse a TTL file and extract member lists.

    First attempts GeoSciML format, then falls back to ICS geological
    timescale format. Dual-fallback logic is preserved from the original.

    Args:
        file_path: Path to the TTL file.

    Returns:
        Dict mapping collection_uri → list[member_names], or None if both
        format parsers fail.
    """
    if not os.path.exists(file_path):
        return None

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
    except Exception:
        return None

    # ── First attempt: GeoSciML format (full URI angle-bracket notation) ──────
    # Handles old-style TTL: <http://resource.geosciml.org/classifier/cgi/X/member>
    try:
        collection_pattern = r"<(http://resource\.geosciml\.org/classifier/cgi/[^/>]+)>"
        collection_match = re.search(
            collection_pattern, content, re.MULTILINE | re.DOTALL
        )

        if collection_match:
            base_uri = collection_match.group(1).rstrip("/")
            member_pattern = rf"<{re.escape(base_uri)}/([^>]+)>"
            members_matches = re.findall(member_pattern, content, re.MULTILINE)
            members = sorted(
                list(set(m.strip() for m in members_matches if m.strip()))
            )
            if members:
                return {base_uri: members}
    except Exception:
        pass

    # ── Second attempt: GeoSciML prefix notation (Prez anot+turtle format) ───
    # Handles new-style TTL returned by cgi-api.vocabs.ga.gov.au where members
    # are written as `vocabprefix:localname` rather than full angle-bracket URIs.
    try:
        # Build prefix → namespace map from @prefix declarations
        prefix_map: Dict[str, str] = {}
        for m in re.finditer(
            r"@prefix\s+(\w+):\s+<([^>]+)>\s*\.", content
        ):
            prefix_map[m.group(1)] = m.group(2)

        # Find the vocab-specific prefix: namespace ends with /classifier/cgi/<vocab>/
        cgi_ns_pattern = re.compile(
            r"http://resource\.geosciml\.org/classifier/cgi/[^/]+/$"
        )
        for prefix, namespace in prefix_map.items():
            if cgi_ns_pattern.match(namespace):
                base_uri = namespace.rstrip("/")
                # Extract members from the skos:member block(s) for this prefix
                # Members appear as `prefix:localname` separated by commas/semicolons
                member_entry_pattern = re.compile(
                    rf"\b{re.escape(prefix)}:([A-Za-z0-9_]+)"
                )
                # Restrict to content after skos:member to avoid false matches
                # (e.g. the @prefix line itself won't match since it has no localname)
                after_member_kw = re.sub(
                    r"^.*?skos:member\b", "", content, count=1, flags=re.DOTALL
                )
                members = sorted(
                    set(
                        m.group(1)
                        for m in member_entry_pattern.finditer(after_member_kw)
                    )
                )
                if members:
                    return {base_uri: members}
    except Exception:
        pass

    # ── Second attempt: ICS geological timescale format ───────────────────────
    try:
        collections: Dict[str, List[str]] = {}

        collection_pattern = (
            r"<(http://resource\.geosciml\.org/classifier/ics/ischart/([^/>]+))>"
            r"\s*rdfs:label"
        )
        collection_matches = re.findall(collection_pattern, content)

        for full_uri, collection_name in collection_matches:
            subject_pattern = re.escape(f"<{full_uri}>") + r"\s*rdfs:label"
            subject_match = re.search(subject_pattern, content)
            if not subject_match:
                continue

            start_pos = subject_match.start()
            remaining = content[start_pos + 1 :]
            next_coll_pattern = (
                r"<http://resource\.geosciml\.org/classifier/ics/ischart/[^/>]+>"
                r"\s*rdfs:label"
            )
            next_match = re.search(next_coll_pattern, remaining)

            if next_match:
                block = content[start_pos : start_pos + 1 + next_match.start()]
            else:
                block = content[start_pos:]

            if "skos:member" not in block:
                continue

            member_lines: List[str] = []
            in_member_section = False
            for line in block.split("\n"):
                if "skos:member" in line:
                    in_member_section = True
                    member_lines.append(line)
                elif in_member_section:
                    if (
                        "http://resource.geosciml.org/classifier/ics/ischart/" in line
                        and "rdfs:label" not in line
                        and "skos:prefLabel" not in line
                    ):
                        member_lines.append(line)
                    elif line.strip().endswith(";") or line.strip().endswith("."):
                        member_lines.append(line)
                        break

            members: List[str] = []
            for line in member_lines:
                uri_matches = re.findall(
                    r"http://resource\.geosciml\.org/classifier/ics/ischart/([^/>]+)",
                    line,
                )
                for member_name in uri_matches:
                    if member_name != collection_name:
                        members.append(member_name)

            unique_members = sorted(list(set(members)))
            if unique_members:
                collections[full_uri] = unique_members

        if collections:
            return collections

    except Exception:
        pass

    return None


# ── LLM-based description generation ─────────────────────────────────────────

_DESCRIPTION_SYSTEM_PROMPT = """\
You are a geological terminology expert tasked with creating concise descriptions \
for GeosciML vocabulary files.

Your task: Generate a single sentence description for a TTL vocabulary file based \
on its URI and member terms.

Requirements:
1. Write in English
2. Use professional, straightforward language
3. Focus on describing the vocabulary's SCOPE/DOMAIN rather than listing specific members
4. Avoid words like "covering" or "including" that might imply other terms are excluded
5. Do not list multiple specific members (reference members will be provided separately)
6. Use URI domain information to understand the vocabulary's conceptual area

Sentence structure examples:
- "This vocabulary defines terms related to [domain/scope]"
- "This vocabulary contains terminology for [conceptual area]"
- "This vocabulary provides standard terms for [field/classification]"

Output: One clear, professional sentence describing what geological concepts this \
vocabulary addresses.\
"""


async def _generate_description(llm, uri: str, members: List[str]) -> str:
    """
    Generate an LLM description for a single TTL vocabulary entry.
    Retries up to 3 times.
    """
    sample = members[:10]
    members_text = ", ".join(f"'{m}'" for m in sample)
    if len(members) > 10:
        members_text += f" ... (and {len(members) - 10} more)"

    human_prompt = f"URI: {uri}\nMembers: [{members_text}]\n\nGenerate description:"

    for attempt in range(3):
        try:
            response = await llm.ainvoke(
                [("system", _DESCRIPTION_SYSTEM_PROMPT), ("human", human_prompt)]
            )
            description = str(response.content).strip()
            if description.startswith('"') and description.endswith('"'):
                description = description[1:-1]
            return description
        except Exception as exc:
            if attempt == 2:
                raise exc

    raise RuntimeError("Failed to generate description after 3 attempts")


async def _process_ttl_directory(
    directory_path: str, output_md_path: str
) -> Optional[str]:
    """
    Process all TTL files in directory_path and write a markdown descriptions file.
    Skips generation if output already exists.
    """
    if os.path.exists(output_md_path):
        return output_md_path

    from langchain_openai import AzureChatOpenAI
    from utils.config import settings

    llm = AzureChatOpenAI(
        deployment_name=settings.deployment_name,
        api_version=settings.api_version,
        azure_endpoint=settings.azure_endpoint,
        api_key=settings.api_key,
        temperature=settings.geosciml_temperature,
    )

    ttl_files = glob.glob(os.path.join(directory_path, "*.ttl"))
    if not ttl_files:
        return None

    results = []
    for ttl_file in ttl_files:
        filename = os.path.basename(ttl_file)
        try:
            parsed = extract_ttl_members(ttl_file)
            if not parsed:
                continue

            for uri, members in parsed.items():
                if not members:
                    continue
                try:
                    description = await _generate_description(llm, uri, members)
                    results.append(
                        {
                            "filename": filename,
                            "uri": uri,
                            "members": members[:5],
                            "description": description,
                        }
                    )
                except Exception:
                    results.append(
                        {
                            "filename": filename,
                            "uri": uri,
                            "members": members[:5],
                            "description": "Error: Could not generate description",
                        }
                    )
                break  # One collection per file
        except Exception:
            pass

    # Write markdown file
    os.makedirs(os.path.dirname(output_md_path) or ".", exist_ok=True)
    with open(output_md_path, "w", encoding="utf-8") as f:
        for result in results:
            f.write(f"## {result['filename']}\n\n")
            f.write(f"**Description:** {result['description']}\n\n")
            f.write("---\n\n")

    return output_md_path


async def generate_vocab_descriptions(
    VOCAB_PATH: str = "./data/vocabularies",
    OUTPUT_PATH: str = "./data/vocabularies/_geosciml_descriptions.md",
) -> Optional[str]:
    """
    Generate LLM-based descriptions for all TTL vocabulary files.

    Skips generation if OUTPUT_PATH already exists.

    Args:
        VOCAB_PATH:  Directory containing .ttl files.
        OUTPUT_PATH: Path for the output markdown descriptions file.

    Returns:
        Path to the descriptions file, or None on failure.
    """
    if os.path.exists(OUTPUT_PATH):
        return OUTPUT_PATH

    return await _process_ttl_directory(
        directory_path=VOCAB_PATH, output_md_path=OUTPUT_PATH
    )
