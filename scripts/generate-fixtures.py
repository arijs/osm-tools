#!/usr/bin/env python3
"""Generate sample .osm.bz2 fixtures of several sizes for tests.

Committed / default fixtures (always generated):
  tiny    ~ 1 KB
  small   ~ 64 KB
  medium  ~ 1.3 MB

Large fixtures (gitignored; only with --large):
  large20   ~ 20 MB uncompressed OSM XML
  large200  ~ 200 MB uncompressed OSM XML

Run from repo root:
  python scripts/generate-fixtures.py           # default set
  python scripts/generate-fixtures.py --large   # also large20 + large200
  python scripts/generate-fixtures.py --only large20
"""

from __future__ import annotations

import argparse
import bz2
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "test" / "fixtures"

# Approximate bytes per node XML (with 2 tags) and per way (4 nd + 1 tag)
BYTES_PER_NODE = 155
BYTES_PER_WAY = 145


def osm_header() -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<osm version="0.6" generator="osm-tools-fixtures">\n'
        '  <bounds minlat="-23.6" minlon="-46.7" maxlat="-23.5" maxlon="-46.6"/>\n'
    )


def osm_footer() -> str:
    return "</osm>\n"


def node_xml(i: int) -> str:
    lat = -23.55 + (i % 1000) * 0.00001
    lon = -46.63 + (i % 1000) * 0.00001
    return (
        f'  <node id="{i}" version="1" lat="{lat:.6f}" lon="{lon:.6f}">\n'
        f'    <tag k="name" v="Node {i}"/>\n'
        f'    <tag k="source" v="fixture"/>\n'
        f"  </node>\n"
    )


def way_xml(i: int, node_ids: list[int]) -> str:
    nds = "".join(f'    <nd ref="{n}"/>\n' for n in node_ids)
    return (
        f'  <way id="{i}" version="1">\n'
        f"{nds}"
        f'    <tag k="highway" v="residential"/>\n'
        f"  </way>\n"
    )


def counts_for_target_bytes(target_bytes: int) -> tuple[int, int]:
    """Choose node/way counts so uncompressed size is near target_bytes."""
    # ways ≈ nodes / 8 (same ratio as medium fixture)
    # size ≈ header + nodes*BYTES_PER_NODE + ways*BYTES_PER_WAY
    overhead = 200
    # n * BYTES_PER_NODE + (n/8) * BYTES_PER_WAY ≈ target - overhead
    per_node = BYTES_PER_NODE + BYTES_PER_WAY / 8
    nodes = max(1, int((target_bytes - overhead) / per_node))
    ways = max(0, nodes // 8)
    return nodes, ways


def build_osm(node_count: int, way_count: int = 0) -> str:
    parts = [osm_header()]
    for i in range(1, node_count + 1):
        parts.append(node_xml(i))
    for w in range(1, way_count + 1):
        base = (w - 1) * 4 + 1
        refs = [base, base + 1, base + 2, base + 3]
        refs = [r if r <= node_count else ((r - 1) % node_count) + 1 for r in refs]
        parts.append(way_xml(100000 + w, refs))
    parts.append(osm_footer())
    return "".join(parts)


def write_bz2_from_string(name: str, xml: str, compresslevel: int = 9) -> tuple[Path, int, int]:
    OUT.mkdir(parents=True, exist_ok=True)
    raw_path = OUT / f"{name}.osm"
    bz2_path = OUT / f"{name}.osm.bz2"
    data = xml.encode("utf-8")
    raw_path.write_bytes(data)
    compressed = bz2.compress(data, compresslevel=compresslevel)
    bz2_path.write_bytes(compressed)
    print(
        f"{name:10s}  xml={len(data):10d} B  bz2={len(compressed):9d} B  "
        f"ratio={len(compressed)/len(data):.3f}  -> {bz2_path.relative_to(ROOT)}"
    )
    return bz2_path, len(data), len(compressed)


def write_bz2_streaming(
    name: str,
    node_count: int,
    way_count: int,
    compresslevel: int = 1,
    write_raw: bool = False,
) -> tuple[Path, int, int]:
    """Stream OSM XML into bz2 (and optionally a raw .osm) without building one giant string."""
    OUT.mkdir(parents=True, exist_ok=True)
    bz2_path = OUT / f"{name}.osm.bz2"
    raw_path = OUT / f"{name}.osm" if write_raw else None
    compressor = bz2.BZ2Compressor(compresslevel)
    raw_f = raw_path.open("wb") if raw_path else None
    total = 0

    def feed(s: str) -> None:
        nonlocal total
        b = s.encode("utf-8")
        total += len(b)
        if raw_f:
            raw_f.write(b)
        chunk = compressor.compress(b)
        if chunk:
            out_f.write(chunk)

    with bz2_path.open("wb") as out_f:
        feed(osm_header())
        for i in range(1, node_count + 1):
            feed(node_xml(i))
            if i % 100000 == 0:
                print(f"  … {name}: {i}/{node_count} nodes ({total/1e6:.1f} MB xml)", file=sys.stderr)
        for w in range(1, way_count + 1):
            base = (w - 1) * 4 + 1
            refs = [base, base + 1, base + 2, base + 3]
            refs = [r if r <= node_count else ((r - 1) % node_count) + 1 for r in refs]
            feed(way_xml(100000 + w, refs))
            if w % 50000 == 0:
                print(f"  … {name}: {w}/{way_count} ways ({total/1e6:.1f} MB xml)", file=sys.stderr)
        feed(osm_footer())
        out_f.write(compressor.flush())

    if raw_f:
        raw_f.close()

    bz2_size = bz2_path.stat().st_size
    print(
        f"{name:10s}  xml={total:10d} B  bz2={bz2_size:9d} B  "
        f"ratio={bz2_size/total:.3f}  -> {bz2_path.relative_to(ROOT)}"
        + (" (no raw .osm)" if not write_raw else "")
    )
    return bz2_path, total, bz2_size


# Spec: name -> (nodes, ways, compresslevel, large?, write_raw_osm?)
DEFAULT_SPECS = {
    "tiny": {"nodes": 3, "ways": 1, "level": 9, "large": False, "raw": True},
    "small": {"nodes": 400, "ways": 50, "level": 9, "large": False, "raw": True},
    "medium": {"nodes": 8000, "ways": 1000, "level": 1, "large": False, "raw": True},
}


def large_specs() -> dict:
    n20, w20 = counts_for_target_bytes(20 * 1024 * 1024)
    n200, w200 = counts_for_target_bytes(200 * 1024 * 1024)
    return {
        "large20": {
            "nodes": n20,
            "ways": w20,
            "level": 1,
            "large": True,
            "raw": False,  # avoid another 20MB on disk
            "target_mb": 20,
        },
        "large200": {
            "nodes": n200,
            "ways": w200,
            "level": 1,
            "large": True,
            "raw": False,
            "target_mb": 200,
        },
    }


def load_manifest() -> dict:
    meta = OUT / "manifest.json"
    if meta.exists():
        return json.loads(meta.read_text(encoding="utf-8"))
    return {}


def save_manifest(manifest: dict) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    meta = OUT / "manifest.json"
    meta.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"manifest -> {meta.relative_to(ROOT)}")


def generate_one(name: str, spec: dict, manifest: dict) -> None:
    nodes, ways, level = spec["nodes"], spec["ways"], spec["level"]
    if spec.get("large") or nodes > 20000:
        path, xml_bytes, bz2_bytes = write_bz2_streaming(
            name, nodes, ways, compresslevel=level, write_raw=spec.get("raw", False)
        )
    else:
        xml = build_osm(nodes, ways)
        path, xml_bytes, bz2_bytes = write_bz2_from_string(name, xml, compresslevel=level)

    manifest[name] = {
        "nodes": nodes,
        "ways": ways,
        "xmlBytes": xml_bytes,
        "bz2Bytes": bz2_bytes,
        "large": bool(spec.get("large")),
        "hasRawOsm": bool(spec.get("raw", True)),
    }
    if "target_mb" in spec:
        manifest[name]["targetMb"] = spec["target_mb"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate OSM .bz2 test fixtures")
    parser.add_argument(
        "--large",
        action="store_true",
        help="Also generate large20 (~20MB) and large200 (~200MB) gitignored fixtures",
    )
    parser.add_argument(
        "--only",
        metavar="NAME",
        help="Generate only this fixture (tiny|small|medium|large20|large200)",
    )
    args = parser.parse_args()

    all_specs = {**DEFAULT_SPECS, **large_specs()}
    manifest = load_manifest()

    if args.only:
        if args.only not in all_specs:
            print(f"Unknown fixture: {args.only}", file=sys.stderr)
            print(f"Known: {', '.join(all_specs)}", file=sys.stderr)
            sys.exit(1)
        generate_one(args.only, all_specs[args.only], manifest)
    else:
        for name, spec in DEFAULT_SPECS.items():
            generate_one(name, spec, manifest)
        if args.large:
            for name, spec in large_specs().items():
                generate_one(name, spec, manifest)

    save_manifest(manifest)


if __name__ == "__main__":
    main()
