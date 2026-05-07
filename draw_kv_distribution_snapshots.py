#!/usr/bin/env python3

from __future__ import annotations

import argparse
import collections
import pathlib
import re


KV_WRITE_RE = re.compile(
    r"^(?P<ts>\d+\.\d+).*KV 写入成功: key=(?P<key>[^,]+), version=(?P<version>\d+), bucket=#(?P<bucket>\d+)"
)

NODE_COLORS = {
    "/one": "#f28482",
    "/two": "#84a59d",
    "/three": "#8ecae6",
    "/four": "#f6bd60",
    "/five": "#b8c0ff",
    "/six": "#cdb4db",
}

NODE_ORDER = {
    "/one": 1,
    "/two": 2,
    "/three": 3,
    "/four": 4,
    "/five": 5,
    "/six": 6,
}


def esc(value: str) -> str:
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def text(x: float, y: float, value: str, size: int = 24, weight: str = "normal",
         anchor: str = "middle", fill: str = "#111") -> str:
    return (
        f'<text x="{x}" y="{y}" font-family="DejaVu Sans, Noto Sans SC, sans-serif" '
        f'font-size="{size}" font-weight="{weight}" text-anchor="{anchor}" fill="{fill}">{esc(value)}</text>'
    )


def rect(x: float, y: float, w: float, h: float, fill: str = "#fff",
         stroke: str = "#333", rx: int = 14) -> str:
    return (
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" fill="{fill}" '
        f'stroke="{stroke}" stroke-width="2" rx="{rx}" />'
    )


def pill(x: float, y: float, w: float, h: float, label: str, fill: str = "#fff") -> str:
    parts = [rect(x, y, w, h, fill=fill, stroke="#666", rx=18)]
    parts.append(text(x + w / 2, y + h / 2 + 7, label, size=20, weight="bold"))
    return "\n".join(parts)


def parse_markers(path: pathlib.Path):
    markers = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        label, ts_text = line.split("\t", 1)
        markers.append((label, float(ts_text)))
    return markers


def parse_kv_events(path: pathlib.Path):
    events = []
    if not path.exists():
        return events
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        ts_text, label, key, value = line.split("\t", 3)
        events.append({
            "ts": float(ts_text),
            "label": label,
            "key": key,
            "value": value,
        })
    return events


def parse_write_logs(log_dir: pathlib.Path):
    writes = []
    nodes = []
    for path in sorted(log_dir.glob("*.log")):
        node = f"/{path.stem}"
        nodes.append(node)
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            match = KV_WRITE_RE.match(line)
            if not match:
                continue
            writes.append({
                "ts": float(match.group("ts")),
                "node": node,
                "key": match.group("key"),
                "version": int(match.group("version")),
                "bucket": int(match.group("bucket")),
            })
    writes.sort(key=lambda item: (item["ts"], item["node"], item["key"], item["version"]))
    return writes, sorted(set(nodes), key=lambda item: (NODE_ORDER.get(item, 999), item))


def resolve_values(write_events, writes):
    value_map = {}
    for event in write_events:
        candidates = [
            item for item in writes
            if item["key"] == event["key"] and event["ts"] - 0.5 <= item["ts"] <= event["ts"] + 6.0
        ]
        if not candidates:
            continue
        version = min(candidates, key=lambda item: item["ts"])["version"]
        value_map[(event["key"], version)] = event["value"]
    return value_map


def state_at(writes, value_map, ts):
    per_node = collections.defaultdict(dict)
    for item in writes:
        if item["ts"] > ts:
            break
        key = item["key"]
        version = item["version"]
        per_node[item["node"]][key] = {
            "version": version,
            "bucket": item["bucket"],
            "value": value_map.get((key, version), "?"),
        }
    return per_node


def latest_versions(per_node_state):
    best = {}
    for node_state in per_node_state.values():
        for key, item in node_state.items():
            current = best.get(key)
            if current is None or item["version"] > current["version"]:
                best[key] = item
    return best


def render_svg(title, markers, nodes, writes, value_map, output, base_ts):
    panel_count = len(markers)
    width = 1720
    panel_height = 355
    height = 180 + panel_count * panel_height
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#faf8f2" />',
        text(width / 2, 60, title, size=42, weight="bold"),
    ]

    for index, (label, ts) in enumerate(markers):
        top = 150 + index * panel_height
        parts.append(rect(40, top, width - 80, 308, fill="#fffdf8", stroke="#c7bda8", rx=18))
        parts.append(text(90, top + 44, label, size=30, weight="bold", anchor="start"))
        parts.append(text(width - 90, top + 44, f"t={ts - base_ts:.1f}s", size=24, anchor="end", fill="#666"))

        per_node = state_at(writes, value_map, ts)
        best = latest_versions(per_node)

        summary_x = 78
        summary_y = top + 82
        parts.append(text(summary_x, summary_y, "全局最新值", size=22, weight="bold", anchor="start", fill="#6b3f00"))
        chip_x = 220
        for key in sorted(best):
            item = best[key]
            label_text = f"{key}={item['value']} (b{item['bucket']}, v{str(item['version'])[-4:]})"
            parts.append(pill(chip_x, summary_y - 24, 250, 40, label_text, fill="#fff1c7"))
            chip_x += 270

        card_y = top + 116
        card_w = 300
        card_h = 160
        gap = 18
        item_gap = 30
        for card_index, node in enumerate(nodes):
            x = 78 + card_index * (card_w + gap)
            fill = NODE_COLORS.get(node, "#e6e6e6")
            parts.append(rect(x, card_y, card_w, card_h, fill=fill, stroke="#666", rx=20))
            parts.append(text(x + 24, card_y + 38, node.lstrip("/"), size=30, weight="bold", anchor="start"))

            node_items = per_node.get(node, {})
            if not node_items:
                parts.append(text(x + 24, card_y + 92, "无本地键值副本", size=20, anchor="start", fill="#4f4f4f"))
                continue

            item_y = card_y + 74
            for key in sorted(node_items):
                item = node_items[key]
                is_latest = best.get(key, {}).get("version") == item["version"]
                prefix = "latest" if is_latest else "stale"
                fill_color = "#fff1c7" if is_latest else "#f8d7da"
                item_label = f"{prefix} {key}={item['value']}  b{item['bucket']}  v{str(item['version'])[-4:]}"
                parts.append(pill(x + 18, item_y - 22, card_w - 36, 34, item_label, fill=fill_color))
                item_y += item_gap

    parts.append("</svg>")
    output.write_text("\n".join(parts), encoding="utf-8")


def write_summary(title, markers, nodes, writes, value_map, output, base_ts):
    lines = [f"[{title}]", ""]
    for label, ts in markers:
        lines.append(f"[{label}] t={ts - base_ts:.1f}s")
        per_node = state_at(writes, value_map, ts)
        best = latest_versions(per_node)
        for node in nodes:
            node_items = per_node.get(node, {})
            if not node_items:
                lines.append(f"  {node.lstrip('/')}: (empty)")
                continue
            chunks = []
            for key in sorted(node_items):
                item = node_items[key]
                suffix = "latest" if best.get(key, {}).get("version") == item["version"] else "stale"
                chunks.append(f"{key}={item['value']}@b{item['bucket']}/v{item['version']}[{suffix}]")
            lines.append(f"  {node.lstrip('/')}: " + ", ".join(chunks))
        lines.append("")
    output.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Render KV distribution snapshots from experiment logs")
    parser.add_argument("--log-dir", required=True)
    parser.add_argument("--times-file", required=True)
    parser.add_argument("--events-file", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--summary-output", required=True)
    parser.add_argument("--title", required=True)
    args = parser.parse_args()

    log_dir = pathlib.Path(args.log_dir)
    markers = parse_markers(pathlib.Path(args.times_file))
    write_events = parse_kv_events(pathlib.Path(args.events_file))
    writes, nodes = parse_write_logs(log_dir)
    if not markers:
        raise SystemExit(f"No markers found in {args.times_file}")
    if not writes:
        raise SystemExit(f"No KV write logs found in {log_dir}")

    value_map = resolve_values(write_events, writes)
    base_ts = markers[0][1]
    output = pathlib.Path(args.output)
    summary_output = pathlib.Path(args.summary_output)
    render_svg(args.title, markers, nodes, writes, value_map, output, base_ts)
    write_summary(args.title, markers, nodes, writes, value_map, summary_output, base_ts)
    print(f"wrote {output}")
    print(f"wrote {summary_output}")


if __name__ == "__main__":
    main()