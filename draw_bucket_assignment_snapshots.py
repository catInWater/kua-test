#!/usr/bin/env python3

from __future__ import annotations

import argparse
import collections
import pathlib
import re


SNAPSHOT_RE = re.compile(r"^(?P<ts>\d+\.\d+).*BUCKET_SNAPSHOT node=(?P<node>/\S+)(?P<body>.*)$")
BUCKET_RE = re.compile(r"bucket(?P<bucket>\d+)=\[(?P<owners>[^\]]*)\]")
SEND_RE = re.compile(r"^(?P<ts>\d+\.\d+).*发送迁移请求 #\d+: (?P<data>/\S+) -> (?P<target>/\S+)")
RECEIVE_RE = re.compile(r"^(?P<ts>\d+\.\d+).*接收数据并存储成功: (?P<data>/\S+) 到 bucket #(?P<bucket>\d+)")
CLIENT_PUT_RE = re.compile(r"已为前缀 (?P<prefix>/\S+) 创建 \d+ 个 chunk")
REQUEST_BUCKET_RE = re.compile(r"收到请求 : #(?P<bucket>\d+) : /(?P<name>[^\s]+)/seg=0")

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


def parse_logs(log_dir: pathlib.Path):
    snapshots = collections.defaultdict(list)
    for path in sorted(log_dir.glob("*.log")):
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            match = SNAPSHOT_RE.match(line)
            if not match:
                continue
            ts = float(match.group("ts"))
            node = match.group("node")
            owners_by_bucket = {}
            for bucket_match in BUCKET_RE.finditer(match.group("body")):
                bucket_id = int(bucket_match.group("bucket"))
                owners_text = bucket_match.group("owners").strip()
                owners = tuple(part for part in owners_text.split(",") if part)
                owners_by_bucket[bucket_id] = owners
            if owners_by_bucket:
                snapshots[node].append((ts, owners_by_bucket))
    return snapshots


def parse_migration_receives(log_dir: pathlib.Path):
    receives = []
    for path in sorted(log_dir.glob("*.log")):
        node = f"/{path.stem}"
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            match = RECEIVE_RE.match(line)
            if not match:
                continue
            receives.append({
                "ts": float(match.group("ts")),
                "node": node,
                "bucket": int(match.group("bucket")),
                "data": match.group("data"),
            })
    return receives


def parse_migration_sends(log_dir: pathlib.Path):
    sends = []
    for path in sorted(log_dir.glob("*.log")):
        node = f"/{path.stem}"
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            match = SEND_RE.match(line)
            if not match:
                continue
            sends.append({
                "ts": float(match.group("ts")),
                "node": node,
                "target": match.group("target"),
                "data": match.group("data"),
            })
    return sends


def parse_inserted_data(log_dir: pathlib.Path):
    inserted_names = []
    client_put_path = log_dir / "client-put.log"
    if client_put_path.exists():
        for line in client_put_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            match = CLIENT_PUT_RE.search(line)
            if match:
                name = match.group("prefix").lstrip("/")
                if name not in inserted_names:
                    inserted_names.append(name)

    bucket_by_name = {}
    for path in sorted(log_dir.glob("*.log")):
        if path.name == "client-put.log":
            continue
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            match = REQUEST_BUCKET_RE.search(line)
            if not match:
                continue
            name = match.group("name")
            if name not in bucket_by_name:
                bucket_by_name[name] = int(match.group("bucket"))

    by_bucket = collections.defaultdict(list)
    unknown = []
    for name in inserted_names:
        bucket_id = bucket_by_name.get(name)
        if bucket_id is None:
            unknown.append(name)
            continue
        by_bucket[bucket_id].append(name)

    return by_bucket, unknown


def parse_markers(path: pathlib.Path):
    markers = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        label, ts_text = line.split("\t", 1)
        markers.append((label, float(ts_text)))
    return markers


def latest_snapshot_before(entries, ts):
    latest = None
    for entry_ts, assignment in entries:
        if entry_ts <= ts:
            latest = (entry_ts, assignment)
        else:
            break
    return latest


def consensus_snapshot(snapshots, ts):
    per_node = {}
    freshness_window = 9.0
    for node, entries in snapshots.items():
        latest = latest_snapshot_before(entries, ts)
        if latest is None:
            continue
        entry_ts, assignment = latest
        if ts - entry_ts <= freshness_window:
            per_node[node] = assignment

    bucket_ids = sorted({bucket_id for assignment in per_node.values() for bucket_id in assignment})
    resolved = {}
    disagreements = {}
    for bucket_id in bucket_ids:
        variants = [assignment[bucket_id] for assignment in per_node.values() if bucket_id in assignment]
        if not variants:
          continue
        counts = collections.Counter(variants)
        resolved[bucket_id], _ = counts.most_common(1)[0]
        disagreements[bucket_id] = len(counts) > 1
    return resolved, disagreements, sorted(per_node)


def migration_receives_between(receives, start_ts, end_ts):
    by_node = collections.defaultdict(lambda: collections.defaultdict(list))
    for event in receives:
        if start_ts < event["ts"] <= end_ts:
            bucket_events = by_node[event["node"]][event["bucket"]]
            if event["data"] not in bucket_events:
                bucket_events.append(event["data"])
    return by_node


def migration_sends_between(sends, data_to_bucket, start_ts, end_ts):
    by_node = collections.defaultdict(lambda: collections.defaultdict(list))
    for event in sends:
        if start_ts < event["ts"] <= end_ts:
            bucket_id = data_to_bucket.get(event["data"])
            if bucket_id is None:
                continue
            bucket_events = by_node[event["node"]][bucket_id]
            if event["data"] not in bucket_events:
                bucket_events.append(event["data"])
    return by_node


def short_data_name(name: str) -> str:
    short = name.lstrip("/")
    short = short.replace("/seg=0", "")
    return short


def node_sort_key(node: str):
    return (NODE_ORDER.get(node, 999), node)


def build_alias_map(receives, start_ts, end_ts):
    ordered = []
    seen = set()
    for event in sorted(receives, key=lambda item: (item["ts"], item["bucket"], item["data"])):
        if start_ts < event["ts"] <= end_ts and event["data"] not in seen:
            seen.add(event["data"])
            ordered.append(event["data"])
    return {name: f"d{index + 1}" for index, name in enumerate(ordered)}

def bucket_data_summary(events_by_node):
    by_bucket = collections.defaultdict(list)
    for bucket_map in events_by_node.values():
        for bucket_id, names in bucket_map.items():
            for name in names:
                if name not in by_bucket[bucket_id]:
                    by_bucket[bucket_id].append(name)
    return by_bucket


def chip(x, y, w, h, label, fill):
    parts = [rect(x, y, w, h, fill=fill, stroke="#555", rx=18)]
    parts.append(text(x + w / 2, y + h / 2 + 8, label, size=24, weight="bold"))
    return "\n".join(parts)


def bucket_chip(x, y, label, highlighted=False, thick=False):
    fill = "#fff1c7" if highlighted else "#ffffff"
    stroke = "#d17b00" if highlighted else "#5a5a5a"
    stroke_width = "4" if thick else "2"
    return (chip(x, y, 72, 42, label, fill)
            .replace('stroke="#555"', f'stroke="{stroke}"')
            .replace('stroke-width="2"', f'stroke-width="{stroke_width}"', 1))


def render_svg(title, markers, snapshots, sends, receives, inserted_by_bucket, output, base_ts):
    panel_count = len(markers)
    width = 1720
    panel_height = 440
    height = 180 + panel_count * panel_height
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#faf8f2" />',
        text(width / 2, 60, title, size=42, weight="bold"),
    ]

    migration_start = markers[0][1] - 0.001
    migration_end = markers[-1][1]
    alias_map = build_alias_map(receives, migration_start, migration_end)
    data_to_bucket = {event["data"]: event["bucket"] for event in receives}
    future_send_by_node = migration_sends_between(sends, data_to_bucket, migration_start, migration_end)

    alias_x = 70
    alias_y = 108
    parts.append(text(alias_x, alias_y, "数据标记", size=24, weight="bold", anchor="start"))
    alias_y += 28
    for name in sorted(alias_map, key=lambda item: alias_map[item]):
        parts.append(text(alias_x, alias_y, f"{alias_map[name]} = {short_data_name(name)}", size=20, anchor="start", fill="#333"))
        alias_x += 210
        if alias_x > width - 230:
            alias_x = 70
            alias_y += 24

    first_resolved, _, _ = consensus_snapshot(snapshots, markers[0][1])
    last_resolved, _, _ = consensus_snapshot(snapshots, markers[-1][1])
    all_nodes = sorted({*snapshots.keys(), *(event["node"] for event in sends), *(event["node"] for event in receives)}, key=node_sort_key)

    first_buckets_by_node = collections.defaultdict(list)
    for bucket_id, owners in first_resolved.items():
        for owner in owners:
            first_buckets_by_node[owner].append(bucket_id)

    for index, (label, ts) in enumerate(markers):
        top = 150 + index * panel_height
        panel_height_inner = 388 if index == 0 else 350
        parts.append(rect(40, top, width - 80, panel_height_inner, fill="#fffdf8", stroke="#c7bda8", rx=18))
        parts.append(text(90, top + 44, label, size=30, weight="bold", anchor="start"))
        parts.append(text(width - 90, top + 44, f"t={ts - base_ts:.1f}s", size=24, anchor="end", fill="#666"))

        resolved, disagreements, active_nodes = consensus_snapshot(snapshots, ts)
        if not resolved:
            parts.append(text(width / 2, top + 140, "未找到可用桶分配快照", size=24, fill="#b42318"))
            continue

        previous_ts = base_ts - 0.001 if index == 0 else markers[index - 1][1]
        migration_by_node = migration_receives_between(receives, previous_ts, ts)

        buckets_by_node = collections.defaultdict(list)
        for bucket_id, owners in resolved.items():
            for owner in owners:
                buckets_by_node[owner].append(bucket_id)

        changed_buckets_by_node = collections.defaultdict(set)
        if index == len(markers) - 1:
            for node in all_nodes:
                previous_node_buckets = set(first_buckets_by_node.get(node, []))
                current_node_buckets = set(buckets_by_node.get(node, []))
                changed_buckets_by_node[node] = current_node_buckets - previous_node_buckets

        if index == 0:
            summary_y = top + 72
            parts.append(text(78, summary_y, "实验全部数据", size=22, weight="bold", anchor="start", fill="#6b3f00"))
            summary_x = 240
            summary_col_gap = 700
            summary_row_gap = 30
            for idx, bucket_id in enumerate(sorted(first_resolved)):
                col = idx % 2
                row = idx // 2
                line_x = summary_x + col * summary_col_gap
                line_y = summary_y + row * summary_row_gap
                names = ", ".join(short_data_name(name) for name in inserted_by_bucket.get(bucket_id, []))
                label_text = names if names else "无数据"
                parts.append(text(line_x, line_y, f"b{bucket_id}: {label_text}", size=20, anchor="start", fill="#333"))

        card_y = top + 152 if index == 0 else top + 96
        card_w = 300
        card_h = 230
        gap = 18
        bucket_col_gap = 82
        bucket_row_gap = 50

        for card_index, node in enumerate(all_nodes):
            x = 78 + card_index * (card_w + gap)
            y = card_y
            is_active = node in active_nodes
            fill = NODE_COLORS.get(node, "#e6e6e6") if is_active else "#efefef"
            parts.append(rect(x, y, card_w, card_h, fill=fill, stroke="#666", rx=20))
            parts.append(text(x + 24, y + 40, node.lstrip("/"), size=30, weight="bold", anchor="start"))

            node_buckets = sorted(buckets_by_node.get(node, []))
            bucket_start_x = x + 20
            bucket_start_y = y + 60
            for bucket_index, bucket_id in enumerate(node_buckets):
                col = bucket_index % 3
                row = bucket_index // 3
                chip_x = bucket_start_x + col * bucket_col_gap
                chip_y = bucket_start_y + row * bucket_row_gap
                is_changed = bucket_id in changed_buckets_by_node[node]
                parts.append(bucket_chip(chip_x, chip_y, f"b{bucket_id}", highlighted=is_changed, thick=is_changed))

            if not node_buckets:
                parts.append(text(x + 24, y + 104, "无相关 bucket", size=19, anchor="start", fill="#4f4f4f"))

            if index != 0:
                migrated = migration_by_node.get(node, {})
                if migrated:
                    parts.append(text(x + 24, y + 182, "已迁入", size=20, weight="bold", anchor="start", fill="#6b3f00"))
                    text_y = y + 202
                    for bucket_id in sorted(migrated):
                        aliases = ", ".join(alias_map.get(name, short_data_name(name)) for name in migrated[bucket_id][:6])
                        parts.append(text(x + 24, text_y, f"b{bucket_id}: {aliases}", size=18, anchor="start", fill="#2c2c2c"))
                        text_y += 22
                elif is_active:
                    parts.append(text(x + 24, y + 198, "本阶段无迁入数据", size=19, anchor="start", fill="#4f4f4f"))

        if any(disagreements.values()):
            note_y = top + (364 if index == 0 else 326)
            parts.append(text(width - 90, note_y, "注: 高亮 bucket 表示该时刻各节点日志尚未完全收敛", size=20, anchor="end", fill="#8a6d3b"))

    parts.append("</svg>")
    output.write_text("\n".join(parts), encoding="utf-8")


def write_summary(markers, snapshots, sends, receives, output, base_ts):
    lines = []
    migration_start = markers[0][1] - 0.001
    migration_end = markers[-1][1]
    alias_map = build_alias_map(receives, migration_start, migration_end)
    data_to_bucket = {event["data"]: event["bucket"] for event in receives}
    future_send_by_node = migration_sends_between(sends, data_to_bucket, migration_start, migration_end)
    lines.append("[数据别名]")
    for name in sorted(alias_map, key=lambda item: alias_map[item]):
        lines.append(f"  {alias_map[name]}: {short_data_name(name)}")
    lines.append("")

    for index, (label, ts) in enumerate(markers):
        resolved, disagreements, active_nodes = consensus_snapshot(snapshots, ts)
        previous_ts = base_ts - 0.001 if index == 0 else markers[index - 1][1]
        migration_by_node = migration_receives_between(receives, previous_ts, ts)
        send_by_node = future_send_by_node if index == 0 else migration_sends_between(sends, data_to_bucket, previous_ts, ts)
        lines.append(f"[{label}] t={ts - base_ts:.1f}s active-nodes={','.join(active_nodes)}")
        for bucket_id in sorted(resolved):
            owners = ", ".join(owner.lstrip("/") for owner in resolved[bucket_id]) or "-"
            suffix = " (pending)" if disagreements.get(bucket_id) else ""
            lines.append(f"  bucket {bucket_id}: {owners}{suffix}")
        if index == 0:
            for node in sorted(send_by_node, key=node_sort_key):
                for bucket_id in sorted(send_by_node[node]):
                    names = ", ".join(alias_map.get(name, short_data_name(name)) for name in send_by_node[node][bucket_id])
                    lines.append(f"  send {node.lstrip('/')} bucket {bucket_id}: {names}")
        else:
            for node in sorted(migration_by_node, key=node_sort_key):
                for bucket_id in sorted(migration_by_node[node]):
                    names = ", ".join(alias_map.get(name, short_data_name(name)) for name in migration_by_node[node][bucket_id])
                    lines.append(f"  receive {node.lstrip('/')} bucket {bucket_id}: {names}")
        lines.append("")
    output.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Render bucket assignment snapshots from experiment logs")
    parser.add_argument("--log-dir", required=True)
    parser.add_argument("--times-file", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--summary-output", required=True)
    parser.add_argument("--title", required=True)
    args = parser.parse_args()

    log_dir = pathlib.Path(args.log_dir)
    times_file = pathlib.Path(args.times_file)
    output = pathlib.Path(args.output)
    summary_output = pathlib.Path(args.summary_output)

    snapshots = parse_logs(log_dir)
    sends = parse_migration_sends(log_dir)
    receives = parse_migration_receives(log_dir)
    inserted_by_bucket, _ = parse_inserted_data(log_dir)
    markers = parse_markers(times_file)
    if not snapshots:
        raise SystemExit(f"No BUCKET_SNAPSHOT lines found in {log_dir}")
    if not markers:
        raise SystemExit(f"No markers found in {times_file}")

    if len(markers) > 2:
        markers = [markers[0], markers[-1]]

    base_ts = markers[0][1]
    render_svg(args.title, markers, snapshots, sends, receives, inserted_by_bucket, output, base_ts)
    write_summary(markers, snapshots, sends, receives, summary_output, base_ts)
    print(f"wrote {output}")
    print(f"wrote {summary_output}")


if __name__ == "__main__":
    main()