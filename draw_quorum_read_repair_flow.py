#!/usr/bin/env python3

from pathlib import Path


OUTPUT = Path("quorum_read_repair_flow.svg")


def esc(value: str) -> str:
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def text(x, y, value, size=30, weight="normal", anchor="middle", fill="#111"):
    return (
        f'<text x="{x}" y="{y}" font-family="DejaVu Sans, Noto Sans SC, sans-serif" '
        f'font-size="{size}" font-weight="{weight}" text-anchor="{anchor}" fill="{fill}">{esc(value)}</text>'
    )


def line(x1, y1, x2, y2, stroke="#444", width=2, dash=None):
    attrs = [
        f'x1="{x1}"',
        f'y1="{y1}"',
        f'x2="{x2}"',
        f'y2="{y2}"',
        f'stroke="{stroke}"',
        f'stroke-width="{width}"',
    ]
    if dash:
        attrs.append(f'stroke-dasharray="{dash}"')
    return f"<line {' '.join(attrs)} />"


def rect(x, y, w, h, fill="#fff", stroke="#333", rx=10):
    return (
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" '
        f'fill="{fill}" stroke="{stroke}" stroke-width="2" rx="{rx}" />'
    )


def diamond(cx, cy, w, h, fill="#fff", stroke="#333"):
    points = [
        (cx, cy - h / 2),
        (cx + w / 2, cy),
        (cx, cy + h / 2),
        (cx - w / 2, cy),
    ]
    points_text = " ".join(f"{x},{y}" for x, y in points)
    return f'<polygon points="{points_text}" fill="{fill}" stroke="{stroke}" stroke-width="2" />'


def arrow(x1, y1, x2, y2, label, stroke="#2f5bea", dashed=False, label_dx=0, label_dy=-18):
    parts = [line(x1, y1, x2, y2, stroke=stroke, width=2.5, dash="8,6" if dashed else None)]
    head = 10
    if abs(y2 - y1) > abs(x2 - x1):
        if y2 >= y1:
            p1 = (x2, y2)
            p2 = (x2 - 5, y2 - head)
            p3 = (x2 + 5, y2 - head)
        else:
            p1 = (x2, y2)
            p2 = (x2 - 5, y2 + head)
            p3 = (x2 + 5, y2 + head)
    elif x2 >= x1:
        p1 = (x2, y2)
        p2 = (x2 - head, y2 - 5)
        p3 = (x2 - head, y2 + 5)
    else:
        p1 = (x2, y2)
        p2 = (x2 + head, y2 - 5)
        p3 = (x2 + head, y2 + 5)
    parts.append(
        f'<polygon points="{p1[0]},{p1[1]} {p2[0]},{p2[1]} {p3[0]},{p3[1]}" fill="{stroke}" />'
    )
    if label:
        parts.append(text((x1 + x2) / 2 + label_dx, (y1 + y2) / 2 + label_dy, label, size=26, fill=stroke))
    return "\n".join(parts)


def self_box(x, y, w, h, title, body=None, fill="#f4f4f4", stroke="#666"):
    parts = [rect(x, y, w, h, fill=fill, stroke=stroke, rx=10)]
    parts.append(text(x + w / 2, y + 38, title, size=32, weight="bold"))
    if body:
        for idx, line_text in enumerate(body):
            parts.append(text(x + w / 2, y + 82 + idx * 32, line_text, size=26, fill="#333"))
    return "\n".join(parts)


def decision(cx, cy, title, yes_label=None, no_label=None, fill="#f2e9fb", stroke="#8a5cf6"):
    parts = [diamond(cx, cy, 320, 170, fill=fill, stroke=stroke)]
    parts.append(text(cx, cy - 8, title, size=30, weight="bold"))
    if yes_label:
        parts.append(text(cx + 150, cy - 8, yes_label, size=22, anchor="start", fill=stroke))
    if no_label:
        parts.append(text(cx - 150, cy - 8, no_label, size=22, anchor="end", fill=stroke))
    return "\n".join(parts)


def build_svg():
    width = 1640
    height = 1600
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#faf8f2" />',
        text(width / 2, 64, "法定人数读与读修复流程图", size=54, weight="bold"),
        text(width / 2, 110, "R=2", size=30, fill="#666"),
    ]

    cx = width / 2
    y = 200
    step = 156

    parts.append(self_box(cx - 220, y, 440, 108, "发起读取", ["KV_GET"], fill="#fff4cf", stroke="#c29b1f"))

    y += step
    parts.append(arrow(cx, y - 48, cx, y - 6, "", stroke="#d1495b"))
    parts.append(self_box(cx - 260, y, 520, 126, "并行查询", ["发送读取请求", "记录返回结果"], fill="#e6f4fb", stroke="#4c7ea6"))

    y += step + 8
    parts.append(arrow(cx, y - 44, cx, y, "", stroke="#2a9d8f"))
    parts.append(self_box(cx - 380, y, 760, 140, "收集结果", ["获得多个副本响应", "存在新旧版本差异"], fill="#e8f5e9", stroke="#4f7f76"))

    y += step + 18
    parts.append(arrow(cx, y - 52, cx, y - 10, "", stroke="#8a5cf6"))
    parts.append(decision(cx, y + 40, "达到 R=2 ?", yes_label="是"))

    y += step + 34
    parts.append(arrow(cx, y - 32, cx, y + 8, "", stroke="#8a5cf6"))
    parts.append(decision(cx, y + 40, "存在有效值 ?", yes_label="是", no_label="否"))

    parts.append(arrow(cx - 160, y + 40, 320, y + 40, "否", stroke="#c95d63", label_dy=-24))
    parts.append(self_box(60, y - 10, 260, 104, "结束", ["返回未命中"], fill="#fde2e4", stroke="#c95d63"))

    y += step + 36
    parts.append(arrow(cx, y - 34, cx, y + 6, "", stroke="#8a5cf6"))
    parts.append(self_box(cx - 240, y, 480, 126, "选择最新版本", ["比较各副本结果", "确定 best"], fill="#f2e9fb", stroke="#8a5cf6"))

    y += step + 4
    parts.append(arrow(cx, y - 44, cx, y, "", stroke="#8a5cf6"))
    parts.append(decision(cx, y + 40, "存在滞后副本 ?", yes_label="是", no_label="否"))

    parts.append(arrow(cx + 160, y + 40, 1320, y + 40, "否", stroke="#2f5bea", label_dy=-24))
    parts.append(self_box(1320, y - 10, 260, 104, "结束", ["返回最新值"], fill="#e7f6fd", stroke="#2f5bea"))

    y += step + 36
    parts.append(arrow(cx, y - 34, cx, y + 6, "", stroke="#e76f51"))
    parts.append(self_box(cx - 260, y, 520, 126, "发送修复", ["向滞后副本写回", "携带最新版本"], fill="#fff1c7", stroke="#d17b00"))

    y += step
    parts.append(arrow(cx, y - 44, cx, y, "", stroke="#e76f51"))
    parts.append(self_box(cx - 240, y, 480, 126, "副本更新", ["覆盖旧版本", "完成收敛"], fill="#fff8e1", stroke="#d17b00"))

    y += step
    parts.append(arrow(cx, y - 44, cx, y, "", stroke="#2f5bea"))
    parts.append(self_box(cx - 250, y, 500, 126, "返回结果", ["客户端得到最新值", "同时触发修复"], fill="#e7f6fd", stroke="#2f5bea"))

    y += step
    parts.append(arrow(cx, y - 44, cx, y - 2, "", stroke="#2f5bea"))
    parts.append(self_box(cx - 180, y, 360, 104, "结束", ["读到最新值", "副本收敛"], fill="#e0f0eb", stroke="#4f7f76"))

    parts.append("</svg>")
    return "\n".join(parts)


def main():
    OUTPUT.write_text(build_svg(), encoding="utf-8")
    print(f"wrote {OUTPUT}")


if __name__ == "__main__":
    main()