#!/usr/bin/env python3

from pathlib import Path


OUTPUT = Path("object_rw_topology.svg")


def esc(value: str) -> str:
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def text(x, y, value, size=22, weight="normal", anchor="middle", fill="#111"):
    return (
        f'<text x="{x}" y="{y}" font-family="DejaVu Sans, Noto Sans SC, sans-serif" '
        f'font-size="{size}" font-weight="{weight}" text-anchor="{anchor}" fill="{fill}">{esc(value)}</text>'
    )


def rect(x, y, w, h, fill="#fff", stroke="#333", rx=16):
    return (
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" fill="{fill}" '
        f'stroke="{stroke}" stroke-width="2" rx="{rx}" />'
    )


def circle(cx, cy, r, fill="#fff", stroke="#333"):
    return f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="{fill}" stroke="{stroke}" stroke-width="2" />'


def line(x1, y1, x2, y2, stroke="#333", width=2, dash=None):
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


def arrow(x1, y1, x2, y2, label, stroke="#2f5bea", size=20, dashed=False, label_dx=0, label_dy=-14):
    parts = [line(x1, y1, x2, y2, stroke=stroke, width=2.5, dash="8,6" if dashed else None)]
    head = 10
    if x2 >= x1:
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
    parts.append(text((x1 + x2) / 2 + label_dx, (y1 + y2) / 2 + label_dy, label, size=size, fill=stroke))
    return "\n".join(parts)


def build_svg():
    width = 1320
    height = 980

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#faf8f2" />',
        text(width / 2, 52, "原始 kua 对象读写拓扑图", size=40, weight="bold"),
    ]

    # Write topology
    parts.append(rect(40, 96, 1240, 360, fill="#fffdf8", stroke="#c7bda8", rx=24))
    parts.append(text(110, 136, "对象写入拓扑", size=30, weight="bold", anchor="start"))

    parts.append(rect(80, 210, 190, 120, fill="#f6d365", stroke="#c29b1f", rx=20))
    parts.append(text(175, 276, "Client", size=30, weight="bold"))

    parts.append(rect(980, 182, 210, 84, fill="#efe3ff", stroke="#8a5cf6", rx=18))
    parts.append(text(1085, 214, "NDN", size=28, weight="bold", fill="#6b44c7"))
    parts.append(text(1085, 246, "原始 Data", size=24, fill="#6b44c7"))

    write_nodes = [
        (470, 270, "接入副本", "#8ecae6"),
        (720, 190, "副本 A", "#84a59d"),
        (835, 340, "副本 B", "#84a59d"),
        (635, 360, "副本 C", "#84a59d"),
    ]
    for cx, cy, label, fill in write_nodes:
        parts.append(circle(cx, cy, 52, fill=fill, stroke="#4f4f4f"))
        parts.append(text(cx, cy + 8, label, size=22, weight="bold"))

    parts.append(line(518, 250, 674, 208, stroke="#999", width=2, dash="8,8"))
    parts.append(line(512, 292, 786, 332, stroke="#999", width=2, dash="8,8"))
    parts.append(line(486, 312, 604, 330, stroke="#999", width=2, dash="8,8"))

    parts.append(arrow(270, 270, 418, 270, "写请求", stroke="#d1495b", size=20))
    parts.append(arrow(520, 248, 980, 220, "抓取原始 Data", stroke="#2a9d8f", size=18, label_dx=40))
    parts.append(arrow(768, 186, 980, 202, "抓取", stroke="#2a9d8f", size=18, label_dx=14))
    parts.append(arrow(883, 334, 980, 234, "抓取", stroke="#2a9d8f", size=18, label_dx=18, label_dy=8))
    parts.append(arrow(683, 358, 980, 248, "抓取", stroke="#2a9d8f", size=18, label_dx=30, label_dy=8))
    parts.append(arrow(720, 142, 470, 220, "确认", stroke="#8a5cf6", size=18, label_dx=-20, label_dy=-18))
    parts.append(arrow(835, 288, 496, 286, "确认", stroke="#8a5cf6", size=18, label_dx=-12, label_dy=-18))
    parts.append(arrow(635, 308, 452, 302, "确认", stroke="#8a5cf6", size=18, label_dx=-12, label_dy=-18))
    parts.append(arrow(418, 316, 270, 316, "写入成功", stroke="#2f5bea", size=20))

    # Read topology
    parts.append(rect(40, 500, 1240, 360, fill="#fffdf8", stroke="#c7bda8", rx=24))
    parts.append(text(110, 540, "对象读取拓扑", size=30, weight="bold", anchor="start"))

    parts.append(rect(80, 612, 190, 120, fill="#f6d365", stroke="#c29b1f", rx=20))
    parts.append(text(175, 678, "Client", size=30, weight="bold"))

    read_nodes = [
        (560, 670, "接入副本", "#8ecae6"),
        (940, 620, "网络缓存", "#cdb4db"),
    ]
    for cx, cy, label, fill in read_nodes:
        parts.append(circle(cx, cy, 58, fill=fill, stroke="#4f4f4f"))
        parts.append(text(cx, cy + 8, label, size=22, weight="bold"))

    parts.append(arrow(270, 650, 500, 650, "读请求", stroke="#2f5bea", size=20))
    parts.append(arrow(618, 638, 882, 620, "同名请求可命中缓存", stroke="#8a5cf6", size=18, dashed=True, label_dx=18))
    parts.append(arrow(500, 696, 270, 696, "返回 Data / NACK", stroke="#2a9d8f", size=20))

    parts.append(text(640, 412, "副本节点收到写请求后，从 NDN 抓取原始 Data 并本地写入", size=24, fill="#666"))
    parts.append(text(640, 812, "读取时，请求被引导到对应副本；命中后直接返回 Data", size=24, fill="#666"))

    parts.append("</svg>")
    return "\n".join(parts)


def main():
    OUTPUT.write_text(build_svg(), encoding="utf-8")
    print(f"wrote {OUTPUT}")


if __name__ == "__main__":
    main()