#!/usr/bin/env python3

from pathlib import Path


OUTPUT = Path("migration_swimlane.svg")


def esc(value: str) -> str:
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def text(x, y, value, size=28, weight="normal", anchor="middle", fill="#111"):
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


def end_symbol(cx, cy, w=180, h=68, label="结束", fill="#ffe3e3", stroke="#c95d63"):
    return "\n".join(
        [
            f'<ellipse cx="{cx}" cy="{cy}" rx="{w / 2}" ry="{h / 2}" fill="{fill}" stroke="{stroke}" stroke-width="2" />',
            text(cx, cy + 10, label, size=30, weight="bold", fill=stroke),
        ]
    )


def arrow(x1, y1, x2, y2, label, stroke="#2f5bea", dashed=False):
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
    parts.append(text((x1 + x2) / 2, y1 - 20, label, size=30, fill=stroke))
    return "\n".join(parts)


def self_box(x, y, w, h, title, fill="#f4f4f4", stroke="#666"):
    parts = [rect(x, y, w, h, fill=fill, stroke=stroke, rx=8)]
    parts.append(text(x + w / 2, y + h / 2 + 10, title, size=29))
    return "\n".join(parts)


def build_svg():
    width = 1760
    height = 1400
    header_y = 120
    lane_top = 155
    lane_bottom = 1340

    lanes = [
        (220, "SVS / 成员视图", "#f6d365"),
        (730, "旧 owner", "#f28482"),
        (1220, "新 owner", "#84a59d"),
        (1630, "NDN 路由", "#8ecae6"),
    ]

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#faf8f2" />',
        text(width / 2, 60, "数据迁移泳道图", size=50, weight="bold"),
    ]

    for x, label, fill in lanes:
        parts.append(rect(x - 135, header_y - 42, 270, 58, fill=fill))
        parts.append(text(x, header_y + 2, label, size=38, weight="bold"))
        parts.append(line(x, lane_top, x, lane_bottom, stroke="#777", width=2, dash="10,8"))

    svs_x, old_x, new_x, net_x = [item[0] for item in lanes]
    y = 220
    step = 124

    parts.append(self_box(svs_x - 180, y - 34, 360, 64, "SVS 更新视图", fill="#fff4cf", stroke="#c29b1f"))

    y += step
    parts.append(arrow(svs_x, y, old_x, y, "触发重算", stroke="#b8860b"))
    parts.append(arrow(svs_x, y + 42, new_x, y + 42, "触发重算", stroke="#b8860b"))

    y += step
    parts.append(self_box(old_x - 210, y - 34, 420, 64, "Maglev 识别归属变化", fill="#ffe3e3", stroke="#c95d63"))
    parts.append(self_box(new_x - 210, y - 34, 420, 64, "Maglev 识别归属变化", fill="#e0f0eb", stroke="#4f7f76"))

    y += step
    parts.append(self_box(new_x - 225, y - 46, 450, 84, "创建本地服务\n注册名称前缀", fill="#e0f0eb", stroke="#4f7f76"))

    y += step
    parts.append(self_box(old_x - 210, y - 42, 420, 82, "等待片刻\n确保新 owner 就绪", fill="#ffe3e3", stroke="#c95d63"))

    y += step
    parts.append(arrow(old_x, y, new_x, y, "INSERT | NO_REPLICATE | MIGRATE", stroke="#d1495b"))

    y += step
    parts.append(self_box(new_x - 225, y - 42, 450, 82, "收到迁移命令\n准备抓取数据", fill="#e0f0eb", stroke="#4f7f76"))

    y += step
    parts.append(arrow(new_x, y, net_x, y, "发送 Interest", stroke="#2f5bea"))

    y += step
    parts.append(arrow(net_x, y, old_x, y, "名称路由", stroke="#4c78a8", dashed=True))

    y += step - 42
    parts.append(arrow(old_x, y, new_x, y, "返回 Data", stroke="#2a9d8f"))

    y += step
    parts.append(line(old_x, y - 18, old_x, y + 30, stroke="#c95d63", width=2.5))
    parts.append(end_symbol(old_x, y + 72))

    parts.append("</svg>")
    return "\n".join(parts)


def main():
    OUTPUT.write_text(build_svg(), encoding="utf-8")
    print(f"wrote {OUTPUT}")


if __name__ == "__main__":
    main()