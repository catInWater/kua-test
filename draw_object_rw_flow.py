#!/usr/bin/env python3

from pathlib import Path


OUTPUT = Path("object_rw_flow.svg")


def esc(value: str) -> str:
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def text(x, y, value, size=22, weight="normal", anchor="middle", fill="#111"):
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


def rect(x, y, w, h, fill="#fff", stroke="#333", rx=12):
    return (
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" '
        f'fill="{fill}" stroke="{stroke}" stroke-width="2" rx="{rx}" />'
    )


def panel(x, y, w, h, title):
    parts = [rect(x, y, w, h, fill="#fffdf8", stroke="#c7bda8", rx=18)]
    parts.append(text(x + 18, y + 36, title, size=30, weight="bold", anchor="start"))
    return "\n".join(parts)


def arrow(x1, y1, x2, y2, label, stroke="#2f5bea", dashed=False, size=20):
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
    parts.append(text((x1 + x2) / 2, y1 - 16, label, size=size, fill=stroke))
    return "\n".join(parts)


def note(x, y, w, h, title, body, fill="#fff4cf", stroke="#b8860b"):
    parts = [rect(x, y, w, h, fill=fill, stroke=stroke, rx=10)]
    parts.append(text(x + 14, y + 26, title, size=18, weight="bold", anchor="start", fill=stroke))
    line_y = y + 52
    for line_text in body:
        parts.append(text(x + 14, line_y, line_text, size=16, anchor="start", fill="#333"))
        line_y += 20
    return "\n".join(parts)


def build_svg():
    width = 1680
    height = 1320

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#faf8f2" />',
        text(width / 2, 52, "原始 kua 对象读写流程", size=42, weight="bold"),
    ]

    # Write panel
    parts.append(panel(40, 90, 1600, 560, "对象写入流程"))
    wx = [180, 540, 930, 1310, 1530]
    write_labels = [
        (wx[0], "客户端", "#f6d365"),
        (wx[1], "最近副本", "#8ecae6"),
        (wx[2], "其他副本", "#84a59d"),
        (wx[3], "客户端数据源", "#f5cac3"),
        (wx[4], "网络缓存", "#cdb4db"),
    ]

    for x, label, fill in write_labels:
        parts.append(rect(x - 90, 134, 180, 50, fill=fill))
        parts.append(text(x, 166, label, size=24, weight="bold"))
        parts.append(line(x, 194, x, 616, stroke="#777", width=2, dash="10,8"))

    y = 236
    step = 72
    parts.append(arrow(wx[0], y, wx[1], y, "/<prefix>/<bucket>/INSERT/<data-name>"))

    y += step
    parts.append(note(390, y - 34, 300, 84, "写入入口", ["客户端先计算 bucket 编号", "Interest 被路由到最近副本"]))

    y += step
    parts.append(arrow(wx[1], y, wx[2], y, "复制并转发到所有副本", stroke="#d1495b"))

    y += step
    parts.append(arrow(wx[1], y, wx[3], y, "Interest 抓取原始 Data", stroke="#2a9d8f"))
    parts.append(arrow(wx[2], y + 28, wx[3], y + 28, "NO_REPLICATE 后抓取原始 Data", stroke="#2a9d8f"))

    y += step
    parts.append(arrow(wx[3], y, wx[4], y, "Data 返回并可被缓存", stroke="#f08c00"))
    parts.append(arrow(wx[4], y + 28, wx[2], y + 28, "缓存/聚合复用同一份 Data", stroke="#f08c00", dashed=True, size=18))
    parts.append(arrow(wx[4], y + 56, wx[1], y + 56, "缓存/聚合复用同一份 Data", stroke="#f08c00", dashed=True, size=18))

    y += step + 26
    parts.append(note(770, y - 28, 470, 88, "副本写入", ["各副本写入本地对象存储", "客户端只需响应一次数据传输"]))

    y += step
    parts.append(arrow(wx[2], y, wx[1], y, "副本确认", stroke="#8a5cf6"))

    y += step
    parts.append(arrow(wx[1], y, wx[0], y, "全部副本确认后返回写入成功", stroke="#2f5bea"))

    # Read panel
    parts.append(panel(40, 700, 1600, 560, "对象读取流程"))
    rx = [210, 680, 1120, 1460]
    read_labels = [
        (rx[0], "客户端", "#f6d365"),
        (rx[1], "NDN 路由", "#8ecae6"),
        (rx[2], "最近副本", "#84a59d"),
        (rx[3], "网络缓存", "#cdb4db"),
    ]

    for x, label, fill in read_labels:
        parts.append(rect(x - 90, 744, 180, 50, fill=fill))
        parts.append(text(x, 776, label, size=24, weight="bold"))
        parts.append(line(x, 804, x, 1226, stroke="#777", width=2, dash="10,8"))

    y = 846
    parts.append(arrow(rx[0], y, rx[1], y, "Interest(data-name) + Forwarding Hint"))

    y += step
    parts.append(arrow(rx[1], y, rx[2], y, "按 <cluster-prefix,bucket-id> 路由", stroke="#4c78a8"))

    y += step
    parts.append(note(930, y - 30, 350, 84, "本地查找", ["副本节点查找本地对象存储", "命中则直接返回 Data"]))

    y += step
    parts.append(arrow(rx[2], y, rx[3], y, "Data 以原始名称返回", stroke="#2a9d8f"))
    parts.append(arrow(rx[3], y + 28, rx[0], y + 28, "后续请求可直接命中缓存", stroke="#2a9d8f", dashed=True, size=18))

    y += step + 18
    parts.append(note(430, y - 26, 540, 88, "读取特性", ["同名请求可通过 Interest 聚合共享返回结果", "未命中则由副本返回应用层 NACK"]))

    y += step + 12
    parts.append(arrow(rx[2], y, rx[0], y, "命中则返回 Data / 未命中则返回 NACK", stroke="#d1495b"))

    parts.append("</svg>")
    return "\n".join(parts)


def main():
    OUTPUT.write_text(build_svg(), encoding="utf-8")
    print(f"wrote {OUTPUT}")


if __name__ == "__main__":
    main()