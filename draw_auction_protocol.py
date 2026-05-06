#!/usr/bin/env python3

from pathlib import Path


OUTPUT = Path("auction_protocol_flow.svg")


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


def rect(x, y, w, h, fill, stroke="#333", rx=8):
    return (
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" '
        f'fill="{fill}" stroke="{stroke}" stroke-width="2" rx="{rx}" />'
    )


def text(x, y, value, size=20, weight="normal", anchor="middle", fill="#111"):
    safe = (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )
    return (
        f'<text x="{x}" y="{y}" font-family="DejaVu Sans, Noto Sans SC, sans-serif" '
        f'font-size="{size}" font-weight="{weight}" text-anchor="{anchor}" fill="{fill}">{safe}</text>'
    )


def arrow(x1, y1, x2, y2, label, stroke="#2f5bea", dashed=False, align="middle"):
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

    if align == "left":
        tx = min(x1, x2) + 8
        anchor = "start"
    elif align == "right":
        tx = max(x1, x2) - 8
        anchor = "end"
    else:
        tx = (x1 + x2) / 2
        anchor = "middle"
    parts.append(text(tx, y1 - 12, label, size=18, anchor=anchor, fill=stroke))
    return "\n".join(parts)


def note(x, y, w, h, title, body_lines, fill="#fff7d6"):
    parts = [rect(x, y, w, h, fill=fill, stroke="#b78b00", rx=10)]
    parts.append(text(x + 12, y + 24, title, size=16, weight="bold", anchor="start", fill="#6d5200"))
    line_y = y + 48
    for body in body_lines:
        parts.append(text(x + 12, line_y, body, size=14, anchor="start", fill="#5a4a15"))
        line_y += 20
    return "\n".join(parts)


def build_svg():
    width = 1120
    height = 760
    header_y = 110
    bottom_y = 650
    master_x = 300
    bidder_x = 820

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#faf7f0" />',
        text(560, 42, "对一个 bucket 的一轮拍卖", size=32, weight="bold"),
        text(560, 74, "SVS-PS / Heartbeat", size=20, fill="#666"),
    ]

    headers = [
        (master_x, "主节点", "#f6c85f"),
        (bidder_x, "存储节点", "#6fb1fc"),
    ]

    for x, label_value, fill in headers:
        parts.append(rect(x - 90, header_y - 38, 180, 48, fill=fill))
        parts.append(text(x, header_y - 6, label_value, size=22, weight="bold"))
        parts.append(line(x, header_y + 10, x, bottom_y, stroke="#666", width=2, dash="10,8"))

    y = 170
    step = 92

    parts.append(arrow(master_x, y, bidder_x, y, "AUCTION(bucket)", stroke="#2f5bea"))

    y += step
    parts.append(arrow(bidder_x, y, master_x, y, "BID(price)", stroke="#d1495b"))

    y += step
    parts.append(arrow(master_x, y, bidder_x, y, "WIN(top-n)", stroke="#2a9d8f"))

    y += step
    parts.append(arrow(bidder_x, y, master_x, y, "WIN_ACK", stroke="#8a5cf6"))

    y += step
    parts.append(arrow(master_x, y, bidder_x, y, "AUCTION_END(winnerList)", stroke="#f08c00"))

    y += step
    parts.append(arrow(master_x, y, bidder_x, y, "Heartbeat", stroke="#888", dashed=True))
    parts.append(arrow(bidder_x, y + 28, master_x, y + 28, "Heartbeat", stroke="#888", dashed=True))

    y += step + 10
    parts.append(text((master_x + bidder_x) / 2, y, "AUCTION -> BID -> WIN -> WIN_ACK -> AUCTION_END", size=22, weight="bold", fill="#333"))

    parts.append("</svg>")
    return "\n".join(parts)


def main():
    OUTPUT.write_text(build_svg(), encoding="utf-8")
    print(f"wrote {OUTPUT}")


if __name__ == "__main__":
    main()