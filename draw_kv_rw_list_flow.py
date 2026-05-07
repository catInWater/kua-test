#!/usr/bin/env python3

from pathlib import Path


OUTPUT_PUT = Path("kv_put_swimlane.svg")
OUTPUT_GET = Path("kv_get_swimlane.svg")
OUTPUT_LIST = Path("kv_list_swimlane.svg")


def esc(value: str) -> str:
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def text(x, y, value, size=26, weight="normal", anchor="middle", fill="#111"):
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
    parts.append(text((x1 + x2) / 2 + label_dx, (y1 + y2) / 2 + label_dy, label, size=24, fill=stroke))
    return "\n".join(parts)


def self_box(x, y, w, h, title, body=None, fill="#f4f4f4", stroke="#666"):
    parts = [rect(x, y, w, h, fill=fill, stroke=stroke, rx=10)]
    parts.append(text(x + w / 2, y + 34, title, size=26, weight="bold"))
    if body:
        for idx, line_text in enumerate(body):
            parts.append(text(x + w / 2, y + 70 + idx * 28, line_text, size=22, fill="#333"))
    return "\n".join(parts)


def add_lanes(parts, lanes, lane_top, lane_bottom, header_y):
    for x, label, fill in lanes:
        parts.append(rect(x - 125, header_y - 42, 250, 58, fill=fill, stroke="#777", rx=14))
        parts.append(text(x, header_y + 2, label, size=34, weight="bold"))
        parts.append(line(x, lane_top, x, lane_bottom, stroke="#777", width=2, dash="10,8"))


def base_svg(title, subtitle, height=1180):
    width = 1760
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#faf8f2" />',
        text(width / 2, 64, title, size=46, weight="bold"),
        text(width / 2, 106, subtitle, size=24, fill="#666"),
    ]
    return parts


def build_put_svg():
    parts = base_svg("KV_PUT 写流程泳道图", "客户端生成版本号；接入副本并行转发到多个 owner，本地写入后以 W=2 判定成功")
    lane_top = 170
    lane_bottom = 1110
    header_y = 126
    lanes = [
        (220, "Client", "#f6d365"),
        (630, "接入副本", "#8ecae6"),
        (1040, "副本 owner", "#84a59d"),
        (1480, "协调结果", "#cdb4db"),
    ]
    add_lanes(parts, lanes, lane_top, lane_bottom, header_y)

    client_x, ingress_x, replica_x, result_x = [item[0] for item in lanes]
    y = 230
    step = 132

    parts.append(self_box(client_x - 200, y - 34, 400, 104, "生成 PUT Interest", ["/kua/bucket/keyHex/valueHex", "version/KV_PUT"], fill="#fff4cf", stroke="#c29b1f"))
    y += step
    parts.append(arrow(client_x, y, ingress_x, y, "发送写请求", stroke="#d1495b"))
    y += step
    parts.append(self_box(ingress_x - 215, y - 48, 430, 126, "解析请求并复制", ["遍历 confirmedHosts", "构造 KV_PUT | NO_REPLICATE", "初始化 ackCount / doneCount"], fill="#e6f4fb", stroke="#4c7ea6"))
    y += step
    parts.append(arrow(ingress_x, y, replica_x, y, "并行转发到各副本", stroke="#2a9d8f"))
    y += step
    parts.append(self_box(replica_x - 220, y - 48, 440, 126, "本地 putKv", ["version 新于旧值 -> 覆盖", "旧版本写入 -> 忽略", "返回 OK / IGNORED_OLD_VERSION"], fill="#e0f0eb", stroke="#4f7f76"))
    y += step
    parts.append(arrow(replica_x, y, result_x, y, "ACK / IGNORED", stroke="#8a5cf6"))
    y += step
    parts.append(self_box(result_x - 215, y - 48, 430, 126, "法定人数判断", ["ackCount >= 2 -> OK", "全部结束且不足 2 -> FAILED", "replied 防止重复返回"], fill="#f2e9fb", stroke="#8a5cf6"))
    y += step
    parts.append(arrow(result_x, y, client_x, y, "返回 OK / FAILED", stroke="#2f5bea", label_dy=-24))

    parts.append("</svg>")
    return "\n".join(parts)


def build_get_svg():
    parts = base_svg("KV_GET 读流程泳道图", "接入副本并行读取多个副本；达到 R=2 后选择最高版本，并对滞后副本发起读修复", height=1300)
    lane_top = 170
    lane_bottom = 1230
    header_y = 126
    lanes = [
        (220, "Client", "#f6d365"),
        (630, "接入副本", "#8ecae6"),
        (1040, "副本 owner", "#84a59d"),
        (1480, "一致性决策", "#cdb4db"),
    ]
    add_lanes(parts, lanes, lane_top, lane_bottom, header_y)

    client_x, ingress_x, replica_x, decision_x = [item[0] for item in lanes]
    y = 230
    step = 122

    parts.append(self_box(client_x - 200, y - 34, 400, 104, "生成 GET Interest", ["/kua/bucket/keyHex/KV_GET", "MustBeFresh = true"], fill="#fff4cf", stroke="#c29b1f"))
    y += step
    parts.append(arrow(client_x, y, ingress_x, y, "发送读请求", stroke="#d1495b"))
    y += step
    parts.append(self_box(ingress_x - 215, y - 48, 430, 126, "并行查询副本", ["发送 KV_GET | NO_REPLICATE", "记录 responseCount", "维护 hostVersions 与 best"], fill="#e6f4fb", stroke="#4c7ea6"))
    y += step
    parts.append(arrow(ingress_x, y, replica_x, y, "并行读取", stroke="#2a9d8f"))
    y += step
    parts.append(self_box(replica_x - 220, y - 48, 440, 126, "单副本本地返回", ["命中 -> version\\nvalue", "不存在 -> NOT_FOUND", "不参与副本间协调"], fill="#e0f0eb", stroke="#4f7f76"))
    y += step
    parts.append(arrow(replica_x, y, decision_x, y, "返回 version/value 或 NOT_FOUND", stroke="#8a5cf6", label_dy=-24))
    y += step
    parts.append(self_box(decision_x - 215, y - 48, 430, 126, "选择最新版本", ["responseCount >= 2", "best.version 最大者胜出", "无有效值则返回 NOT_FOUND"], fill="#f2e9fb", stroke="#8a5cf6"))
    y += step
    parts.append(arrow(decision_x, y, replica_x, y, "读修复: KV_PUT | NO_REPLICATE", stroke="#e76f51", dashed=True, label_dy=-24))
    y += step
    parts.append(arrow(decision_x, y + 32, client_x, y + 32, "返回最高版本结果", stroke="#2f5bea", label_dy=-24))

    parts.append("</svg>")
    return "\n".join(parts)


def build_list_svg():
    parts = base_svg("KV_LIST 列表流程泳道图", "接入副本向多个副本收集本地键版本表；达到 R=2 后按 key 合并并保留最高版本")
    lane_top = 170
    lane_bottom = 1110
    header_y = 126
    lanes = [
        (220, "Client", "#f6d365"),
        (630, "接入副本", "#8ecae6"),
        (1040, "副本 owner", "#84a59d"),
        (1480, "合并结果", "#cdb4db"),
    ]
    add_lanes(parts, lanes, lane_top, lane_bottom, header_y)

    client_x, ingress_x, replica_x, merge_x = [item[0] for item in lanes]
    y = 230
    step = 132

    parts.append(self_box(client_x - 200, y - 34, 400, 104, "生成 LIST Interest", ["/kua/bucket/KV_LIST", "按 bucket 分别查询"], fill="#fff4cf", stroke="#c29b1f"))
    y += step
    parts.append(arrow(client_x, y, ingress_x, y, "发送列表请求", stroke="#d1495b"))
    y += step
    parts.append(self_box(ingress_x - 215, y - 48, 430, 126, "并行拉取列表", ["发送 KV_LIST | NO_REPLICATE", "解析 key\\tversion", "维护 merged[key] = max(version)"], fill="#e6f4fb", stroke="#4c7ea6"))
    y += step
    parts.append(arrow(ingress_x, y, replica_x, y, "拉取各副本本地列表", stroke="#2a9d8f"))
    y += step
    parts.append(self_box(replica_x - 220, y - 48, 440, 126, "返回局部键表", ["alpha\\t12", "beta\\t8", "gamma\\t5"], fill="#e0f0eb", stroke="#4f7f76"))
    y += step
    parts.append(arrow(replica_x, y, merge_x, y, "返回 key\\tversion 列表", stroke="#8a5cf6", label_dy=-24))
    y += step
    parts.append(self_box(merge_x - 215, y - 48, 430, 126, "版本合并", ["达到 R=2 后输出", "同 key 保留更高 version", "形成该 bucket 的合并视图"], fill="#f2e9fb", stroke="#8a5cf6"))
    y += step
    parts.append(arrow(merge_x, y, client_x, y, "返回合并后的键表", stroke="#2f5bea", label_dy=-24))

    parts.append("</svg>")
    return "\n".join(parts)


def main():
    OUTPUT_PUT.write_text(build_put_svg(), encoding="utf-8")
    OUTPUT_GET.write_text(build_get_svg(), encoding="utf-8")
    OUTPUT_LIST.write_text(build_list_svg(), encoding="utf-8")
    print(f"wrote {OUTPUT_PUT}")
    print(f"wrote {OUTPUT_GET}")
    print(f"wrote {OUTPUT_LIST}")


if __name__ == "__main__":
    main()
