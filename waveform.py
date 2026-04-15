"""
热度波形 SVG 生成 — 弹幕密度可视化
类似心电图的交互式波形，颜色从绿(低)→黄→红(高)
"""


def generate_waveform_svg(timeline: list[dict], width: int = 800, height: int = 120,
                           bar_gap: int = 1) -> str:
    """从弹幕密度时间线生成 SVG 波形图

    Args:
        timeline: [{t, density, count}, ...]
        width: SVG 宽度
        height: SVG 高度
    Returns:
        SVG 字符串
    """
    if not timeline:
        return _empty_svg(width, height)

    max_density = max((p.get("density", 0) for p in timeline), default=1) or 1
    n = len(timeline)
    bar_width = max(1, (width - bar_gap * n) / n)

    # SVG 头部
    svg_parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
        f'width="100%" height="{height}" style="border-radius:8px;background:#1a1a2e">',
        '<defs>',
        '  <linearGradient id="wg" x1="0" y1="1" x2="0" y2="0">',
        '    <stop offset="0%" stop-color="#00b894"/>',
        '    <stop offset="50%" stop-color="#fdcb6e"/>',
        '    <stop offset="100%" stop-color="#e17055"/>',
        '  </linearGradient>',
        '</defs>',
    ]

    # 绘制柱状波形
    for i, point in enumerate(timeline):
        density = point.get("density", 0)
        ratio = density / max_density if max_density > 0 else 0
        bar_h = max(2, ratio * (height - 10))
        x = i * (bar_width + bar_gap)
        y = height - bar_h - 2

        # 颜色根据强度变化
        color = _density_color(ratio)

        svg_parts.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_width:.1f}" height="{bar_h:.1f}" '
            f'rx="1" fill="{color}" opacity="0.85">'
            f'<title>{point.get("t", 0)}s: {density:.1f}/s ({point.get("count", 0)}条)</title>'
            f'</rect>'
        )

    # 平滑曲线叠加（使用 path）
    if n > 2:
        points = []
        for i, point in enumerate(timeline):
            density = point.get("density", 0)
            ratio = density / max_density if max_density > 0 else 0
            x = i * (bar_width + bar_gap) + bar_width / 2
            y = height - 2 - ratio * (height - 10)
            points.append((x, y))

        path_d = _smooth_path(points)
        svg_parts.append(
            f'<path d="{path_d}" fill="none" stroke="url(#wg)" '
            f'stroke-width="2" stroke-linecap="round" opacity="0.9"/>'
        )

    svg_parts.append('</svg>')
    return "\n".join(svg_parts)


def _density_color(ratio: float) -> str:
    """根据密度比例返回颜色"""
    if ratio < 0.3:
        return "#00b894"  # 绿
    elif ratio < 0.6:
        return "#fdcb6e"  # 黄
    elif ratio < 0.8:
        return "#e17055"  # 橙
    else:
        return "#d63031"  # 红


def _smooth_path(points: list[tuple[float, float]]) -> str:
    """生成平滑贝塞尔曲线路径"""
    if len(points) < 2:
        return ""
    parts = [f"M {points[0][0]:.1f} {points[0][1]:.1f}"]
    for i in range(1, len(points)):
        x0, y0 = points[i - 1]
        x1, y1 = points[i]
        cx = (x0 + x1) / 2
        parts.append(f"C {cx:.1f} {y0:.1f}, {cx:.1f} {y1:.1f}, {x1:.1f} {y1:.1f}")
    return " ".join(parts)


def _empty_svg(width: int, height: int) -> str:
    """空状态 SVG"""
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
        f'width="100%" height="{height}" style="border-radius:8px;background:#1a1a2e">'
        f'<text x="{width/2}" y="{height/2}" text-anchor="middle" '
        f'fill="#666" font-size="14">暂无弹幕数据</text>'
        f'</svg>'
    )
