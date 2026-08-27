"""
Reusable chart builders dùng Plotly.
Mỗi function nhận DataFrame đã được clean và trả về go.Figure.
Áp dụng theme constants nhất quán.
"""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

from app import theme


def _apply_base_layout(fig: go.Figure, title: str = "", height: int = 380) -> go.Figure:
    """Áp dụng layout defaults cho mọi chart."""
    fig.update_layout(
        template=theme.CHART_TEMPLATE,
        paper_bgcolor=theme.CHART_PAPER_BG,
        plot_bgcolor=theme.CHART_PLOT_BG,
        font=dict(color=theme.CHART_FONT_COLOR, family="Inter, sans-serif", size=12),
        title=dict(text=title, font=dict(size=14, color=theme.TEXT_PRIMARY)),
        margin=theme.CHART_MARGIN,
        height=height,
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            bordercolor=theme.BORDER,
            font=dict(size=11),
        ),
        xaxis=dict(
            gridcolor=theme.CHART_GRID_COLOR,
            showgrid=True,
            zeroline=False,
        ),
        yaxis=dict(
            gridcolor=theme.CHART_GRID_COLOR,
            showgrid=True,
            zeroline=False,
        ),
    )
    return fig


# ════════════════════════════════════════════════════════════════════════════
# Market Health Charts
# ════════════════════════════════════════════════════════════════════════════

def market_cap_trend(df: pd.DataFrame) -> go.Figure:
    """
    Dual-axis: Total Market Cap (bar) + 24h Change % (line).
    df cần: fetched_at, total_market_cap_usd, market_cap_change_pct_24h
    """
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Bar(
            x=df["fetched_at"],
            y=df["total_market_cap_usd"] / 1e12,
            name="Market Cap (T USD)",
            marker_color=theme.ACCENT_BLUE,
            opacity=0.7,
        ),
        secondary_y=False,
    )

    if "market_cap_change_pct_24h" in df.columns and df["market_cap_change_pct_24h"].notna().any():
        colors_line = [
            theme.POSITIVE_GREEN if v >= 0 else theme.NEGATIVE_RED
            for v in df["market_cap_change_pct_24h"]
        ]
        fig.add_trace(
            go.Scatter(
                x=df["fetched_at"],
                y=df["market_cap_change_pct_24h"],
                name="24h Change %",
                mode="lines+markers",
                line=dict(color=theme.WARN_AMBER, width=2),
                marker=dict(size=6, color=colors_line),
            ),
            secondary_y=True,
        )

    fig.update_layout(
        template=theme.CHART_TEMPLATE,
        paper_bgcolor=theme.CHART_PAPER_BG,
        plot_bgcolor=theme.CHART_PLOT_BG,
        font=dict(color=theme.CHART_FONT_COLOR, family="Inter, sans-serif", size=12),
        title=dict(text="Total Market Cap & 24h Change", font=dict(size=14, color=theme.TEXT_PRIMARY)),
        margin=theme.CHART_MARGIN,
        height=360,
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=11)),
        xaxis=dict(gridcolor=theme.CHART_GRID_COLOR, showgrid=True, zeroline=False),
    )
    fig.update_yaxes(
        title_text="Market Cap (Trillion USD)",
        gridcolor=theme.CHART_GRID_COLOR,
        secondary_y=False,
    )
    fig.update_yaxes(
        title_text="24h Change %",
        gridcolor=theme.CHART_GRID_COLOR,
        secondary_y=True,
    )
    return fig


def dominance_area(df: pd.DataFrame) -> go.Figure:
    """
    Stacked area: BTC dominance vs ETH dominance vs Other (implied).
    df cần: fetched_at, btc_dominance_pct, eth_dominance_pct
    """
    fig = go.Figure()

    other_pct = 100 - df["btc_dominance_pct"] - df["eth_dominance_pct"]

    fig.add_trace(go.Scatter(
        x=df["fetched_at"], y=df["btc_dominance_pct"],
        name="BTC", fill="tozeroy",
        line=dict(color=theme.COIN_COLORS["bitcoin"], width=2),
        fillcolor=f"rgba(247,147,26,0.3)",
        mode="lines",
    ))
    fig.add_trace(go.Scatter(
        x=df["fetched_at"], y=df["eth_dominance_pct"],
        name="ETH", fill="tozeroy",
        line=dict(color=theme.COIN_COLORS["ethereum"], width=2),
        fillcolor=f"rgba(98,126,234,0.3)",
        mode="lines",
    ))
    fig.add_trace(go.Scatter(
        x=df["fetched_at"], y=other_pct,
        name="Others",
        line=dict(color=theme.NEUTRAL_GRAY, width=1.5, dash="dot"),
        mode="lines",
    ))

    _apply_base_layout(fig, "BTC & ETH Market Dominance (%)", height=300)
    fig.update_yaxes(title_text="Dominance %")
    return fig


# ════════════════════════════════════════════════════════════════════════════
# Top Movers Charts
# ════════════════════════════════════════════════════════════════════════════

def gainers_losers_bar(df: pd.DataFrame, pct_col: str = "price_change_percentage_24h",
                        title: str = "Price Change 24h (%)") -> go.Figure:
    """
    Horizontal bar chart cho gainers/losers.
    df cần: coin_id, [pct_col]
    """
    df = df.dropna(subset=[pct_col]).sort_values(pct_col, ascending=True)
    
    colors = [
        theme.POSITIVE_GREEN if v >= 0 else theme.NEGATIVE_RED
        for v in df[pct_col]
    ]
    labels = [theme.COIN_NAMES.get(c, c) for c in df["coin_id"]]

    fig = go.Figure(go.Bar(
        x=df[pct_col],
        y=labels,
        orientation="h",
        marker_color=colors,
        text=[f"{v:+.2f}%" for v in df[pct_col]],
        textposition="outside",
        textfont=dict(color=theme.TEXT_SECONDARY, size=11),
    ))

    _apply_base_layout(fig, title, height=max(300, len(df) * 38))
    fig.update_xaxes(
        title_text="Change %",
        zeroline=True,
        zerolinecolor=theme.BORDER,
        zerolinewidth=1.5,
    )
    return fig


def volume_spike_scatter(df: pd.DataFrame) -> go.Figure:
    """
    Scatter: X = price_change_24h, Y = volume_spike_ratio.
    Phân tích bất thường volume vs. giá.
    df cần: coin_id, price_change_percentage_24h, volume_spike_ratio
    """
    df = df.dropna(subset=["volume_spike_ratio", "price_change_percentage_24h"])
    if df.empty:
        return _empty_fig("Volume spike data sẽ có sau khi pipeline chạy ≥2 ngày")

    colors = [theme.COIN_COLORS.get(c, theme.NEUTRAL_GRAY) for c in df["coin_id"]]
    labels = [theme.COIN_NAMES.get(c, c) for c in df["coin_id"]]

    fig = go.Figure(go.Scatter(
        x=df["price_change_percentage_24h"],
        y=df["volume_spike_ratio"],
        mode="markers+text",
        marker=dict(size=14, color=colors, line=dict(width=1, color=theme.BORDER)),
        text=[theme.COIN_SYMBOLS.get(c, c) for c in df["coin_id"]],
        textposition="top center",
        textfont=dict(size=10, color=theme.TEXT_PRIMARY),
        customdata=list(zip(labels, df["price_change_percentage_24h"], df["volume_spike_ratio"])),
        hovertemplate=(
            "<b>%{customdata[0]}</b><br>"
            "Price Change: %{customdata[1]:+.2f}%<br>"
            "Volume Spike: %{customdata[2]:.2f}x avg<br>"
            "<extra></extra>"
        ),
    ))

    # Reference lines
    fig.add_hline(y=2.0, line_dash="dash", line_color=theme.WARN_AMBER,
                  annotation_text="2x spike threshold", annotation_font_size=10)
    fig.add_vline(x=0, line_color=theme.BORDER, line_width=1)

    _apply_base_layout(fig, "Volume Spike vs. Price Change (24h)", height=400)
    fig.update_xaxes(title_text="Price Change 24h (%)")
    fig.update_yaxes(title_text="Volume / 7-day Avg")
    return fig


# ════════════════════════════════════════════════════════════════════════════
# Coin Deep Dive Charts
# ════════════════════════════════════════════════════════════════════════════

def price_line(df: pd.DataFrame, coin_id: str, use_hourly: bool = False) -> go.Figure:
    """
    Line chart giá USD.
    df cần: [fetched_at hoặc snapshot_date], current_price hoặc close
    """
    x_col = "fetched_at" if use_hourly else "snapshot_date"
    y_col = "current_price" if use_hourly else "close"
    color = theme.COIN_COLORS.get(coin_id, theme.ACCENT_BLUE)
    label = theme.COIN_NAMES.get(coin_id, coin_id)

    fig = go.Figure(go.Scatter(
        x=df[x_col], y=df[y_col],
        mode="lines+markers",
        name=label,
        line=dict(color=color, width=2.5),
        marker=dict(size=5, color=color),
        fill="tozeroy",
        fillcolor=f"rgba({int(color[1:3],16)},{int(color[3:5],16)},{int(color[5:7],16)},0.07)",
        hovertemplate="<b>%{x}</b><br>Price: $%{y:,.4f}<extra></extra>",
    ))

    _apply_base_layout(fig, f"{label} — Price (USD)", height=350)
    fig.update_yaxes(title_text="Price (USD)")
    return fig


def daily_return_bar(df: pd.DataFrame, coin_id: str) -> go.Figure:
    """
    Bar chart daily return (xanh/đỏ).
    df cần: snapshot_date, daily_return
    """
    df = df.dropna(subset=["daily_return"])
    if df.empty:
        return _empty_fig("Cần ≥2 data points để tính daily return")

    colors = [
        theme.POSITIVE_GREEN if v >= 0 else theme.NEGATIVE_RED
        for v in df["daily_return"]
    ]
    label = theme.COIN_NAMES.get(coin_id, coin_id)

    fig = go.Figure(go.Bar(
        x=df["snapshot_date"],
        y=df["daily_return"] * 100,
        marker_color=colors,
        hovertemplate="<b>%{x}</b><br>Daily Return: %{y:+.3f}%<extra></extra>",
    ))

    fig.add_hline(y=0, line_color=theme.BORDER, line_width=1)
    _apply_base_layout(fig, f"{label} — Daily Return (%)", height=260)
    fig.update_yaxes(title_text="Return %")
    return fig


def rolling_return_lines(df: pd.DataFrame, coin_id: str) -> go.Figure:
    """
    Line chart rolling returns 7d/30d/90d.
    df cần: snapshot_date, rolling_return_7d, rolling_return_30d, rolling_return_90d
    """
    label = theme.COIN_NAMES.get(coin_id, coin_id)
    fig = go.Figure()

    series = [
        ("rolling_return_7d",  "7-Day Return",  theme.POSITIVE_GREEN, "solid"),
        ("rolling_return_30d", "30-Day Return", theme.ACCENT_BLUE,    "dot"),
        ("rolling_return_90d", "90-Day Return", theme.WARN_AMBER,     "dash"),
    ]

    any_data = False
    for col, name, color, dash in series:
        sub = df.dropna(subset=[col])
        if not sub.empty:
            any_data = True
            fig.add_trace(go.Scatter(
                x=sub["snapshot_date"],
                y=sub[col] * 100,
                name=name,
                mode="lines+markers",
                line=dict(color=color, width=2, dash=dash),
                marker=dict(size=4),
                hovertemplate=f"<b>%{{x}}</b><br>{name}: %{{y:+.2f}}%<extra></extra>",
            ))

    if not any_data:
        return _empty_fig("Rolling returns sẽ có sau khi pipeline chạy đủ ngày (7/30/90)")

    fig.add_hline(y=0, line_color=theme.BORDER, line_width=1)
    _apply_base_layout(fig, f"{label} — Rolling Returns", height=320)
    fig.update_yaxes(title_text="Return %")
    return fig


def drawdown_area(df: pd.DataFrame, coin_id: str, pipeline_start_date: str = "") -> go.Figure:
    """
    Area chart drawdown (luôn ≤ 0).
    df cần: snapshot_date, drawdown_pct
    """
    label = theme.COIN_NAMES.get(coin_id, coin_id)
    df = df.dropna(subset=["drawdown_pct"])

    fig = go.Figure(go.Scatter(
        x=df["snapshot_date"],
        y=df["drawdown_pct"] * 100,
        mode="lines",
        fill="tozeroy",
        line=dict(color=theme.NEGATIVE_RED, width=2),
        fillcolor="rgba(239,83,80,0.15)",
        name="Drawdown",
        hovertemplate="<b>%{x}</b><br>Drawdown: %{y:.2f}%<extra></extra>",
    ))

    annotation_text = (
        f"⚠️ Drawdown tính từ đỉnh giá đầu pipeline"
        + (f" ({pipeline_start_date})" if pipeline_start_date else "")
        + " — KHÔNG phải ATH lịch sử"
    )
    fig.add_annotation(
        text=annotation_text,
        xref="paper", yref="paper",
        x=0, y=-0.18,
        showarrow=False,
        font=dict(size=10, color=theme.WARN_AMBER),
        align="left",
    )

    _apply_base_layout(fig, f"{label} — Drawdown from Pipeline Peak", height=280)
    fig.update_layout(margin=dict(l=10, r=10, t=40, b=50))
    fig.update_yaxes(title_text="Drawdown %")
    return fig


# ════════════════════════════════════════════════════════════════════════════
# Comparison Charts
# ════════════════════════════════════════════════════════════════════════════

def normalized_price_lines(df: pd.DataFrame, coin_ids: list[str]) -> go.Figure:
    """
    Normalized price chart (base = 100 at start date).
    df cần: snapshot_date, coin_id, normalized_price
    """
    fig = go.Figure()

    for cid in coin_ids:
        sub = df[df["coin_id"] == cid].dropna(subset=["normalized_price"])
        if sub.empty:
            continue
        color = theme.COIN_COLORS.get(cid, theme.NEUTRAL_GRAY)
        label = theme.COIN_SYMBOLS.get(cid, cid)
        fig.add_trace(go.Scatter(
            x=sub["snapshot_date"],
            y=sub["normalized_price"],
            name=label,
            mode="lines+markers",
            line=dict(color=color, width=2),
            marker=dict(size=5),
            hovertemplate=f"<b>%{{x}}</b><br>{label}: %{{y:.1f}}<extra></extra>",
        ))

    fig.add_hline(y=100, line_dash="dash", line_color=theme.NEUTRAL_GRAY,
                  annotation_text="Base (100)", annotation_font_size=10)

    _apply_base_layout(fig, "Normalized Price Performance (Base = 100)", height=380)
    fig.update_yaxes(title_text="Normalized Price")
    return fig


def risk_return_scatter(df: pd.DataFrame) -> go.Figure:
    """
    Risk-Return scatter: X = volatility_30d, Y = rolling_return_30d.
    Bubble size = market_cap_rank (inverted — rank 1 = biggest).
    df cần: coin_id, volatility_30d, rolling_return_30d
    """
    df = df.dropna(subset=["volatility_30d", "rolling_return_30d"])
    if df.empty:
        return _empty_fig("Risk-Return scatter cần ≥30 ngày data (volatility_30d, rolling_return_30d)")

    colors = [theme.COIN_COLORS.get(c, theme.NEUTRAL_GRAY) for c in df["coin_id"]]
    labels = [theme.COIN_NAMES.get(c, c) for c in df["coin_id"]]

    fig = go.Figure(go.Scatter(
        x=df["volatility_30d"] * 100,
        y=df["rolling_return_30d"] * 100,
        mode="markers+text",
        marker=dict(
            size=16,
            color=colors,
            line=dict(width=1.5, color=theme.BORDER),
        ),
        text=[theme.COIN_SYMBOLS.get(c, c) for c in df["coin_id"]],
        textposition="top center",
        textfont=dict(size=10, color=theme.TEXT_PRIMARY),
        customdata=list(zip(labels, df["volatility_30d"] * 100, df["rolling_return_30d"] * 100)),
        hovertemplate=(
            "<b>%{customdata[0]}</b><br>"
            "Volatility 30d: %{customdata[1]:.2f}%<br>"
            "Return 30d: %{customdata[2]:+.2f}%<br>"
            "<extra></extra>"
        ),
    ))

    fig.add_hline(y=0, line_color=theme.BORDER, line_width=1)
    _apply_base_layout(fig, "Risk-Return Map (30-Day)", height=420)
    fig.update_xaxes(title_text="Volatility 30d (daily std %)")
    fig.update_yaxes(title_text="Return 30d (%)")
    return fig


def rolling_return_grouped_bar(df: pd.DataFrame, coin_ids: list[str]) -> go.Figure:
    """
    Grouped bar: coin vs. 7d/30d/90d returns.
    df cần: coin_id, rolling_return_7d, rolling_return_30d, rolling_return_90d
    """
    fig = go.Figure()

    windows = [
        ("rolling_return_7d",  "7-Day",  theme.POSITIVE_GREEN),
        ("rolling_return_30d", "30-Day", theme.ACCENT_BLUE),
        ("rolling_return_90d", "90-Day", theme.WARN_AMBER),
    ]

    filtered = df[df["coin_id"].isin(coin_ids)]
    labels = [theme.COIN_SYMBOLS.get(c, c) for c in filtered["coin_id"]]
    any_bar = False

    for col, name, color in windows:
        sub = filtered.dropna(subset=[col])
        if sub.empty:
            continue
        any_bar = True
        bar_labels = [theme.COIN_SYMBOLS.get(c, c) for c in sub["coin_id"]]
        bar_colors = [
            theme.POSITIVE_GREEN if v >= 0 else theme.NEGATIVE_RED
            for v in sub[col]
        ]
        fig.add_trace(go.Bar(
            name=name,
            x=bar_labels,
            y=sub[col] * 100,
            marker_color=color,
            opacity=0.85,
            hovertemplate=f"<b>%{{x}}</b><br>{name}: %{{y:+.2f}}%<extra></extra>",
        ))

    if not any_bar:
        return _empty_fig("Rolling return data sẽ có sau khi pipeline chạy đủ ngày")

    fig.update_layout(barmode="group")
    fig.add_hline(y=0, line_color=theme.BORDER, line_width=1)
    _apply_base_layout(fig, "Rolling Returns Comparison (7/30/90 Days)", height=360)
    fig.update_yaxes(title_text="Return %")
    return fig


def drawdown_heatmap(df: pd.DataFrame) -> go.Figure:
    """
    Heatmap drawdown: rows = coins, columns = dates.
    df cần: snapshot_date, coin_id, drawdown_pct
    """
    if df.empty:
        return _empty_fig("Chưa có data drawdown")

    pivot = df.pivot(index="coin_id", columns="snapshot_date", values="drawdown_pct")
    pivot = pivot * 100  # convert to %

    coin_labels = [theme.COIN_SYMBOLS.get(c, c) for c in pivot.index]

    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=[str(d) for d in pivot.columns],
        y=coin_labels,
        colorscale=[
            [0.0, theme.NEGATIVE_RED],
            [0.5, theme.WARN_AMBER],
            [1.0, theme.POSITIVE_GREEN],
        ],
        zmid=0,
        zmin=-30, zmax=0,
        colorbar=dict(
            title="Drawdown %",
            tickfont=dict(size=10, color=theme.TEXT_SECONDARY),
        ),
        hovertemplate="<b>%{y}</b><br>%{x}<br>Drawdown: %{z:.1f}%<extra></extra>",
    ))

    _apply_base_layout(fig, "Drawdown Heatmap from Pipeline Peak", height=380)
    fig.update_layout(margin=dict(l=10, r=10, t=40, b=60))
    fig.add_annotation(
        text="⚠️ Peak = giá cao nhất kể từ đầu pipeline, không phải ATH lịch sử",
        xref="paper", yref="paper",
        x=0, y=-0.18, showarrow=False,
        font=dict(size=10, color=theme.WARN_AMBER),
    )
    return fig


def correlation_heatmap(df_wide: pd.DataFrame) -> go.Figure:
    """
    Correlation matrix từ daily returns.
    df_wide: pivot table — index=date, columns=coin_id.
    """
    if df_wide.shape[0] < 2:
        return _empty_fig("Correlation matrix cần ≥2 ngày data (daily returns)")

    corr = df_wide.corr(numeric_only=True)
    labels = [theme.COIN_SYMBOLS.get(c, c) for c in corr.columns]

    fig = go.Figure(go.Heatmap(
        z=corr.values,
        x=labels, y=labels,
        colorscale=[
            [0.0, theme.NEGATIVE_RED],
            [0.5, "#2D3748"],
            [1.0, theme.POSITIVE_GREEN],
        ],
        zmin=-1, zmax=1,
        text=[[f"{v:.2f}" for v in row] for row in corr.values],
        texttemplate="%{text}",
        textfont=dict(size=10),
        colorbar=dict(title="Correlation", tickfont=dict(size=10, color=theme.TEXT_SECONDARY)),
        hovertemplate="<b>%{x} × %{y}</b><br>Correlation: %{z:.3f}<extra></extra>",
    ))

    _apply_base_layout(fig, "Return Correlation Matrix", height=400)
    return fig


# ════════════════════════════════════════════════════════════════════════════
# Utility
# ════════════════════════════════════════════════════════════════════════════

def _empty_fig(message: str = "Chưa có đủ dữ liệu") -> go.Figure:
    """Trả về figure trống với message informative."""
    fig = go.Figure()
    fig.add_annotation(
        text=f"📊 {message}",
        xref="paper", yref="paper",
        x=0.5, y=0.5,
        showarrow=False,
        font=dict(size=13, color=theme.TEXT_SECONDARY),
        align="center",
    )
    _apply_base_layout(fig, "", height=280)
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    return fig
