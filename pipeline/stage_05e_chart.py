import argparse
import io
import logging
import locale
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import seaborn as sns
import plotly.express as px

import config

logger = logging.getLogger(__name__)

# Try to set locale if available on the system
try:
    locale.setlocale(locale.LC_ALL, config.CHART_LOCALE)
except locale.Error:
    logger.warning(f"Locale {config.CHART_LOCALE} not supported by the system. Continuing with default.")


class ChartBuilder:
    """Generates charts from pandas DataFrames.
    
    Usage:
        builder = ChartBuilder()                        # no args
        png_bytes, fig = builder.build(df, query=...)   # df passed here
    
    Legacy usage still works:
        builder = ChartBuilder(df=df, chart_type='bar')
        png_bytes, fig = builder.build()
    """

    def __init__(self, df: Optional[pd.DataFrame] = None, chart_type: str = "auto"):
        self._df = df
        self._default_chart_type = chart_type
        # If df provided at init time, pre-detect type (legacy path)
        if df is not None and chart_type == "auto":
            self.chart_type = self._auto_detect_chart_type(df)
        else:
            self.chart_type = chart_type

    def _auto_detect_chart_type(self, df: pd.DataFrame) -> str:
        """Heuristics to detect the best chart type for the given DataFrame."""
        if df.empty:
            return "none"
        
        columns = df.columns.tolist()
        num_cols = df.select_dtypes(include='number').columns.tolist()
        cat_cols = df.select_dtypes(exclude='number').columns.tolist()

        scatter_keywords_x = ['рост']
        scatter_keywords_y = ['доля']
        scatter_keywords_size = ['продажи', 'доход']
        
        has_x = any(any(kw in c.lower() for kw in scatter_keywords_x) for c in num_cols)
        has_y = any(any(kw in c.lower() for kw in scatter_keywords_y) for c in num_cols)
        has_size = any(any(kw in c.lower() for kw in scatter_keywords_size) for c in num_cols)
        
        if has_x and has_y and has_size and len(num_cols) >= 3:
            return "scatter"

        date_keywords = ['дата', 'период', 'кв ', 'місяць', 'рік', 'месяц', 'год']
        has_date = any(any(kw in c.lower() for kw in date_keywords) for c in columns)
        if has_date and len(num_cols) >= 1:
            return "line"

        if len(cat_cols) >= 1 and len(num_cols) >= 1:
            if len(df) <= 4 and len(num_cols) == 1:
                return "pie"
            return "barh" if len(df) > 6 else "bar"

        if len(num_cols) >= 1:
            return "barh"

        return "table_styled"

    def build(
        self,
        df: Optional[pd.DataFrame] = None,
        query: str = "",
        chart_type: str = "auto",
    ) -> Tuple[Optional[bytes], Optional[Any]]:
        """Builds the chart.

        Args:
            df: DataFrame to chart. If omitted, uses df passed to __init__.
            query: Original user query — used as chart title.
            chart_type: Override chart type. "auto" = detect from data.

        Returns:
            Tuple of (PNG bytes, Plotly Figure). Either can be None
            depending on config.CHART_OUTPUT_FORMAT.
        """
        # Resolve which df to use
        active_df = df if df is not None else self._df
        if active_df is None or active_df.empty:
            return None, None

        # Resolve chart type: call arg > __init__ arg > auto-detect
        if chart_type != "auto":
            self.chart_type = chart_type
        elif self._default_chart_type != "auto":
            self.chart_type = self._default_chart_type
        else:
            self.chart_type = self._auto_detect_chart_type(active_df)

        self._active_df = active_df
        self._query = query

        if self.chart_type in ("none", "table_styled"):
            return None, None

        out_format = config.CHART_OUTPUT_FORMAT.lower()
        png_bytes = None
        html_fig = None

        if out_format in ("png", "both"):
            png_bytes = self._build_matplotlib()

        if out_format in ("html", "both"):
            html_fig = self._build_plotly()

        return png_bytes, html_fig

    def _build_matplotlib(self) -> Optional[bytes]:
        """Generates static matplotlib chart and returns PNG bytes."""
        df = self._active_df
        fig, ax = plt.subplots(figsize=(10, 6), dpi=config.CHART_DPI)
        cmap = plt.get_cmap(config.CHART_COLORMAP)

        num_cols = df.select_dtypes(include='number').columns.tolist()
        cat_cols = df.select_dtypes(exclude='number').columns.tolist()

        if self.chart_type == "bar":
            if cat_cols and num_cols:
                x_col, y_col = cat_cols[0], num_cols[0]
                colors = [cmap(i / max(len(df) - 1, 1)) for i in range(len(df))]
                ax.bar(df[x_col].astype(str), df[y_col], color=colors)
                ax.set_xlabel(x_col)
                ax.set_ylabel(y_col)
                plt.xticks(rotation=45, ha='right')

        elif self.chart_type == "barh":
            if cat_cols and num_cols:
                y_col, x_col = cat_cols[0], num_cols[0]
                colors = [cmap(i / max(len(df) - 1, 1)) for i in range(len(df))]
                ax.barh(df[y_col].astype(str), df[x_col], color=colors)
                ax.set_xlabel(x_col)
                ax.set_ylabel(y_col)

        elif self.chart_type == "line":
            if cat_cols and num_cols:
                x_col, y_col = cat_cols[0], num_cols[0]
                ax.plot(df[x_col].astype(str), df[y_col], marker='o', color=cmap(0.2))
                ax.set_xlabel(x_col)
                ax.set_ylabel(y_col)
                plt.xticks(rotation=45, ha='right')

        elif self.chart_type == "scatter":
            if len(num_cols) >= 3:
                x_col, y_col, size_col = num_cols[0], num_cols[1], num_cols[2]
                scatter = ax.scatter(
                    df[x_col], df[y_col],
                    s=df[size_col] / df[size_col].max() * 1000,
                    alpha=0.6,
                    c=df[size_col],
                    cmap=config.CHART_COLORMAP,
                )
                if cat_cols:
                    for i, txt in enumerate(df[cat_cols[0]]):
                        ax.annotate(txt, (df[x_col].iloc[i], df[y_col].iloc[i]))
                ax.set_xlabel(x_col)
                ax.set_ylabel(y_col)
                fig.colorbar(scatter, ax=ax, label=size_col)

        elif self.chart_type == "pie":
            if cat_cols and num_cols:
                ax.pie(df[num_cols[0]], labels=df[cat_cols[0]],
                       autopct='%1.1f%%', startangle=90,
                       colors=[cmap(i / max(len(df) - 1, 1)) for i in range(len(df))])
                ax.axis('equal')

        # Title from query
        if self._query:
            ax.set_title(self._query, fontsize=12, pad=10)

        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        plt.close(fig)
        return buf.getvalue()

    def _build_plotly(self) -> Optional[Any]:
        """Generates interactive Plotly chart."""
        df = self._active_df
        num_cols = df.select_dtypes(include='number').columns.tolist()
        cat_cols = df.select_dtypes(exclude='number').columns.tolist()

        if not num_cols:
            return None

        title = self._query or ""
        x_col = cat_cols[0] if cat_cols else None
        y_col = num_cols[0]

        if self.chart_type in ("bar", "barh"):
            orientation = 'h' if self.chart_type == "barh" else 'v'
            x_val = y_col if orientation == 'h' else x_col
            y_val = x_col if orientation == 'h' else y_col
            fig = px.bar(df, x=x_val, y=y_val, orientation=orientation,
                         color=x_col, title=title,
                         color_discrete_sequence=px.colors.qualitative.Plotly)
        elif self.chart_type == "line":
            fig = px.line(df, x=x_col, y=y_col, markers=True, title=title)
        elif self.chart_type == "scatter" and len(num_cols) >= 3:
            fig = px.scatter(df, x=num_cols[0], y=num_cols[1], size=num_cols[2],
                             hover_name=x_col, color=x_col, title=title)
        elif self.chart_type == "pie" and x_col:
            fig = px.pie(df, values=y_col, names=x_col, title=title)
        else:
            return None

        return fig


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stage 05e - Chart Builder")
    parser.add_argument("--csv", type=str, required=True, help="Path to input CSV file")
    parser.add_argument("--chart-type", type=str, default="auto", help="Chart type (auto, bar, line, scatter, pie)")
    parser.add_argument("--output", type=str, default="out.png", help="Path to output PNG file")
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, config.LOG_LEVEL), format="%(levelname)s: %(message)s")
    
    try:
        df = pd.read_csv(args.csv)
        builder = ChartBuilder(df=df, chart_type=args.chart_type)
        logger.info(f"Detected chart type: {builder.chart_type}")
        
        png_bytes, _ = builder.build()
        
        if png_bytes:
            with open(args.output, "wb") as f:
                f.write(png_bytes)
            logger.info(f"Saved chart to {args.output}")
        else:
            logger.warning("No chart was generated (possibly none/table fallback).")
            
    except Exception as e:
        logger.error(f"Error building chart: {e}")
