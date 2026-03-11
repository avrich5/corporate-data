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
    """Generates charts from pandas DataFrames."""

    def __init__(self, df: pd.DataFrame, chart_type: str = "auto"):
        self.df = df
        self.chart_type = chart_type if chart_type != "auto" else self._auto_detect_chart_type()

    def _auto_detect_chart_type(self) -> str:
        """Heuristics to detect the best chart type for the given DataFrame."""
        if self.df.empty:
            return "none"
        
        columns = self.df.columns.tolist()
        num_cols = self.df.select_dtypes(include='number').columns.tolist()
        cat_cols = self.df.select_dtypes(exclude='number').columns.tolist()

        # BCG Scatter check: needs "Рост", "Доля", "Продажи" (or similar semantics)
        # Note: adjust these keywords based on exact schema if needed
        scatter_keywords_x = ['рост']
        scatter_keywords_y = ['доля']
        scatter_keywords_size = ['продажи', 'доход']
        
        has_x = any(any(kw in c.lower() for kw in scatter_keywords_x) for c in num_cols)
        has_y = any(any(kw in c.lower() for kw in scatter_keywords_y) for c in num_cols)
        has_size = any(any(kw in c.lower() for kw in scatter_keywords_size) for c in num_cols)
        
        if has_x and has_y and has_size and len(num_cols) >= 3:
            return "scatter"

        # Line check: if there's a date/time/quarter column
        date_keywords = ['дата', 'период', 'кв ', 'месяц', 'год']
        has_date = any(any(kw in c.lower() for kw in date_keywords) for c in columns)
        if has_date and len(num_cols) >= 1:
            return "line"

        # Bar check: categorical vs numerical
        if len(cat_cols) >= 1 and len(num_cols) >= 1:
            return "bar"

        # Pie check (fallback for single numerical col with < 20 rows)
        if len(num_cols) == 1 and len(self.df) <= 20 and len(cat_cols) == 1:
            return "pie"
            
        # Fallback
        if len(num_cols) == 1:
            return "barh"

        return "table_styled"

    def build(self) -> Tuple[Optional[bytes], Optional[Any]]:
        """Builds the chart based on the format in config.
        
        Returns:
            Tuple containing (PNG bytes if output is png/both, Plotly Figure if html/both).
        """
        if self.chart_type == "none" or self.chart_type == "table_styled":
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
        fig, ax = plt.subplots(figsize=(10, 6), dpi=config.CHART_DPI)
        cmap = plt.get_cmap(config.CHART_COLORMAP)
        
        num_cols = self.df.select_dtypes(include='number').columns.tolist()
        cat_cols = self.df.select_dtypes(exclude='number').columns.tolist()

        if self.chart_type == "bar":
            if cat_cols and num_cols:
                x_col = cat_cols[0]
                y_col = num_cols[0]
                colors = cmap(range(len(self.df)))
                ax.bar(self.df[x_col].astype(str), self.df[y_col], color=colors)
                ax.set_xlabel(x_col)
                ax.set_ylabel(y_col)
                plt.xticks(rotation=45, ha='right')
        
        elif self.chart_type == "barh":
            if cat_cols and num_cols:
                y_col = cat_cols[0]
                x_col = num_cols[0]
                ax.barh(self.df[y_col].astype(str), self.df[x_col], color=cmap(range(len(self.df))))
                ax.set_xlabel(x_col)
                ax.set_ylabel(y_col)

        elif self.chart_type == "line":
            if cat_cols and num_cols:
                x_col = cat_cols[0]
                y_col = num_cols[0]
                ax.plot(self.df[x_col].astype(str), self.df[y_col], marker='o', color=cmap(0))
                ax.set_xlabel(x_col)
                ax.set_ylabel(y_col)
                plt.xticks(rotation=45, ha='right')
                
        elif self.chart_type == "scatter":
            if len(num_cols) >= 3:
                x_col = num_cols[0]
                y_col = num_cols[1]
                size_col = num_cols[2]
                scatter = ax.scatter(
                    self.df[x_col], 
                    self.df[y_col], 
                    s=self.df[size_col] / self.df[size_col].max() * 1000, 
                    alpha=0.6, 
                    c=self.df[size_col],
                    cmap=config.CHART_COLORMAP
                )
                if cat_cols:
                    for i, txt in enumerate(self.df[cat_cols[0]]):
                        ax.annotate(txt, (self.df[x_col].iloc[i], self.df[y_col].iloc[i]))
                ax.set_xlabel(x_col)
                ax.set_ylabel(y_col)
                fig.colorbar(scatter, ax=ax, label=size_col)
                
        elif self.chart_type == "pie":
            if cat_cols and num_cols:
                labels = self.df[cat_cols[0]]
                sizes = self.df[num_cols[0]]
                ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90, colors=cmap(range(len(self.df))))
                ax.axis('equal')

        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        plt.close(fig)
        return buf.getvalue()

    def _build_plotly(self) -> Optional[Any]:
        """Generates interactive Plotly chart."""
        num_cols = self.df.select_dtypes(include='number').columns.tolist()
        cat_cols = self.df.select_dtypes(exclude='number').columns.tolist()
        
        fig = None
        if not cat_cols or not num_cols:
            return None

        x_col = cat_cols[0]
        y_col = num_cols[0]

        if self.chart_type in ("bar", "barh"):
            orientation = 'h' if self.chart_type == "barh" else 'v'
            x_val = y_col if orientation == 'h' else x_col
            y_val = x_col if orientation == 'h' else y_col
            fig = px.bar(self.df, x=x_val, y=y_val, orientation=orientation, color=x_col, color_discrete_sequence=px.colors.qualitative.Plotly)
        elif self.chart_type == "line":
            fig = px.line(self.df, x=x_col, y=y_col, markers=True)
        elif self.chart_type == "scatter":
            if len(num_cols) >= 3:
                fig = px.scatter(
                    self.df, 
                    x=num_cols[0], 
                    y=num_cols[1], 
                    size=num_cols[2], 
                    hover_name=cat_cols[0] if cat_cols else None,
                    color=cat_cols[0] if cat_cols else None
                )
        elif self.chart_type == "pie":
            fig = px.pie(self.df, values=y_col, names=x_col)

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
