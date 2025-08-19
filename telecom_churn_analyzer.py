# telecom_churn_analyzer.py

import os
import io
import json
import base64
import textwrap
import requests
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime


class TelecomChurnAnalyzer:
    def __init__(self):
        self.raw_data = None
        self.df = None
        self.results = {}

    # =========================
    # 1. Cargar datos
    # =========================
    def load_data(self, url=None, file_path=None):
        try:
            if url:
                r = requests.get(url)
                r.raise_for_status()
                self.raw_data = pd.json_normalize(r.json())
                print(f"[OK] Datos cargados desde URL ({len(self.raw_data)} registros)")
            elif file_path:
                self.raw_data = pd.read_json(file_path)
                print(f"[OK] Datos cargados desde archivo ({len(self.raw_data)} registros)")
            else:
                raise ValueError("Debes proporcionar una URL o un file_path.")
        except Exception as e:
            print(f"[ERROR] cargando datos: {e}")
            self.raw_data = None

    # =========================
    # 2. Limpiar datos
    # =========================
    def clean_data(self):
        df = self.raw_data.copy()

        # Convierte dict a string si hay columnas anidadas
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, dict)).any():
                df[col] = df[col].astype(str)
                print(f"[INFO] Columna '{col}' contenía diccionarios. Convertida a string.")

        # Elimina duplicados
        before = len(df)
        df.drop_duplicates(inplace=True)
        after = len(df)
        print(f"[OK] Se eliminaron {before - after} duplicados. Quedan {after} registros.")

        # Llena nulos de texto
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].fillna("Desconocido")

        # Llena nulos numéricos
        for col in df.select_dtypes(include=["float", "int"]).columns:
            df[col] = df[col].fillna(0)

        self.df = df
        print("[OK] Datos limpios y listos para análisis.")

    # =========================
    # 3. Crear columna diaria
    # =========================
    def create_daily_column(self, days_per_month=30.42):
        if "MonthlyCharges" in self.df.columns:
            self.df["DailyCharges"] = self.df["MonthlyCharges"] / days_per_month
            print("[OK] Columna 'DailyCharges' creada.")

    # =========================
    # 4. Análisis de datos
    # =========================
    def analyze_data(self):
    # Si existe columna de churn
        if "Churn" not in self.df.columns:
            raise KeyError("No se encontró la columna 'Churn' en los datos.")

        churn_rate = self.df["Churn"].value_counts(normalize=True).to_dict()

        # Buscar columna de cargos
        charge_col = None
        for col in self.df.columns:
            if "charge" in col.lower() or "cargos" in col.lower():
                charge_col = col
                break

        avg_charges = {}
        if charge_col:
            avg_charges = self.df.groupby("Churn")[charge_col].mean().to_dict()
            print(f"[OK] Se usó la columna '{charge_col}' para calcular cargos.")
        else:
            print("[WARN] No se encontró ninguna columna de cargos.")

        self.results = {
            "churn_rate": churn_rate,
            "avg_charges": avg_charges,
            "total_customers": len(self.df)
        }

        print("[OK] Análisis completado.")
        return self.results

    # =========================
    # 5. Graficar
    # =========================
    def plot_churn(self):
        plt.figure(figsize=(6, 4))
        self.df["Churn"].value_counts().plot(kind="bar", color=["green", "red"])
        plt.title("Distribución de Churn")
        plt.xlabel("Churn")
        plt.ylabel("Cantidad de clientes")

        buf = io.BytesIO()
        plt.savefig(buf, format="png")
        plt.close()
        buf.seek(0)
        img_b64 = base64.b64encode(buf.read()).decode("utf-8")
        return img_b64

    # =========================
    # 6. Generar reporte HTML
    # =========================
    def generate_report(self, output_html="informe_churn.html"):
        churn_img = self.plot_churn()

        html = f"""
        <html>
        <head><title>Informe de Churn</title></head>
        <body>
            <h1>Informe de Churn - Telecom</h1>
            <p><b>Total de clientes:</b> {self.results['total_customers']}</p>
            <p><b>Tasa de churn:</b> {self.results['churn_rate']}</p>
            <p><b>Promedio de cargos por churn:</b> {self.results['avg_charges']}</p>
            <h2>Gráfico</h2>
            <img src="data:image/png;base64,{churn_img}" />
        </body>
        </html>
        """

        with open(output_html, "w", encoding="utf-8") as f:
            f.write(html)

        print(f"[OK] Informe generado: {output_html}")

    # =========================
    # 7. Ejecutar todo el pipeline
    # =========================
    def run_and_report(self, url=None, file_path=None, output_html="informe_churn.html", days_per_month=30.42):
        self.load_data(url, file_path)
        self.clean_data()
        print("[DEBUG] Columnas disponibles:", self.df.columns.tolist())
        self.create_daily_column(days_per_month)
        self.analyze_data()
        self.generate_report(output_html)

    # =========================
    # 8. Mostrar primeras filas
    # =========================
    def show_head(self, n=5):
        if self.df is not None:
            print(self.df.head(n))
        else:
            print("[WARN] No hay datos cargados.")


# =========================
# Ejemplo de ejecución directa
# =========================
if __name__ == "__main__":
    analyzer = TelecomChurnAnalyzer()
    analyzer.run_and_report(
        url="https://raw.githubusercontent.com/ingridcristh/challenge2-data-science-LATAM/main/TelecomX_Data.json",
        output_html="informe_churn.html",
        days_per_month=30.42
    )
    analyzer.show_head(8)
