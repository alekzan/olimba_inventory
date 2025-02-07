import streamlit as st
import pandas as pd
import gspread
import os
import pytz
import json
from datetime import datetime
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()
# Cargar credenciales de Google desde el archivo .env
service_account_info = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT"))
creds = Credentials.from_service_account_info(
    service_account_info,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
    ],
)

SHEET_ID = os.getenv("SHEET_ID")

# Configurar la zona horaria de México (GMT-6)
mexico_tz = pytz.timezone("America/Mexico_City")


def process_orders(ordenes_file):
    """Procesa el archivo de órdenes y devuelve un DataFrame con las columnas seleccionadas."""
    df = pd.read_excel(ordenes_file)
    df.columns = df.columns.str.strip().str.lower()
    column_mapping = {
        "id del pedido": "ID del pedido",
        "estado": "Estado",
        "sku de la oferta": "SKU de la oferta",
        "información adicional sku": "Información adicional sku",
        "cantidad": "Cantidad",
    }
    df.rename(columns=column_mapping, inplace=True)
    selected_columns = list(column_mapping.values())
    return df[selected_columns].copy()


def filter_and_sum_orders(df):
    """Filtra y agrupa pedidos eliminando los cancelados y sumando cantidades."""
    df_filtered = df[df["Estado"] != "Cancelado"]
    df_grouped = df_filtered.groupby(
        ["SKU de la oferta", "Información adicional sku"], as_index=False
    )["Cantidad"].sum()
    return df_grouped


def update_google_sheet(df):
    """Actualiza la hoja de Google con las cantidades procesadas y registra SKUs no encontrados."""
    # Cargar credenciales desde las variables de entorno (.env)
    service_account_info = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT"))
    creds = Credentials.from_service_account_info(
        service_account_info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
        ],
    )

    # Autenticar con Google Sheets API
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SHEET_ID)
    worksheet = spreadsheet.sheet1
    sheet_data = worksheet.get_all_values()
    headers = sheet_data[0]
    df_sheet = pd.DataFrame(sheet_data[1:], columns=headers)
    df_sheet["SKU_Liverpool"] = df_sheet["SKU_Liverpool"].astype(str)
    df_sheet["PEDIDOS LIVERPOOL"] = (
        pd.to_numeric(df_sheet["PEDIDOS LIVERPOOL"], errors="coerce")
        .fillna(0)
        .astype(int)
    )

    updates = []
    missing_skus = []

    for _, row in df.iterrows():
        sku = row["SKU de la oferta"]
        cantidad = row["Cantidad"]
        match_index = df_sheet[df_sheet["SKU_Liverpool"] == sku].index

        if not match_index.empty:
            row_idx = match_index[0]
            google_sheet_row = row_idx + 2
            new_value = int(df_sheet.at[row_idx, "PEDIDOS LIVERPOOL"] + cantidad)
            updates.append((f"K{google_sheet_row}", [[new_value]]))
        else:
            missing_skus.append(f"SKU: {sku}, Cantidad: {cantidad}\n")

    if updates:
        worksheet.batch_update(
            [{"range": cell, "values": value} for cell, value in updates],
            value_input_option="USER_ENTERED",
        )

    # Guardar SKUs faltantes en un archivo si hay alguno
    if missing_skus:
        with open("missing_skus.txt", "w") as f:
            f.writelines(missing_skus)
        st.warning(
            "Algunos SKUs no fueron encontrados en Google Sheets. Puedes descargar el reporte a continuación."
        )
        with open("missing_skus.txt", "rb") as f:
            st.download_button(
                label="Descargar SKUs no encontrados",
                data=f,
                file_name="missing_skus.txt",
                mime="text/plain",
            )

    st.success("Google Sheet actualizado correctamente!")


def main():
    st.title(
        "📊 Aplicación de Procesamiento de Órdenes y Actualización de Google Sheets"
    )
    st.markdown(
        "📌 Esta aplicación actualiza la siguiente tabla de prueba en Google Sheets. Puedes hacer clic en el enlace para verificar los cambios en tiempo real: [Ver tabla de prueba](https://docs.google.com/spreadsheets/d/1q-voDCxNaHA7kVKRFYZNjrSWoXOjKDYvXWOANuVT6MY/edit?usp=sharing)"
    )
    uploaded_file = st.file_uploader(
        "Sube el archivo de órdenes (Excel)", type=["xlsx"]
    )
    if uploaded_file:
        df_orders = process_orders(uploaded_file)
        df_filtered = filter_and_sum_orders(df_orders)
        now = datetime.now(mexico_tz)
        timestamp = now.strftime("%d-%m-%Y--%H_%M")
        original_filename = uploaded_file.name.rsplit(".", 1)[0]
        processed_filename = f"{original_filename}_procesado_{timestamp}.xlsx"
        df_filtered.to_excel(processed_filename, index=False, engine="openpyxl")
        st.download_button(
            label="Descargar archivo procesado",
            data=open(processed_filename, "rb"),
            file_name=processed_filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        if st.button("Actualizar Google Sheets"):
            update_google_sheet(df_filtered)


if __name__ == "__main__":
    main()
