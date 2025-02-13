import streamlit as st
import pandas as pd
import gspread
import pytz
from datetime import datetime
from google.oauth2.service_account import Credentials
import io
import psycopg2  # Using psycopg2 for PostgreSQL


# ---------------------------------------------------------------------------
# Configuration & Authentication
# ---------------------------------------------------------------------------
# For local only
#import os
#from dotenv import load_dotenv

#load_dotenv()
#SHEET_ID = os.getenv("SHEET_ID")
#SERVICE_ACCOUNT_FILE = "/Users/alex/Projects/Olimba/automatization_olimba/Gus files/inventory_app/json_google/spreadsheet-demo-for-hr-9cf643c81c21.json"
#SCOPES = [
#    "https://www.googleapis.com/auth/spreadsheets",
#    "https://www.googleapis.com/auth/drive",
#]
#creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# For Streamlit Cloud deployment, you can use:
SHEET_ID = st.secrets["SHEET_ID"]
GOOGLE_SERVICE_ACCOUNT = st.secrets["GOOGLE_SERVICE_ACCOUNT"]
creds = Credentials.from_service_account_info(
   GOOGLE_SERVICE_ACCOUNT,
   scopes=[
       "https://www.googleapis.com/auth/spreadsheets",
       "https://www.googleapis.com/auth/drive.file",
   ],
)

mexico_tz = pytz.timezone("America/Mexico_City")

# ---------------------------------------------------------------------------
# PostgreSQL Helper Functions (using Neon)
# ---------------------------------------------------------------------------
def get_connection():
    try:
        return psycopg2.connect(os.getenv("DATABASE_URL"))
    except Exception:
        return psycopg2.connect(st.secrets["DATABASE_URL"])

def init_db():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_orders (
            order_id TEXT PRIMARY KEY
        );
        """
    )
    conn.commit()
    cur.close()
    conn.close()

def get_processed_order_ids():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT order_id FROM processed_orders;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return set([row[0] for row in rows])

def add_new_order_ids(order_ids):
    conn = get_connection()
    cur = conn.cursor()
    for oid in order_ids:
        try:
            cur.execute("INSERT INTO processed_orders (order_id) VALUES (%s) ON CONFLICT DO NOTHING;", (oid,))
        except Exception:
            pass
    conn.commit()
    cur.close()
    conn.close()

def filter_new_orders(df):
    processed_ids = get_processed_order_ids()
    return df[~df["ID del pedido"].astype(str).isin(processed_ids)]

# ---------------------------------------------------------------------------
# Data Processing & Google Sheet Functions
# ---------------------------------------------------------------------------
def process_orders(ordenes_file):
    df = pd.read_excel(ordenes_file)
    df.columns = df.columns.str.strip().str.lower()
    mapping = {
        "id del pedido": "ID del pedido",
        "estado": "Estado",
        "sku de la oferta": "SKU de la oferta",
        "no de sku": "No de SKU",
        "información adicional sku": "Información adicional sku",
        "cantidad": "Cantidad",
    }
    df.rename(columns=mapping, inplace=True)
    return df[list(mapping.values())].copy()

def filter_and_sum_orders(df):
    df_filtered = df[df["Estado"] != "Cancelado"]
    if "SKU de la oferta" in df_filtered.columns:
        group_cols = ["No de SKU", "SKU de la oferta", "Información adicional sku"]
    else:
        group_cols = ["No de SKU", "Información adicional sku"]
    df_grouped = df_filtered.groupby(group_cols, as_index=False)["Cantidad"].sum()
    if "SKU de la oferta" not in df_grouped.columns:
        df_grouped["SKU de la oferta"] = df_grouped["No de SKU"]
    return df_grouped[["No de SKU", "SKU de la oferta", "Información adicional sku", "Cantidad"]]

def update_google_sheet(df):
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SHEET_ID)
    worksheet = spreadsheet.sheet1
    sheet_data = worksheet.get_all_values()
    headers = sheet_data[0]
    df_sheet = pd.DataFrame(sheet_data[1:], columns=headers)
    df_sheet["SKU_Liverpool"] = df_sheet["SKU_Liverpool"].astype(str).str.strip().str.lower()
    df_sheet["PEDIDOS LIVERPOOL"] = pd.to_numeric(df_sheet["PEDIDOS LIVERPOOL"], errors="coerce").fillna(0).astype(int)

    updates = []
    missing_rows = []
    now = datetime.now(mexico_tz)
    timestamp = now.strftime("%d-%m-%Y_%H-%M")
    df_grouped = df.groupby(["No de SKU", "SKU de la oferta"], as_index=False)["Cantidad"].sum()

    for _, row in df_grouped.iterrows():
        sku_no = str(row["No de SKU"]).strip().lower()
        sku_oferta = row["SKU de la oferta"]
        cantidad = row["Cantidad"]
        match_index = df_sheet[df_sheet["SKU_Liverpool"] == sku_no].index
        if not match_index.empty:
            row_idx = match_index[0]
            google_sheet_row = row_idx + 2  # account for header row
            new_value = int(df_sheet.at[row_idx, "PEDIDOS LIVERPOOL"] + cantidad)
            updates.append((f"K{google_sheet_row}", [[new_value]]))
        else:
            missing_rows.append({
                "No de SKU": sku_no,
                "SKU de la oferta": sku_oferta,
                "Cantidad": cantidad,
            })

    if updates:
        worksheet.batch_update(
            [{"range": cell, "values": value} for cell, value in updates],
            value_input_option="USER_ENTERED",
        )

    missing_file = None
    if missing_rows:
        st.warning("Algunos SKUs no fueron encontrados en Google Sheets. Puedes descargar el reporte a continuación.")
        missing_df = pd.DataFrame(missing_rows)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            workbook = writer.book
            ws_xl = workbook.add_worksheet("Missing SKUs")
            writer.sheets["Missing SKUs"] = ws_xl
            ws_xl.write("A1", f"Reporte de SKUs no encontrados - Fecha: {timestamp}")
            missing_df.to_excel(writer, sheet_name="Missing SKUs", startrow=2, index=False)
        output.seek(0)
        missing_file = output

    st.success("Google Sheet actualizado correctamente!")
    return missing_file, timestamp

def add_inventory_to_processed_file(df):
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SHEET_ID)
    worksheet = spreadsheet.sheet1
    sheet_data = worksheet.get_all_values()
    headers = sheet_data[0]
    df_sheet = pd.DataFrame(sheet_data[1:], columns=headers)
    df_sheet["SKU_Liverpool"] = df_sheet["SKU_Liverpool"].astype(str).str.strip().str.lower()
    df_sheet["TOTAL PZAS INVENTARIO"] = pd.to_numeric(df_sheet["TOTAL PZAS INVENTARIO"], errors="coerce").fillna(0).astype(int)
    df["No_de_SKU_norm"] = df["No de SKU"].astype(str).str.strip().str.lower()
    df_merged = pd.merge(
        df,
        df_sheet[["SKU_Liverpool", "TOTAL PZAS INVENTARIO"]],
        left_on="No_de_SKU_norm",
        right_on="SKU_Liverpool",
        how="left",
    )
    df_merged.drop(columns=["No_de_SKU_norm", "SKU_Liverpool"], inplace=True)
    timestamp_inventory = datetime.now(mexico_tz).strftime("%d/%m/%Y, %H:%M")
    new_col_name = f"Inventario al día {timestamp_inventory}"
    df_merged.rename(columns={"TOTAL PZAS INVENTARIO": new_col_name}, inplace=True)
    return df_merged[["No de SKU", "SKU de la oferta", "Información adicional sku", new_col_name, "Cantidad"]]

# ---------------------------------------------------------------------------
# Main App
# ---------------------------------------------------------------------------
def main():
    init_db()

    # Initialize session state variables (only once)
    if "file_data" not in st.session_state:
        st.session_state.file_data = None
        st.session_state.df_orders = None
        st.session_state.df_new = None
        st.session_state.df_filtered_all = None
        st.session_state.df_processed = None
        st.session_state.file_name = None
        st.session_state.processed = False
        st.session_state.processing_update = False
        st.session_state.success_message = ""
        st.session_state.missing_file = None
        st.session_state.missing_timestamp = None

    st.title("📊 Procesamiento de Órdenes y Actualización de Google Sheets")
    st.markdown("📌 Esta aplicación actualiza una tabla de Google Sheets. Revisa los cambios en tiempo real.")

    uploaded_file = st.file_uploader("Sube el archivo de órdenes (Excel)", type=["xlsx"])

    if uploaded_file:
        # Process the file only once per upload.
        file_bytes = uploaded_file.getvalue()
        if st.session_state.file_data != file_bytes:
            st.session_state.file_data = file_bytes
            with st.spinner("Procesando archivo..."):
                df_orders = process_orders(uploaded_file)
                df_new = filter_new_orders(df_orders)
                df_filtered_all = filter_and_sum_orders(df_orders)
                df_processed = add_inventory_to_processed_file(df_filtered_all)
            st.session_state.df_orders = df_orders
            st.session_state.df_new = df_new
            st.session_state.df_filtered_all = df_filtered_all
            st.session_state.df_processed = df_processed
            st.session_state.file_name = uploaded_file.name.rsplit(".", 1)[0]
            st.session_state.processed = True

        # Offer download of the processed file
        if st.session_state.processed:
            now = datetime.now(mexico_tz)
            timestamp = now.strftime("%d-%m-%Y--%H_%M")
            processed_filename = f"{st.session_state.file_name}_procesado_{timestamp}.xlsx"
            output = io.BytesIO()
            st.session_state.df_processed.to_excel(output, index=False, engine="openpyxl")
            output.seek(0)
            st.download_button(
                label="Descargar archivo procesado",
                data=output,
                file_name=processed_filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        # If there are new orders, show the update button.
        if st.session_state.df_new is not None and not st.session_state.df_new.empty:
            if st.button("Actualizar Google Sheets", disabled=st.session_state.processing_update):
                st.session_state.processing_update = True
                # --- Step 1: Update Google Sheets ---
                with st.spinner("Actualizando Google Sheets..."):
                    df_filtered_new = filter_and_sum_orders(st.session_state.df_new)
                    missing_file, missing_timestamp = update_google_sheet(df_filtered_new)
                    st.session_state.missing_file = missing_file
                    st.session_state.missing_timestamp = missing_timestamp
                # --- Step 2: Update Neon DB ---
                with st.spinner("Actualizando base de datos, NO CERRAR"):
                    new_order_ids = st.session_state.df_new["ID del pedido"].astype(str).unique()
                    add_new_order_ids(new_order_ids)
                st.session_state.success_message = "Inventario actualizado y nuevos ID de pedido registrados en la base de datos."
                st.session_state.processing_update = False

        # If there are no new orders, show an informational alert.
        elif st.session_state.df_new is not None and st.session_state.df_new.empty:
            st.info("No hay nuevos pedidos para procesar (todos ya han sido procesados anteriormente).")

        # Show download button for missing SKUs file (if available) outside the update spinners.
        if st.session_state.missing_file is not None:
            st.download_button(
                label="Descargar SKUs no encontrados",
                data=st.session_state.missing_file,
                file_name=f"missing_skus_{st.session_state.missing_timestamp}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="missing_skus_download"
            )

        # Display success message and inform the user that to add a new file, they need to refresh the page.
        if st.session_state.success_message:
            st.success(st.session_state.success_message)
            st.info("El proceso ha finalizado. Si desea agregar un nuevo archivo, por favor refresque la página.")

if __name__ == "__main__":
    main()
