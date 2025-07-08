from flask import Flask, render_template_string, request, send_file, Response
import pandas as pd
import numpy as np
import io
import re
import os

from sqlalchemy import create_engine
from sqlalchemy import text

# Configuração do Neon (PostgreSQL)
DATABASE_URL = (
    'postgresql://neondb_owner:npg_ebIdH5DfpQ6v@'
    'ep-shy-queen-a85fih1g-pooler.eastus2.azure.neon.tech/neondb'
    '?sslmode=require&channel_binding=require'
)
engine = create_engine(DATABASE_URL)

app = Flask(__name__)

HTML_PAGE = '''
<!doctype html>
<title>Dock, Matera & Depara Upload</title>
<h2>Upload Dock (Excel), Matera (CSV) and Optional Depara (Excel)</h2>
<form method="post" enctype="multipart/form-data">
  <label>Dock (Excel) [select multiple]:</label><br>
  <input type="file" name="dock_files" multiple required><br><br>

  <label>Matera (CSV) [select same number]:</label><br>
  <input type="file" name="matera_files" multiple required><br><br>

  <label>Depara (Excel):</label><br>
  <input type="file" name="depara_file" accept=".xlsx"><br>
  <small>Please ensure the Excel has columns: "Id Conta", "CPF", "Nome", "Produto", "Status Conta", "Data Cadastramento".</small><br><br>

  <input type="submit" value="Process Files">
</form>
'''

EXPECTED_DEPARA_COLS = [
    "Id Conta",
    "CPF",
    "Nome",
    "Produto",
    "Status Conta",
    "Data Cadastramento"
]

@app.route('/', methods=['GET', 'POST'])
def upload_files():
    if request.method == 'POST':
        dock_files = request.files.getlist('dock_files')
        matera_files = request.files.getlist('matera_files')
        depara_file = request.files.get('depara_file')

        # --- Depara ---
        if depara_file:
            try:
                depara_df = pd.read_excel(
                    depara_file,
                    dtype=str,
                    usecols=lambda c: c in EXPECTED_DEPARA_COLS
                ).reindex(columns=EXPECTED_DEPARA_COLS)
                depara_df = depara_df.replace({np.nan: None, np.inf: None, -np.inf: None})

                # Truncate & insert
                with engine.begin() as conn:
                    conn.execute("TRUNCATE TABLE depara;")
                depara_df.to_sql('depara', engine, if_exists='append', index=False)
            except Exception as e:
                return f"<h3>Error upserting Depara: {e}</h3>"

        # --- Dock ---
        dock_list = []
        for f in dock_files:
            try:
                filename = f.filename
                match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
                if not match:
                    return f"<h3>Error: Could not extract date from filename '{filename}'. Expected YYYY-MM-DD.</h3>"
                date_doc = match.group(1)

                df = pd.read_excel(f, sheet_name=0)
                start_idx = df[df['Unnamed: 2'].notna()].index[0]
                df = df.iloc[start_idx:].reset_index(drop=True)
                df.columns = df.iloc[0]
                df = df.iloc[1:].reset_index(drop=True)
                df = df.loc[:, df.columns.notna()]
                df['Valor'] = np.where(
                    df['Id Tipo Transacao'].isin([30224, 30350]),
                    -abs(df['Valor']),
                    abs(df['Valor'])
                )
                df['date_doc'] = date_doc
                dock_list.append(df)
            except Exception as e:
                return f"<h3>Error processing Dock file '{filename}': {e}</h3>"

        # --- Matera ---
        matera_list = []
        for f in matera_files:
            try:
                filename = f.filename
                match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
                if not match:
                    return f"<h3>Error: Could not extract date from Matera filename '{filename}'. Expected YYYY-MM-DD.</h3>"
                date_doc = match.group(1)

                df = pd.read_csv(f, sep=None, engine='python')
                df['nVlrLanc'] = df['nVlrLanc'] \
                    .str.replace(',', '.', regex=False) \
                    .astype('float64')
                df['CPF'] = df['sCpf_Cnpj'] \
                    .astype(str) \
                    .str.replace(r'[.\-]', '', regex=True)
                df.drop(columns=['sCpf_Cnpj'], inplace=True)
                df['nVlrLanc'] = np.where(
                    df['nHistorico'] == 9001,
                    -abs(df['nVlrLanc']),
                    abs(df['nVlrLanc'])
                )
                df['date_doc'] = date_doc
                matera_list.append(df)
            except Exception as e:
                return f"<h3>Error processing Matera file '{filename}': {e}</h3>"

        dock_excel = pd.concat(dock_list, ignore_index=True)
        matera_csv = pd.concat(matera_list, ignore_index=True)

        # --- Insert Dock & Matera ---
        try:
            dock_excel = dock_excel.replace({np.nan: None, np.inf: None, -np.inf: None})
            matera_csv  = matera_csv.replace({np.nan: None, np.inf: None, -np.inf: None})

            with engine.begin() as conn:
                conn.execute(text("TRUNCATE TABLE dock;"))
                conn.execute(text("TRUNCATE TABLE matera;"))

            dock_excel.to_sql('dock', engine, if_exists='append', index=False)
            matera_csv.to_sql('matera', engine, if_exists='append', index=False)

            # Chamadas de funções armazenadas (RPC)
            comp_details = pd.read_sql("SELECT * FROM get_comparison_with_details();", engine)
            comp_by_date  = pd.read_sql("SELECT * FROM get_comparison_by_date_doc();", engine)
            comp_grouped  = pd.read_sql("SELECT * FROM get_comparison_grouped_over_dates();", engine)
            filt_matera   = pd.read_sql("SELECT * FROM get_filtered_matera();", engine)
            filt_dock     = pd.read_sql("SELECT * FROM get_filtered_dock();", engine)
        except Exception as e:
            return f"<h3>Error inserting data: {e}</h3>"

        # --- Monta Excel de saída ---
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            dock_excel.to_excel(writer, sheet_name='Dock', index=False)
            matera_csv.to_excel(writer, sheet_name='Matera', index=False)
            comp_details.to_excel(writer, sheet_name='Diferenças por CPF', index=False)
            comp_by_date.to_excel(writer, sheet_name='Somente os dias com descompasso', index=False)
            comp_grouped.to_excel(writer, sheet_name='Descompasso filtrado', index=False)
            filt_matera.to_excel(writer, sheet_name='Descompasso na Matera', index=False)
            filt_dock.to_excel(writer, sheet_name='Descompasso no Dock', index=False)
        output.seek(0)

        return send_file(
            output,
            download_name="dock_matera.xlsx",
            as_attachment=True
        )

    return render_template_string(HTML_PAGE)

if __name__ == '__main__':
    # port = int(os.environ.get("PORT", 5000))
    # app.run(host='0.0.0.0', port=port)
    app.run(debug=True)

