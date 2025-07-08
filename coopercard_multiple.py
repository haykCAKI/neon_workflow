from flask import Flask, render_template_string, request, send_file
import requests
import pandas as pd
import numpy as np
import io
import re
import os

from supabase import create_client, Client

# SUPABASE_URL = "https://ceipbytvskpncdlfyvup.supabase.co"
# SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImNlaXBieXR2c2twbmNkbGZ5dnVwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTE2NjE0NTUsImV4cCI6MjA2NzIzNzQ1NX0.-JxLJsV5Iofn84vs7XU3qcML1Tzu0X4KlKzqHo4cDcI"
SUPABASE_URL = "https://cbgqqfyxgzruazfyvtbb.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImNiZ3FxZnl4Z3pydWF6Znl2dGJiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTE4OTQ3NDYsImV4cCI6MjA2NzQ3MDc0Nn0.163hQ5VF5DkSuouIj9qqSG6hWft72U5rlBQ3m6WmML8"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

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
  <small>Please ensure the Excel has columns: \"Id Conta\", \"CPF\", \"Nome\", \"Produto\", \"Status Conta\", \"Data Cadastramento\".</small><br><br>

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

# URL do seu webhook n8n (copie do nó Webhook → Path = coopercard/submit)
N8N_WEBHOOK = os.environ.get("N8N_WEBHOOK") or "https://hericktaticca.app.n8n.cloud/webhook-test/coopercard/submit"

@app.route('/', methods=['GET', 'POST'])
def upload_files():
    if request.method == 'POST':
        # Retrieve lists of uploaded files
        dock_files = request.files.getlist('dock_files')
        matera_files = request.files.getlist('matera_files')
        depara_file = request.files.get('depara_file')

        # Validate equal count
        if len(dock_files) != len(matera_files):
            return "<h3>Error: You must upload the same number of Dock and Matera files.</h3>"
        
         # Monta o multipart/form-data para o n8n
        files = []
        for f in dock_files:
            files.append(('dock_files', (f.filename, f.stream, f.mimetype)))
        for f in matera_files:
            files.append(('matera_files', (f.filename, f.stream, f.mimetype)))

        # POST para o n8n
        resp = requests.post(N8N_WEBHOOK, files=files)
        if not resp.ok:
            return f"<h3>Erro no Webhook n8n: {resp.status_code} {resp.text}</h3>", 500

        # Optional upsert Depara
        if depara_file:
            try:
                depara_df = pd.read_excel(
                    depara_file,
                    dtype=str,
                    usecols=lambda c: c in EXPECTED_DEPARA_COLS
                )
                for col in EXPECTED_DEPARA_COLS:
                    if col not in depara_df.columns:
                        depara_df[col] = None
                depara_df = depara_df.replace({np.nan: None, np.inf: None, -np.inf: None})
                depara_records = depara_df.to_dict(orient='records')
                supabase.table('depara').upsert(depara_records).execute()
            except Exception as e:
                return f"<h3>Error upserting Depara: {e}</h3>"

        # Process and concatenate Dock files
        dock_list = []
        for f in dock_files:
            try:
                filename = f.filename
                match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
                if not match:
                    return f"<h3>Error: Could not extract date from filename '{filename}'. Expected YYYY-MM-DD.</h3>"
                date_doc = match.group(1)

                df = pd.read_excel(f, sheet_name=0)
                # find first "useful" row by non-null in column 2
                start_idx = df[df['Unnamed: 2'].notna()].index[0]
                df = df.iloc[start_idx:].reset_index(drop=True)
                # promote first row to header
                df.columns = df.iloc[0]
                df = df.iloc[1:].reset_index(drop=True)
                # drop columns with NaN headers
                df = df.loc[:, df.columns.notna()]
                # apply sign logic
                df['Valor'] = np.where(
                    df['Id Tipo Transacao'].isin([30224, 30350]),
                    -abs(df['Valor']),
                    abs(df['Valor'])
                )
                df['date_doc'] = date_doc
                dock_list.append(df)
            except Exception as e:
                return f"<h3>Error processing Dock file '{filename}': {e}</h3>"

        # Process and concatenate Matera files
        matera_list = []
        for f in matera_files:
            try:
                # Extract matching date from corresponding dock filename for consistency
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

        # Concatenate all
        dock_excel = pd.concat(dock_list, ignore_index=True)
        matera_csv = pd.concat(matera_list, ignore_index=True)

        # Upsert tables in Supabase
        try:
            # clear existing data
            supabase.table('dock').delete().gte('primary', '00000000-0000-0000-0000-000000000000').execute()
            supabase.table('matera').delete().gte('primary', '00000000-0000-0000-0000-000000000000').execute()

            # sanitize for JSON
            dock_excel = dock_excel.replace({np.nan: None, np.inf: None, -np.inf: None})
            matera_csv = matera_csv.replace({np.nan: None, np.inf: None, -np.inf: None})

            supabase.table('dock').insert(dock_excel.to_dict(orient='records')).execute()
            supabase.table('matera').insert(matera_csv.to_dict(orient='records')).execute()
            supabase.rpc("update_dock_with_cpf").execute()

            comp_details = pd.DataFrame(supabase.rpc("get_comparison_with_details").execute().data)
            comp_by_date = pd.DataFrame(supabase.rpc("get_comparison_by_date_doc").execute().data)
            comp_grouped = pd.DataFrame(supabase.rpc("get_comparison_grouped_over_dates").execute().data)
            filt_matera = pd.DataFrame(supabase.rpc("get_filtered_matera").execute().data)
            filt_dock = pd.DataFrame(supabase.rpc("get_filtered_dock").execute().data)
        except Exception as e:
            return f"<h3>Error inserting data into Supabase: {e}</h3>"

        # Build output Excel
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

# if __name__ == '__main__':
#     app.run(debug=True)
if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))  # usa a porta definida pelo Render, ou 5000 localmente
    app.run(host='0.0.0.0', port=port)