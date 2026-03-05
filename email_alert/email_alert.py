import pandas as pd
import smtplib
import os
import matplotlib.pyplot as plt
from email.message import EmailMessage
from email.utils import formataddr

# --- CONFIGURATION ---
BASE_DIR = "/Users/206908417/Documents/Dq-Airflow-Orchestration/Data_quality"
CSV_PATH = os.path.join(BASE_DIR, "mock_data_udx_dq.csv")
SNAPSHOT_PATH = os.path.join(BASE_DIR, "dq_snapshot.png")

SMTP_EMAIL = "devakipriyavenkatesan@gmail.com"
SMTP_PASSWORD = "namoknasygrggfrs" 
TO_EMAIL = "c-devaki.priyav@udx.com"

def create_snapshot(df):
    try:
        if df.empty:
            print("Dataframe is empty, skipping snapshot.")
            return False
        
        # Ensure the directory exists before saving
        os.makedirs(os.path.dirname(SNAPSHOT_PATH), exist_ok=True)

        df = df.sort_values("FAILURE_PERCENTAGE", ascending=False).head(6).copy()
        df["TABLE_CLEAN"] = df["TABLE_NAME"].str.replace("UDX_", "", case=False)
        df["RULE_LABEL"] = df["RULE_ID"].astype(str) + " | " + df["TABLE_CLEAN"] + "\n(" + df["COLUMN_NAME"] + ")"

        fig_width_px, dpi = 750, 110
        fig = plt.figure(figsize=(fig_width_px/dpi, 480/dpi), dpi=dpi)
        ax = fig.add_axes([0.3, 0.15, 0.6, 0.7]) # Adjusted margins for labels
        
        colors = ["#d32f2f" if v >= 50 else "#f57c00" for v in df["FAILURE_PERCENTAGE"]]
        ax.barh(df["RULE_LABEL"], df["FAILURE_PERCENTAGE"], color=colors)
        
        # Save and verify
        plt.savefig(SNAPSHOT_PATH, dpi=dpi, bbox_inches="tight")
        plt.close()
        
        if os.path.exists(SNAPSHOT_PATH):
            print(f"Snapshot created at: {SNAPSHOT_PATH}")
            return True
        else:
            print("plt.savefig failed to create file.")
            return False
    except Exception as e:
        print(f" Error in create_snapshot: {e}")
        return False

def process_csv_and_email():
    if not os.path.exists(CSV_PATH):
        print(f" CSV File not found: {CSV_PATH}")
        return

    df = pd.read_csv(CSV_PATH)
    df['SEVERITY'] = df['SEVERITY'].astype(str).str.upper().str.strip()
    critical_df = df[(df['IS_THRESHOLD_BREACHED'] == 1) & (df['SEVERITY'] == 'HIGH')].copy()

    if critical_df.empty:
        print(" No critical breaches found.")
        return

    if create_snapshot(critical_df):
        msg = EmailMessage()
        msg['Subject'] = f"🚨 DQ ALERT: {len(critical_df)} Critical Issues"
        msg['From'] = formataddr(("UDX Operations", SMTP_EMAIL))
        msg['To'] = TO_EMAIL
        
        # HTML Content with Embedded Image Reference
        html_content = f"""
        <html>
            <body>
                <h2 style="color: #b71c1c;">Critical Data Quality Breach</h2>
                <p>Top issues summarized below:</p>
                <img src="cid:dq_snapshot" style="max-width: 100%; border: 1px solid #ddd;">
            </body>
        </html>
        """
        msg.set_content("Please view the HTML version for the report.")
        msg.add_alternative(html_content, subtype='html')

        # Critical for visibility: Attach the image properly to the HTML part
        with open(SNAPSHOT_PATH, 'rb') as f:
            msg.get_payload()[1].add_related(
                f.read(), 
                maintype='image', 
                subtype='png', 
                cid='dq_snapshot' # Matches the <img src="cid:dq_snapshot">
            )

        try:
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
                server.login(SMTP_EMAIL, SMTP_PASSWORD)
                server.send_message(msg)
            print("Alert successfully sent with embedded snapshot.")
        except Exception as e:
            print(f"SMTP Error: {e}")

if __name__ == "__main__":
    process_csv_and_email()