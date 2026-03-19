import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy as sa
from datetime import datetime, timedelta
import os
import pickle
import re
import requests
import sys

# --- CONFIGURATION ---
# PASTE YOUR POWER AUTOMATE WEBHOOK URL HERE
POWER_AUTOMATE_WEBHOOK_URL = "https://default9026c5f486d04b9fbd39b7d4d0fb46.74.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/3bb41d6aa7954f228ccebfd7f65a8448/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=tut4cDCfJzP0vzx-dOnegx5mk9_JOeONQUkXIx7pCdQ"

# Define database connection parameters
db_config = {
    'server': 'EDWPRDRPTDB.teslamotors.com',
    'port': '1433',
    'database': 'master',
    'driver': "ODBC Driver 17 for SQL Server"
}

connection_string = f"mssql+pyodbc://{db_config['server']}:{db_config['port']}/{db_config['database']}?driver={db_config['driver'].replace(' ', '+')}"

# Define a specific location for snapshots - CHANGE THIS PATH to a location you have access to
SNAPSHOT_DIR = r"C:\Users\jexue\Projects\OneDrive_2025-09-22\Offline Repository\snapshots"

sql_query_combined = """
WITH ModelMapping AS (
    SELECT 1 AS ModelID, 'Model S' AS ModelName
    UNION ALL SELECT 2 AS ModelID, 'Model X' AS ModelName
    UNION ALL SELECT 3 AS ModelID, 'Roadster' AS ModelName
    UNION ALL SELECT 7 AS ModelID, 'Model 3' AS ModelName
    UNION ALL SELECT 17 AS ModelID, 'Model Y' AS ModelName
    UNION ALL SELECT 18 AS ModelID, 'Model S Palladium' AS ModelName
    UNION ALL SELECT 19 AS ModelID, 'Model X Palladium' AS ModelName
    UNION ALL SELECT 20 AS ModelID, 'Semi' AS ModelName
    UNION ALL SELECT 35 AS ModelID, '2024+ Model 3' AS ModelName
    UNION ALL SELECT 36 AS ModelID, '2025+ Model Y' AS ModelName
    UNION ALL SELECT 103 AS ModelID, 'Cybertruck' AS ModelName
),
ProductAffectedModels AS (
    SELECT dc.Reference_ID,
        LTRIM(RTRIM(value)) AS ModelName
    FROM service.EDW.DimCampaign dc
    CROSS APPLY STRING_SPLIT(dc.Product_Affected, ',')
    UNION ALL
    SELECT dc.Reference_ID,
        mm.ModelName
    FROM service.EDW.DimCampaign dc
    CROSS JOIN ModelMapping mm
    WHERE dc.Product_Affected IS NULL OR dc.Product_Affected = ''
),
CampaignModels AS (
    SELECT
        Reference_ID,
        STRING_AGG(ModelName, ', ') AS AffectedModels
    FROM ProductAffectedModels
    GROUP BY Reference_ID
),
CampaignFRT AS (
    SELECT
        DistinctFRT.Reference_ID,
        STRING_AGG(
            CASE
                WHEN DistinctFRT.FRT_Hours = 0.00 THEN 'Diagnosis FRT (0)'
                ELSE CAST(DistinctFRT.FRT_Hours AS NVARCHAR)
            END, ', ') AS FRT_List,
        MAX(DistinctFRT.FRT_Hours) AS Max_FRT
    FROM (
        SELECT DISTINCT
            dccc.Reference_ID,
            dsccm.FRT_Hours
        FROM service.EDW.DimCampaignCorrectionCode dccc WITH (NOLOCK)
        LEFT JOIN service.EDW.DimServiceCorrection dsc WITH (NOLOCK)
            ON dsc.Service_Correction_Code = dccc.Correction_Code_Number
        LEFT JOIN service.EDW.DimServiceCorrectionCodeModel dsccm WITH (NOLOCK)
            ON dsc.Service_Correction_Key = dsccm.Service_Correction_Key
        LEFT JOIN ModelMapping mm
            ON dsccm.Model_ID = mm.ModelID
        INNER JOIN ProductAffectedModels pam
            ON dccc.Reference_ID = pam.Reference_ID AND pam.ModelName = mm.ModelName
        WHERE dccc.Is_Active = 1
        AND dsc.Is_Active = 1
        AND dsccm.Is_Active = 1
    ) AS DistinctFRT
    GROUP BY DistinctFRT.Reference_ID
),
RelevantCampaigns AS (
    SELECT
        dc.Reference_ID,
        dc.Campaign_ID,
        dc.Concern_type,
        dc.Action_type,
        dc.Campaign_Manager_Name,
        dc.Campaign_Title,
        dc.Source_Activated_Date,
        CASE
            WHEN dc.Source_Activated_Date >= DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)
            AND dc.Source_Activated_Date < DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()) + 1, 0)
            THEN 'This Week'
            ELSE 'Active High FRT'
        END as Campaign_Category,
        cf.FRT_List,
        ISNULL(cf.Max_FRT, 0) as Max_FRT,
        cm.AffectedModels
    FROM service.EDW.DimCampaign dc (NOLOCK)
    LEFT JOIN CampaignFRT cf ON dc.Reference_ID = cf.Reference_ID
    LEFT JOIN CampaignModels cm ON dc.Reference_ID = cm.Reference_ID
    WHERE (
        (dc.Source_Activated_Date >= DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)
        AND dc.Source_Activated_Date < DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()) + 1, 0))
        OR
        (ISNULL(cf.Max_FRT, 0) > 0.1
        AND dc.Campaign_Status = 'Active'
        AND dc.Action_type IN ('On Next Visit', 'Proactive', 'Required Pre-Delivery', 'Recall'))
    )
    AND (dc.Campaign_Manager_Email in ('jexue@tesla.com', 'gloganathan@tesla.com', 'miclu@tesla.com',
        'xiongh@tesla.com', 'pkennedy@tesla.com', 'ddonovan@tesla.com', 'skarnik@tesla.com')
        OR dc.Engineer_Name in ('jexue@tesla.com', 'gloganathan@tesla.com', 'miclu@tesla.com',
        'xiongh@tesla.com', 'pkennedy@tesla.com', 'ddonovan@tesla.com', 'skarnik@tesla.com')
        OR EXISTS (SELECT 1 FROM STRING_SPLIT(dc.Campaign_Zones, ',')
        WHERE LTRIM(RTRIM([value])) = 'NA'))
),
CampaignStats AS (
    SELECT
        rc.Reference_ID,
        rc.Campaign_ID,
        rc.Concern_type,
        rc.Action_type,
        rc.Campaign_Manager_Name,
        rc.Campaign_Title,
        rc.Source_Activated_Date,
        rc.Campaign_Category,
        COUNT(DISTINCT fcv.VIN) as Total_VINs,
        SUM(CASE WHEN fcv.Vin_Status = 'Completed' THEN 1 ELSE 0 END) as Completed_VINs,
        SUM(CASE WHEN fcv.Vin_Status IN ('New') THEN 1 ELSE 0 END) as Outstanding_VINs,
        ROUND((SUM(CASE WHEN fcv.Vin_Status = 'Completed' THEN 1.0 ELSE 0 END) /
        NULLIF(COUNT(DISTINCT fcv.VIN), 0)) * 100, 2) as Completion_Rate,
        rc.FRT_List,
        rc.Max_FRT,
        rc.AffectedModels
    FROM RelevantCampaigns rc
    LEFT JOIN service.EDW.FactCampaignVins fcv (NOLOCK) ON rc.Reference_ID = fcv.Reference_ID
    WHERE fcv.Vin_Status in ('Completed','New','Incomplete')
    GROUP BY
        rc.Reference_ID,
        rc.Campaign_ID,
        rc.Concern_type,
        rc.Action_type,
        rc.Campaign_Manager_Name,
        rc.Campaign_Title,
        rc.Source_Activated_Date,
        rc.Campaign_Category,
        rc.FRT_List,
        rc.Max_FRT,
        rc.AffectedModels
)
SELECT * FROM CampaignStats
WHERE (Campaign_Category = 'This Week') OR
      (Campaign_Category = 'Active High FRT' AND Completion_Rate < 80)
ORDER BY
    CASE Campaign_Category WHEN 'This Week' THEN 1 ELSE 2 END,
    Concern_type,
    Total_VINs DESC;
"""

def get_campaign_data(sql_query):
    connection_string = (
        f"mssql+pyodbc://{db_config['server']}:{db_config['port']}/{db_config['database']}?driver={db_config['driver'].replace(' ', '+')}&trusted_connection=yes"
    )
    engine = create_engine(connection_string)
    try:
        with engine.connect() as connection:
            result = connection.execute(sa.text(sql_query))  # Use `sa.text()` for raw SQL
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            return df
    finally:
        engine.dispose()

def test_file_access():
    try:
        if not os.path.exists(SNAPSHOT_DIR):
            os.makedirs(SNAPSHOT_DIR)
            print(f"Successfully created directory: {SNAPSHOT_DIR}")
        test_file = os.path.join(SNAPSHOT_DIR, "test_access.txt")
        with open(test_file, 'w') as f:
            f.write("Test file access")
        print(f"Successfully wrote test file: {test_file}")
        os.remove(test_file)
        print("Test file removed. File access is working correctly.")
        return True
    except Exception as e:
        print(f"ERROR: File access test failed: {str(e)}")
        return False

def save_snapshot(df):
    try:
        if not os.path.exists(SNAPSHOT_DIR):
            os.makedirs(SNAPSHOT_DIR)
        today = datetime.today().strftime('%Y-%m-%d')
        filename = os.path.join(SNAPSHOT_DIR, f'campaign_snapshot_{today}.pkl')
        df.to_pickle(filename)
        print(f"Saved snapshot to {filename}")
        return filename
    except Exception as e:
        print(f"ERROR saving snapshot: {str(e)}")
        return None

def get_previous_snapshot(days_ago=7):
    target_date = datetime.today() - timedelta(days=days_ago)
    target_date_str = target_date.strftime('%Y-%m-%d')
    filename = os.path.join(SNAPSHOT_DIR, f'campaign_snapshot_{target_date_str}.pkl')
    if os.path.exists(filename):
        return pd.read_pickle(filename)
    if os.path.exists(SNAPSHOT_DIR):
        files = [f for f in os.listdir(SNAPSHOT_DIR) if f.startswith('campaign_snapshot_')]
        if not files:
            print(f"No previous snapshots found in {SNAPSHOT_DIR}")
            return None
        dates = []
        for file in files:
            try:
                date_str = file.replace('campaign_snapshot_', '').replace('.pkl', '')
                file_date = datetime.strptime(date_str, '%Y-%m-%d')
                dates.append((file_date, file))
            except:
                continue
        dates.sort(key=lambda x: x[0])
        closest_file = None
        for date, file in dates:
            if date <= target_date:
                closest_file = file
            else:
                break
        if closest_file:
            closest_path = os.path.join(SNAPSHOT_DIR, closest_file)
            print(f"Using closest snapshot from {closest_file}")
            return pd.read_pickle(closest_path)
    print("No suitable previous snapshot found")
    return None

def should_show_models(title):
    model_patterns = [
        r'\bModel S\b', r'\bModel X\b', r'\bModel Y\b', r'\bModel 3\b',
        r'\bMS\b', r'\bMX\b', r'\bMY\b', r'\bM3\b',
        r'\bRoadster\b', r'\bSemi\b', r'\bCybertruck\b'
    ]
    for pattern in model_patterns:
        if re.search(pattern, title, re.IGNORECASE):
            return False
    return True

def format_report(df, previous_df=None):
    today = datetime.today()
    days_since_saturday = (today.weekday() + 2) % 7
    start_of_week = today - timedelta(days=days_since_saturday)
    end_of_week = start_of_week + timedelta(days=6)

    weekly_campaigns = df[df['Campaign_Category'] == 'This Week']
    high_frt_campaigns = df[df['Campaign_Category'] == 'Active High FRT']

    previous_vins = {}
    if previous_df is not None:
        previous_vins = previous_df.set_index('Campaign_ID')['Outstanding_VINs'].to_dict()

    report = f"""
<html>
<head>
<style>
    .decrease {{ color: green; }}
    .increase {{ color: red; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ padding: 8px; text-align: left; border: 1px solid #ddd; }}
    th {{ background-color: #f2f2f2; }}
</style>
</head>
<body>
<h2>Weekly IOI: Campaign Summary for {start_of_week.strftime('%B %d, %Y')} to {end_of_week.strftime('%B %d, %Y')}</h2>
"""
    if not weekly_campaigns.empty:
        containment_group = weekly_campaigns[weekly_campaigns['Concern_type'] == 'Containment']
        if not containment_group.empty:
            report += "<h3>New: Pre-Delivery Containment</h3><ul>"
            for _, row in containment_group.iterrows():
                campaign_info = f"{row['Campaign_ID']} {row['Campaign_Title']}"
                if should_show_models(row['Campaign_Title']) and pd.notna(row['AffectedModels']):
                    campaign_info += f" ({row['AffectedModels']})"
                report += f"<li>{campaign_info}: {row['Total_VINs']:,} VIN(s), Completion Rate: {row['Completion_Rate']:.2f}%, FRT = {row['FRT_List'] or '0'}</li>"
            report += "</ul>"
        
        campaign_types = ['Bulletin', 'OWO', 'Engineering Study', 'Due Bill', 'Recall']
        post_delivery = weekly_campaigns[weekly_campaigns['Concern_type'].isin(campaign_types)]
        if not post_delivery.empty:
            report += "<h3>New: Post Delivery Campaign</h3>"
            for campaign_type in campaign_types:
                type_group = post_delivery[post_delivery['Concern_type'] == campaign_type]
                if not type_group.empty:
                    report += f"<h4>{campaign_type}:</h4><ul>"
                    for _, row in type_group.iterrows():
                        campaign_info = f"{row['Campaign_ID']} {row['Campaign_Title']}"
                        if should_show_models(row['Campaign_Title']) and pd.notna(row['AffectedModels']):
                            campaign_info += f" ({row['AffectedModels']})"
                        report += f"<li>{campaign_info}: {row['Total_VINs']:,} VIN(s), Completion Rate: {row['Completion_Rate']:.2f}%, FRT = {row['FRT_List'] or '0'}</li>"
                    report += "</ul>"
    
    report += "<br>"
    
    containment_df = high_frt_campaigns[high_frt_campaigns['Concern_type'] == 'Containment'].copy()
    containment_df['Service_Repair_Hours'] = containment_df['Outstanding_VINs'] * containment_df['Max_FRT']
    containment_df = containment_df.sort_values('Service_Repair_Hours', ascending=False).head(5)
    report += "<h2>Top 5 Open Containment (FRT > 0.1, Completion < 80%)</h2>"
    report += "<table border='1'><tr><th>Rank</th><th>Campaign ID</th><th>Campaign Title</th><th>Affected Models</th><th>Action Type</th><th>Outstanding VINs</th><th>Max FRT</th><th>Service Repair Hours</th><th>Completion Rate</th></tr>"
    if containment_df.empty:
        report += "<tr><td colspan='9'>No active containment campaigns meet the criteria.</td></tr>"
    else:
        for rank, (_, row) in enumerate(containment_df.iterrows(), start=1):
            current_vins = row['Outstanding_VINs']
            previous_vins_count = previous_vins.get(row['Campaign_ID'], current_vins)
            change = current_vins - previous_vins_count
            change_html = ""
            if change < 0:
                change_html = f" <span class='decrease'>({change:,})</span>"
            elif change > 0:
                change_html = f" <span class='increase'>(+{change:,})</span>"
            report += (
                f"<tr><td>{rank}</td><td>{row['Campaign_ID']}</td><td>{row['Campaign_Title']}</td>"
                f"<td>{row['AffectedModels'] if pd.notna(row['AffectedModels']) else 'N/A'}</td>"
                f"<td>{row['Action_type']}</td><td>{current_vins:,}{change_html}</td>"
                f"<td>{row['Max_FRT']:.2f}</td><td>{row['Service_Repair_Hours']:,.2f}</td>"
                f"<td>{row['Completion_Rate']:.2f}%</td></tr>"
            )
    report += "</table><br>"

    post_delivery_df = high_frt_campaigns[high_frt_campaigns['Concern_type'].isin(['OWO', 'Engineering Study', 'Bulletin', 'Due Bill', 'Recall'])].copy()
    post_delivery_df['Service_Repair_Hours'] = post_delivery_df['Outstanding_VINs'] * post_delivery_df['Max_FRT']
    post_delivery_df = post_delivery_df.sort_values('Service_Repair_Hours', ascending=False).head(5)
    report += "<h2>Top 5 Open Post-Delivery Campaigns (FRT > 0.1, Completion < 80%)</h2>"
    report += "<table border='1'><tr><th>Rank</th><th>Campaign ID</th><th>Campaign Title</th><th>Affected Models</th><th>Action Type</th><th>Outstanding VINs</th><th>Max FRT</th><th>Service Repair Hours</th><th>Completion Rate</th></tr>"
    if post_delivery_df.empty:
        report += "<tr><td colspan='9'>No active post-delivery campaigns meet the criteria.</td></tr>"
    else:
        for rank, (_, row) in enumerate(post_delivery_df.iterrows(), start=1):
            current_vins = row['Outstanding_VINs']
            previous_vins_count = previous_vins.get(row['Campaign_ID'], current_vins)
            change = current_vins - previous_vins_count
            change_html = ""
            if change < 0:
                change_html = f" <span class='decrease'>({change:,})</span>"
            elif change > 0:
                change_html = f" <span class='increase'>(+{change:,})</span>"
            report += (
                f"<tr><td>{rank}</td><td>{row['Campaign_ID']}</td><td>{row['Campaign_Title']}</td>"
                f"<td>{row['AffectedModels'] if pd.notna(row['AffectedModels']) else 'N/A'}</td>"
                f"<td>{row['Action_type']}</td><td>{current_vins:,}{change_html}</td>"
                f"<td>{row['Max_FRT']:.2f}</td><td>{row['Service_Repair_Hours']:,.2f}</td>"
                f"<td>{row['Completion_Rate']:.2f}%</td></tr>"
            )
    report += "<tr><td colspan='9'><i>* Numbers in parentheses next to Outstanding VINs show the change from the previous week. <span class='decrease'>Green</span> indicates a decrease, <span class='increase'>red</span> indicates an increase. Service Repair Hours is calculated as Outstanding VINs multiplied by Max FRT.</i></td></tr>"
    report += "</table></body></html>"
    return report

def send_to_power_automate(webhook_url, report_html):
    """Sends the generated HTML report to a Power Automate webhook."""
    print("Sending report to Power Automate...")
    headers = {'Content-Type': 'application/json'}
    payload = {'emailBody': report_html}
    try:
        # Disable SSL verification (use with caution, only as a temporary workaround)
        requests.packages.urllib3.disable_warnings()
        response = requests.post(webhook_url, json=payload, headers=headers, verify=False)
        response.raise_for_status()
        print("Successfully sent report to Power Automate.")
    except requests.exceptions.RequestException as e:
        print(f"Error sending report to Power Automate: {e}")
        sys.exit(1)

def main():
    if not POWER_AUTOMATE_WEBHOOK_URL:
        print("Error: POWER_AUTOMATE_WEBHOOK_URL is not set. Please paste the URL into the script.")
        return
    try:
        if not test_file_access():
            print("Exiting due to file access issues.")
            return
        print("Fetching campaign data from database...")
        df = get_campaign_data(sql_query_combined)
        if df.empty:
            print("No data returned. Check database connection or query. No report will be sent.")
            return
        
        save_snapshot(df)
        
        print("Looking for previous snapshot data...")
        previous_df = get_previous_snapshot(days_ago=7)
        
        print("Generating report...")
        report = format_report(df, previous_df)
        
        send_to_power_automate(POWER_AUTOMATE_WEBHOOK_URL, report)
            
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()