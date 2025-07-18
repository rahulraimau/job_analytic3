# app.py - Python Flask Backend for Job Analytics Portal

from flask import Flask, jsonify, request
from flask_cors import CORS # To allow cross-origin requests from your React app
import pandas as pd
import json
import re
import os # For checking file existence

app = Flask(__name__)
CORS(app) # Enable CORS for all routes, so your React app can access it

# Global variable to store the processed DataFrame
# In a real-world app, you might use a database here.
processed_df = pd.DataFrame()

# --- Data Cleaning and Preprocessing Functions (Reused from your notebook) ---
def parse_company_profile(profile_str):
    if pd.isna(profile_str):
        return {}
    try:
        cleaned_str = str(profile_str).replace('""""', '"')
        if cleaned_str.startswith('"') and cleaned_str.endswith('"'):
            cleaned_str = cleaned_str[1:-1]
        return json.loads(cleaned_str)
    except json.JSONDecodeError:
        # print(f"Error decoding JSON for string: {profile_str[:50]}...") # Uncomment for debugging
        return {}

def extract_min_experience(experience_str):
    if pd.isna(experience_str):
        return None
    match = re.search(r'(\d+)\s*to', str(experience_str))
    if match:
        return int(match.group(1))
    return None

def extract_min_salary(salary_str):
    if pd.isna(salary_str):
        return None
    match = re.search(r'\$(\d+)K', str(salary_str))
    if match:
        return int(match.group(1)) * 1000
    return None

def clean_and_process_data(df_raw):
    """Applies all cleaning and preprocessing steps to the DataFrame."""
    if df_raw.empty:
        return pd.DataFrame()

    df = df_raw.copy()

    # Convert 'Job Posting Date' to datetime objects.
    df['Job Posting Date'] = pd.to_datetime(df['Job Posting Date'], errors='coerce')

    # Apply parsing for 'Company Profile'
    df['Company Profile_Parsed'] = df['Company Profile'].apply(parse_company_profile)
    df['Company Sector'] = df['Company Profile_Parsed'].apply(lambda x: x.get('Sector'))

    # Extract min experience and salary
    df['Min Experience Years'] = df['Experience'].apply(extract_min_experience)
    df['Min Salary USD'] = df['Salary Range'].apply(extract_min_salary)

    return df

# --- API Endpoints ---

@app.before_request
def load_data_on_startup():
    """Loads and processes data once when the Flask app starts."""
    global processed_df
    if processed_df.empty: # Only load if not already loaded
        job_data_filename = 'job_descriptions.csv' # Assuming it's in the same directory
        if os.path.exists(job_data_filename):
            try:
                df_raw = pd.read_csv(job_data_filename)
                processed_df = clean_and_process_data(df_raw)
                print(f"Data loaded and processed successfully from {job_data_filename}.")
                print(f"Processed DataFrame shape: {processed_df.shape}")
            except Exception as e:
                print(f"Error loading or processing data: {e}")
        else:
            print(f"Error: {job_data_filename} not found. Please place it in the backend directory.")

@app.route('/api/job_data', methods=['GET'])
def get_all_job_data():
    """
    Returns a sample of all processed job data.
    In a real app, you'd likely paginate or filter this.
    """
    if processed_df.empty:
        return jsonify({"message": "Data not loaded or processed yet."}), 500
    # Return a sample or first few rows for demonstration
    return jsonify(processed_df.head(100).to_dict(orient='records'))

@app.route('/api/analytics/work_type_distribution', methods=['GET'])
def get_work_type_distribution():
    """Returns the distribution of work types, optionally filtered."""
    if processed_df.empty:
        return jsonify([]), 200 # Return empty if no data

    # Get filter parameters from query string
    work_type_filter = request.args.get('workType', 'All')

    df_filtered = processed_df.copy()
    if work_type_filter != 'All':
        df_filtered = df_filtered[df_filtered['Work Type'] == work_type_filter]

    distribution = df_filtered['Work Type'].value_counts().reset_index()
    distribution.columns = ['name', 'count']
    return jsonify(distribution.to_dict(orient='records'))

@app.route('/api/analytics/qualification_distribution', methods=['GET'])
def get_qualification_distribution():
    """Returns the distribution of qualifications, optionally filtered."""
    if processed_df.empty:
        return jsonify([]), 200

    qualification_filter = request.args.get('qualification', 'All')

    df_filtered = processed_df.copy()
    if qualification_filter != 'All':
        df_filtered = df_filtered[df_filtered['Qualifications'] == qualification_filter]

    distribution = df_filtered['Qualifications'].value_counts().reset_index()
    distribution.columns = ['qualification', 'count']
    return jsonify(distribution.to_dict(orient='records'))

@app.route('/api/analytics/experience_distribution', methods=['GET'])
def get_experience_distribution():
    """Returns the distribution of experience levels, optionally filtered."""
    if processed_df.empty:
        return jsonify([]), 200

    experience_filter = request.args.get('experience', 'All')

    df_filtered = processed_df.copy()
    # Map numerical experience to levels for filtering if needed, or filter directly
    # For simplicity, we'll just return the counts for now.
    # In a real app, you'd categorize Min Experience Years into these levels.
    
    # Example: Categorize based on Min Experience Years
    bins = [-1, 2, 5, 10, float('inf')]
    labels = ['0-2 Years', '3-5 Years', '6-10 Years', '10+ Years']
    df_filtered['Experience_Level_Category'] = pd.cut(df_filtered['Min Experience Years'], bins=bins, labels=labels, right=True)

    if experience_filter != 'All':
        df_filtered = df_filtered[df_filtered['Experience_Level_Category'] == experience_filter]

    distribution = df_filtered['Experience_Level_Category'].value_counts().reset_index()
    distribution.columns = ['level', 'count']
    # Ensure all labels are present, even if count is 0
    full_labels_df = pd.DataFrame({'level': labels})
    distribution = pd.merge(full_labels_df, distribution, on='level', how='left').fillna(0)
    
    return jsonify(distribution.to_dict(orient='records'))

@app.route('/api/analytics/salary_range_distribution', methods=['GET'])
def get_salary_range_distribution():
    """Returns the distribution of salary ranges."""
    if processed_df.empty:
        return jsonify([]), 200

    # Define salary bins and labels
    bins = [0, 50000, 75000, 100000, 125000, 150000, float('inf')]
    labels = ['$0-$50K', '$50K-$75K', '$75K-$100K', '$100K-$125K', '$125K-$150K', '$150K+']
    
    # Categorize 'Min Salary USD'
    processed_df['Salary_Range_Category'] = pd.cut(processed_df['Min Salary USD'], bins=bins, labels=labels, right=False)

    distribution = processed_df['Salary_Range_Category'].value_counts().reset_index()
    distribution.columns = ['range', 'count']
    # Ensure all labels are present, even if count is 0
    full_labels_df = pd.DataFrame({'range': labels})
    distribution = pd.merge(full_labels_df, distribution, on='range', how='left').fillna(0)
    
    return jsonify(distribution.to_dict(orient='records'))

@app.route('/api/analytics/job_portal_distribution', methods=['GET'])
def get_job_portal_distribution():
    """Returns the distribution of job portals."""
    if processed_df.empty:
        return jsonify([]), 200
    distribution = processed_df['Job Portal'].value_counts().reset_index()
    distribution.columns = ['name', 'value']
    return jsonify(distribution.to_dict(orient='records'))

@app.route('/api/analytics/job_postings_trend', methods=['GET'])
def get_job_postings_trend():
    """Returns the trend of job postings over time (monthly)."""
    if processed_df.empty:
        return jsonify([]), 200
    
    # Ensure 'Job Posting Date' is datetime
    df_temp = processed_df.copy()
    df_temp['YearMonth'] = df_temp['Job Posting Date'].dt.to_period('M')
    
    trend = df_temp.groupby('YearMonth').size().reset_index(name='postings')
    trend['month'] = trend['YearMonth'].dt.strftime('%b %Y') # Format for chart
    
    return jsonify(trend[['month', 'postings']].to_dict(orient='records'))

@app.route('/api/analytics/top_10_companies', methods=['GET'])
def get_top_10_companies():
    """
    Returns top 10 companies based on specific filters,
    mimicking the complex logic from the Python notebook.
    """
    if processed_df.empty:
        return jsonify([]), 200

    # Define Asian countries for filtering
    asian_countries = {
        'Afghanistan', 'Armenia', 'Azerbaijan', 'Bahrain', 'Bangladesh', 'Bhutan',
        'Brunei', 'Cambodia', 'China', 'Cyprus', 'Georgia', 'India', 'Indonesia',
        'Iran', 'Iraq', 'Israel', 'Japan', 'Jordan', 'Kazakhstan', 'Kuwait',
        'Kyrgyzstan', 'Laos', 'Lebanon', 'Malaysia', 'Maldives', 'Mongolia',
        'Myanmar', 'Nepal', 'North Korea', 'Oman', 'Pakistan', 'Palestine',
        'Philippines', 'Qatar', 'Russia', 'Saudi Arabia', 'Singapore',
        'South Korea', 'Sri Lanka', 'Syria', 'Taiwan', 'Tajikistan', 'Thailand',
        'Timor-Leste', 'Turkey', 'Turkmenistan', 'United Arab Emirates',
        'Uzbekistan', 'Vietnam', 'Yemen'
    }

    filtered_df = processed_df[
        (processed_df['Role'] == 'Data Engineer') &
        (processed_df['Job Title'] == 'Data Scientist') &
        (processed_df['Preference'] == 'Female') &
        (processed_df['Qualifications'] == 'B.Tech') &
        (~processed_df['Country'].isin(asian_countries)) &
        (~processed_df['Country'].astype(str).str.startswith('C')) &
        (processed_df['latitude'] < 10) & # Latitude should be BELOW 10.
        (processed_df['Job Posting Date'] >= '2023-01-01') &
        (processed_df['Job Posting Date'] <= '2023-06-01')
    ]

    if filtered_df.empty:
        return jsonify([])
    
    top_10 = filtered_df['Company'].value_counts().nlargest(10).reset_index()
    top_10.columns = ['Company', 'Count']
    return jsonify(top_10.to_dict(orient='records'))


@app.route('/api/analytics/company_size_vs_name', methods=['GET'])
def get_company_size_vs_name():
    """
    Returns data for company size vs company name chart,
    mimicking the complex logic from the Python notebook.
    """
    if processed_df.empty:
        return jsonify([]), 200

    # Define Asian countries for filtering (reused)
    asian_countries = {
        'Afghanistan', 'Armenia', 'Azerbaijan', 'Bahrain', 'Bangladesh', 'Bhutan',
        'Brunei', 'Cambodia', 'China', 'Cyprus', 'Georgia', 'India', 'Indonesia',
        'Iran', 'Iraq', 'Israel', 'Japan', 'Jordan', 'Kazakhstan', 'Kuwait',
        'Kyrgyzstan', 'Laos', 'Lebanon', 'Malaysia', 'Maldives', 'Mongolia',
        'Myanmar', 'Nepal', 'North Korea', 'Oman', 'Pakistan', 'Palestine',
        'Philippines', 'Qatar', 'Russia', 'Saudi Arabia', 'Singapore',
        'South Korea', 'Sri Lanka', 'Syria', 'Taiwan', 'Tajikistan', 'Thailand',
        'Timor-Leste', 'Turkey', 'Turkmenistan', 'United Arab Emirates',
        'Uzbekistan', 'Vietnam', 'Yemen'
    }

    filtered_df = processed_df[
        (processed_df['Company Size'] < 50000) &
        (processed_df['Job Title'] == 'Mechanical Engineer') &
        (processed_df['Min Experience Years'] > 5) &
        (processed_df['Country'].isin(asian_countries)) & # Country should be Asian.
        (processed_df['Min Salary USD'] > 50000) &
        (processed_df['Work Type'].isin(['Part-Time', 'Full-Time'])) & # Work type should be part time or full time.
        (processed_df['Preference'] == 'Male') &
        (processed_df['Job Portal'] == 'Idealist')
    ]

    if filtered_df.empty:
        return jsonify([])
    
    # For this chart, we return unique companies and their size (or average size if multiple entries)
    # For simplicity, let's take the first entry's company size if multiple exist for a company
    result = filtered_df[['Company', 'Company Size']].drop_duplicates(subset=['Company']).to_dict(orient='records')
    return jsonify(result)


if __name__ == '__main__':
    # When running locally, Flask will automatically call before_request
    # You can specify host='0.0.0.0' to make it accessible from other devices on your network
    app.run(debug=True, port=5000)
