import json
import re
import time
import os
import pandas as pd
import requests
import folium
import math
import googlemaps
import numpy as np
from folium.plugins import HeatMap
from collections import defaultdict
from branca.element import Element
from dotenv import load_dotenv
from tqdm import tqdm

# --- Configuration ---
# File Paths
RAW_JSON_FILENAME = "map_creation/fitzpatrick_usa_search.json"
FINAL_MASTER_CSV = "map_creation/final_master_dataset.csv"
MAP_OUTPUT_HTML = "index.html"

# ClinicalTrials.gov API Settings
API_BASE_URL = "https://clinicaltrials.gov/api/v2/studies"
SEARCH_KEYWORD = 'fitzpatrick'
COUNTRY_TO_ISOLATE = "United States"

# --- 1. ClinicalTrials.gov Data Fetching ---

def fetch_clinical_trials_data(api_url, keyword, output_filename):
    """
    Searches the ClinicalTrials.gov API for studies matching a keyword
    and saves the raw results to a JSON file.
    """
    print(f"[*] Starting API query to fetch clinical trial data for keyword: '{keyword}'...")
    all_studies = []
    page_count = 1
    next_page_token = None
    eligibility_search = f'AREA[EligibilityCriteria]({keyword}) AND SEARCH[Location](AREA[LocationCountry]"{COUNTRY_TO_ISOLATE}")'
    fields_to_get = ["NCTId", "protocolSection", "resultsSection"]
    params = {'query.term': eligibility_search, 'fields': ",".join(fields_to_get), 'pageSize': 100}

    while True:
        try:
            if next_page_token:
                params['pageToken'] = next_page_token
            response = requests.get(api_url, params=params)
            response.raise_for_status()
            data = response.json()
            current_studies = data.get('studies', [])
            if not current_studies: break
            all_studies.extend(current_studies)
            print(f"[*] Page {page_count}: Fetched {len(current_studies)} studies. Total so far: {len(all_studies)}")
            next_page_token = data.get('nextPageToken')
            if not next_page_token: break
            page_count += 1
            time.sleep(0.5)
        except requests.exceptions.RequestException as e:
            print(f"\n[!] API request failed on page {page_count}: {e}")
            break

    if all_studies:
        os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump({'studies': all_studies}, f, ensure_ascii=False, indent=2)
        print(f"\n[*] Success! Saved {len(all_studies)} total studies to '{output_filename}'.")
    else:
        print("\n[!] No studies were found to save.")

# --- 2. Data Processing and Feature Extraction ---

def parse_eligibility_criteria(study_record, keyword):
    """Finds sentences mentioning a keyword in the eligibility criteria."""
    eligibility_text = study_record.get('protocolSection', {}).get('eligibilityModule', {}).get('eligibilityCriteria', '')
    if not eligibility_text: return []
    parts = re.split(r'exclusion', eligibility_text, flags=re.IGNORECASE)
    found_sentences = []
    for text_part, is_exclusion in [(parts[0], False), (parts[1] if len(parts) > 1 else "", True)]:
        if not text_part: continue
        for sentence in re.split(r'[.\n]', text_part):
            if keyword in sentence.lower() and sentence.strip():
                found_sentences.append({'sentence': sentence.strip(), 'is_exclusion': is_exclusion})
    return found_sentences

def extract_and_standardize_scores(sentence):
    """Analyzes a sentence to extract and format Fitzpatrick scores."""
    if not isinstance(sentence, str): return {}
    text = sentence.lower()
    result = {'extracted_score': 'Not Specified', 'Type_I': 0, 'Type_II': 0, 'Type_III': 0, 'Type_IV': 0, 'Type_V': 0, 'Type_VI': 0}
    if any(word in text for word in ['wrinkle']):
        result['extracted_score'] = 'Not a Skin Type Score'
        return result
    if 'all' in text or 'any' in text:
        result.update({k: 1 for k in result if k.startswith('Type_')})
        result['extracted_score'] = 'All'
        return result

    roman_map = {'I': 1, 'II': 2, 'III': 3, 'IV': 4, 'V': 5, 'VI': 6}
    to_roman_map = {v: k for k, v in roman_map.items()}
    
    def _to_int(s):
        s_upper = s.upper()
        if s_upper == 'L': return 1
        return int(s) if s.isdigit() else roman_map.get(s_upper)

    valid_numeral_pattern = r'\b(vi|v|iv|iii|ii|i|l|[1-6])\b'
    range_pattern = f'{valid_numeral_pattern}\\s*(?:-|to|through)\\s*{valid_numeral_pattern}'
    range_match = re.search(range_pattern, text, re.IGNORECASE)

    if range_match:
        start_str, end_str = range_match.groups()
        start_num, end_num = _to_int(start_str), _to_int(end_str)
        if start_num is not None and end_num is not None and start_num < end_num:
            for i in range(start_num, end_num + 1):
                if i in to_roman_map: result[f"Type_{to_roman_map[i]}"] = 1
            result['extracted_score'] = f"{to_roman_map.get(start_num, '')}-{to_roman_map.get(end_num, '')}"
    else:
        numerals_found = re.findall(valid_numeral_pattern, text, re.IGNORECASE)
        scores = sorted(list(set(_to_int(n) for n in numerals_found if _to_int(n) is not None)))
        roman_scores = []
        for score in scores:
            roman_version = to_roman_map.get(score)
            if roman_version:
                result[f"Type_{roman_version}"] = 1
                roman_scores.append(roman_version)
        if roman_scores: result['extracted_score'] = ", ".join(roman_scores)
    return result

def extract_study_details(study_record, country):
    """Extracts key details from a single study record."""
    details = {'status': "N/A", 'us_facilities': [], 'enrollment': 'N/A', 'enrollment_type': 'N/A', 'race_data': {}, 'last_update_year': 'N/A'}
    protocol = study_record.get('protocolSection', {})
    if not protocol: return details
    
    status_module = protocol.get('statusModule', {})
    details['status'] = status_module.get('overallStatus', 'N/A')
    post_date_struct = status_module.get('lastUpdatePostDateStruct', {})
    if last_update_date := post_date_struct.get('date'):
        if isinstance(last_update_date, str) and len(last_update_date) >= 4:
            details['last_update_year'] = last_update_date[:4]

    if enrollment_info := protocol.get('designModule', {}).get('enrollmentInfo'):
        if 'count' in enrollment_info:
            details['enrollment'] = enrollment_info['count']
            details['enrollment_type'] = enrollment_info.get('type', 'N/A')

    for loc in protocol.get('contactsLocationsModule', {}).get('locations', []):
        if loc.get('country') == country and loc.get('geoPoint'):
            details['us_facilities'].append({
                'facility': loc.get('facility', 'N/A'), 'city': loc.get('city', 'N/A'),
                'state': loc.get('state', 'N/A'), 'zip': loc.get('zip', 'N/A'),
                'latitude': loc.get('geoPoint', {}).get('lat'), 'longitude': loc.get('geoPoint', {}).get('lon')
            })

    if results_section := study_record.get('resultsSection', {}):
        for measure in results_section.get('baselineCharacteristicsModule', {}).get('measures', []):
            if measure.get('title') == "Race (NIH/OMB)":
                for cat in measure.get('classes', [{}])[0].get('categories', []):
                    if race_title := cat.get('title'):
                        total_count = sum(int(m.get('value', 0)) for m in cat.get('measurements', []))
                        details['race_data'][f"Race_{race_title.replace(' ', '_')}"] = total_count
    return details

def process_raw_data(studies):
    """Processes raw JSON data into a clean DataFrame ready for geocoding."""
    print("[*] Processing raw study data...")
    all_facility_rows, all_race_keys = [], set()
    for study in studies:
        nct_id = study.get('protocolSection', {}).get('identificationModule', {}).get('nctId', 'N/A')
        details = extract_study_details(study, COUNTRY_TO_ISOLATE)
        if not details['us_facilities']: continue
        
        all_race_keys.update(details['race_data'].keys())
        inclusion_sentences = [s['sentence'] for s in parse_eligibility_criteria(study, SEARCH_KEYWORD) if not s['is_exclusion']]
        if not inclusion_sentences: continue
        
        score_data = extract_and_standardize_scores(inclusion_sentences[0])
        if score_data['extracted_score'] == 'Not a Skin Type Score': continue
            
        for facility in details['us_facilities']:
            row = {'nctId': nct_id, 'status': details['status'], 'enrollment': details['enrollment'], 'enrollment_type': details['enrollment_type'], 'last_update_year': details['last_update_year']}
            row.update(score_data)
            row.update(facility)
            row.update(details['race_data'])
            all_facility_rows.append(row)
    
    if not all_facility_rows:
        print("[!] No processable US-based facilities found.")
        return pd.DataFrame()

    df = pd.DataFrame(all_facility_rows)
    for race_col in all_race_keys:
        if race_col not in df.columns: df[race_col] = 0
    df.fillna(0, inplace=True)
    
    skin_type_cols = [f'Type_{r}' for r in ['I', 'II', 'III', 'IV', 'V', 'VI']]
    unparsed_mask = df[skin_type_cols].sum(axis=1) == 0
    if not df[~unparsed_mask].empty:
        print(f"[*] Dropping {unparsed_mask.sum()} records with no specific Fitzpatrick scores.")
        df = df[~unparsed_mask].copy()
    
    print(f"[*] Processed data into {len(df)} facility-level records.")
    return df

# --- 3. Google Maps Places API Geocoding ---

def geocode_locations_with_places_api(df_to_geocode):
    """
    Enriches a DataFrame with coordinates and place names using Google Places API.
    Only updates rows that can be successfully geocoded.
    """
    load_dotenv()
    API_KEY = os.getenv('GOOGLE_MAPS_API_KEY')
    if not API_KEY:
        raise ValueError("Google Maps API key not found in .env file.")
    gmaps = googlemaps.Client(key=API_KEY)
    
    df = df_to_geocode.copy()
    
    print("\n[*] Preparing search queries for Google Places API...")
    for col in ['facility', 'city', 'state', 'zip']:
        df[col] = df[col].astype(str)

    ends_with_site = df['facility'].str.lower().str.endswith('site', na=False)
    ends_with_number = df['facility'].str.contains(r'\d+$', regex=True, na=False)
    
    fatal_flaw = (
        df['facility'].isin(['N/A', 'nan']) |
        df['facility'].str.startswith('Call Suneva', na=False) |
        df['zip'].isin(['00000']) |
        ends_with_site |
        ends_with_number
    )
    bad_zip = df['zip'].isin(['N/A', 'nan'])
    
    df['search_query'] = 'SKIP'
    workable_rows = ~fatal_flaw
    
    # Rows with good zip codes
    good_zip_rows = workable_rows & ~bad_zip
    df.loc[good_zip_rows, 'search_query'] = (
        df.loc[good_zip_rows, 'facility'] + ', ' + df.loc[good_zip_rows, 'city'] + ', ' +
        df.loc[good_zip_rows, 'state'] + ' ' + df.loc[good_zip_rows, 'zip']
    )
    # Rows with missing zip codes but otherwise good info
    missing_zip_rows = workable_rows & bad_zip
    df.loc[missing_zip_rows, 'search_query'] = (
        df.loc[missing_zip_rows, 'facility'] + ', ' + df.loc[missing_zip_rows, 'city'] + ', ' + df.loc[missing_zip_rows, 'state']
    )

    rows_to_process = df[df['search_query'] != 'SKIP']
    print(f"[*] Found {len(rows_to_process)} rows to geocode ({len(df) - len(rows_to_process)} rows will be skipped).")
    
    df['place_name'] = '' # Add new column for Google's official place name
    query_cache = {}

    for index, row in tqdm(rows_to_process.iterrows(), total=len(rows_to_process), desc="Geocoding with Places API"):
        query = row['search_query']
        if query in query_cache:
            result = query_cache[query]
        else:
            try:
                # Use Places API Text Search, requesting specific fields for efficiency
                result = gmaps.places(query=query)
                query_cache[query] = result
                time.sleep(0.02)
            except Exception as e:
                print(f"\n[!] API Error for query '{query}': {e}")
                query_cache[query] = None
                continue
        
        if result and result.get('results'):
            place = result['results'][0]
            location = place.get('geometry', {}).get('location', {})
            df.loc[index, 'latitude'] = location.get('lat')
            df.loc[index, 'longitude'] = location.get('lng')
            df.loc[index, 'place_name'] = place.get('name')
        else:
            df.loc[index, 'place_name'] = 'NO_RESULTS_FOUND'
            
    return df

# --- 4. Interactive Map Generation ---

def create_interactive_map_with_sidebar(map_data, filename):
    """Creates an interactive Folium map with a custom sidebar for filtering markers."""
    if not map_data:
        print("[!] No data available to create a map.")
        return

    print(f"[*] Generating interactive map from {len(map_data)} records...")
    us_center = [39.8283, -98.5795]
    m = folium.Map(location=us_center, zoom_start=4, tiles="cartodbpositron")
    HeatMap([]).add_to(m)

    # --- Prepare data for map layers ---
    locations_data = defaultdict(list)
    for rec in map_data:
        if pd.notna(rec.get('latitude')) and pd.notna(rec.get('longitude')):
            key = f"{float(rec['latitude']):.6f},{float(rec['longitude']):.6f}"
            locations_data[key].append(rec)


    heatmap_data = []
    for loc_key, studies_at_loc in locations_data.items():
        lat, lon = map(float, loc_key.split(','))
        count = len(studies_at_loc)
        # Use log1p which calculates log(1 + count) to handle single-study locations
        weight = math.log1p(count) 
        heatmap_data.append([lat, lon, weight])

    heatmap_gradient = {0.4:'blue', 0.6:'lime', 0.8:'yellow', 1.0:'red'}


    # --- Prepare data for sidebar filters ---
    all_race_columns = sorted([col for col in map_data[0].keys() if str(col).startswith('Race_')]) if map_data else []
    enrollment_values = [r.get('enrollment') for r in map_data if isinstance(r.get('enrollment'), (int, float)) and r.get('enrollment', 0) > 0]
    max_enrollment = max(enrollment_values) if enrollment_values else 1000
    year_values = [int(r['last_update_year']) for r in map_data if str(r.get('last_update_year')).isdigit()]
    min_year, max_year = (min(year_values), max(year_values)) if year_values else (2000, 2025)
    all_statuses = sorted(set(r.get('status', 'N/A') for r in map_data))
    status_display_map = {'ACTIVE_NOT_RECRUITING': 'Active, not recruiting', 'COMPLETED': 'Completed', 'ENROLLING_BY_INVITATION': 'Enrolling by invitation', 'NOT_YET_RECRUITING': 'Not yet recruiting', 'RECRUITING': 'Recruiting', 'SUSPENDED': 'Suspended', 'TERMINATED': 'Terminated', 'WITHDRAWN': 'Withdrawn', 'AVAILABLE': 'Available', 'NO_LONGER_AVAILABLE': 'No longer available', 'TEMPORARILY_NOT_AVAILABLE': 'Temporarily not available', 'APPROVED_FOR_MARKETING': 'Approved for marketing', 'WITHHELD': 'Withheld', 'UNKNOWN': 'Unknown status', 'N/A': 'N/A' }
    total_studies = len(set(rec.get('nctId') for rec in map_data))
    total_locations = len(locations_data)
    race_data = {}
    for col in all_race_columns:
        values = [r.get(col, 0) for r in map_data if isinstance(r.get(col), (int, float)) and r.get(col, 0) > 0]
        if values: race_data[col] = {'min': 0, 'max': int(max(values)), 'display_name': str(col).replace('Race_', '').replace('_', ' ')}

    # --- HTML and Sidebar ---
    css_rules = """ body { margin:0; padding:0; font-family:'Segoe UI',sans-serif; } .sidebar { position:fixed; top:0; left:0; width:320px; height:100vh; background:linear-gradient(135deg,#667eea 0%,#764ba2 100%); color:white; padding:20px; box-sizing:border-box; z-index:1001; overflow-y:auto; box-shadow:2px 0 10px rgba(0,0,0,0.2); } .sidebar h2 { margin:0 0 20px 0; font-size:24px; font-weight:300; border-bottom:2px solid rgba(255,255,255,0.3); padding-bottom:10px; } .filter-section { margin-bottom:20px; background:rgba(255,255,255,0.1); padding:15px; border-radius:8px; } .skin-type-item { display:flex; align-items:center; margin:8px 0; padding:8px; border-radius:6px; transition:background 0.3s; cursor:pointer; user-select:none; background:rgba(0,0,0,0.2); } .skin-type-item.active { background:rgba(255,255,255,0.3); } .color-indicator { width:18px; height:18px; border-radius:50%; margin-right:12px; border:2px solid white; } .slider { width:100%; -webkit-appearance:none; appearance:none; height:6px; border-radius:3px; background:rgba(255,255,255,0.3); outline:none; } .slider::-webkit-slider-thumb { -webkit-appearance:none; appearance:none; width:18px; height:18px; border-radius:50%; background:#ffd700; cursor:pointer; } .slider-value { font-size:12px; color:#ffd700; text-align:center; margin-top:5px; font-weight:bold; } .reset-btn { width:100%; padding:10px; background:rgba(255,255,255,0.2); color:white; border:none; border-radius:6px; cursor:pointer; font-size:14px; margin-top:10px; } .checkbox-group, .radio-group { display:flex; flex-direction:column; gap:8px; margin-top:10px; } .checkbox-item, .radio-item { display:flex; align-items:center; cursor:pointer; padding:6px 8px; border-radius:4px; background:rgba(0,0,0,0.2); transition:background 0.2s; } .checkbox-item:hover, .radio-item:hover { background:rgba(255,255,255,0.1); } .checkbox-item input, .radio-item input { margin-right:8px; cursor:pointer; } .checkbox-item label, .radio-item label { cursor:pointer; font-size:13px; flex:1; } .folium-map { position:absolute; top:0; left:320px; right:0; bottom:0; z-index:1000; } .filter-summary { background:rgba(0,0,0,0.2); padding:10px; border-radius:6px; margin-bottom:15px; font-size:13px; } .filter-summary div:not(:last-child) { margin-bottom:4px; } .race-filter { margin-bottom:10px; } .race-filter label { display:block; margin-bottom:5px; font-size:13px; } """
    type_colors = {'I':'#FFE5E5','II':'#FFB3B3','III':'#FF8080','IV':'#CC6600','V':'#8B4513','VI':'#654321'}
    skin_type_html = ''.join([f'<div class="skin-type-item active" data-type="{st}" onclick="this.classList.toggle(\'active\');updateFilters();"><div class="color-indicator" style="background-color:{c};"></div><span>Type {st}</span></div>' for st,c in type_colors.items()])
    race_filter_html = ''.join([f'<div class="race-filter"><label for="{rc.lower()}">{d["display_name"]}:</label><input type="range" id="{rc.lower()}" class="slider" min="0" max="{d["max"]}" value="0" oninput="updateFilters()"><div class="slider-value" id="{rc.lower()}-value">0+</div></div>' for rc,d in race_data.items()])
    status_checkboxes = ''.join([f'<div class="checkbox-item"><input type="checkbox" id="status-{s.lower()}" checked onchange="updateFilters()"><label for="status-{s.lower()}">{status_display_map.get(s,s)}</label></div>' for s in all_statuses])
    
    # --- Re-added for Heatmap ---
    viz_switcher_html = """<div class="filter-section"><h3>Visualization Type</h3><div class="radio-group"><div class="radio-item"><input type="radio" id="viz-dots" name="viz-type" value="dots" checked onchange="updateVisualization()"><label for="viz-dots">Individual Locations (Dots)</label></div><div class="radio-item"><input type="radio" id="viz-heatmap" name="viz-type" value="heatmap" onchange="updateVisualization()"><label for="viz-heatmap">Density (Heatmap)</label></div></div></div>"""

    sidebar_html = f""" <div class="sidebar"> <h2>US Fitzpatrick Trials</h2> <div class="filter-summary"> <div><strong>Studies:</strong> <span id="visible-studies-count">{total_studies}</span> of {total_studies}</div> <div><strong>Locations:</strong> <span id="visible-locations-count">{total_locations}</span> of {total_locations}</div> </div> {viz_switcher_html} <div class="filter-section"><h3>Fitzpatrick Skin Types</h3>{skin_type_html}</div> <div class="filter-section"> <h3>Enrollment</h3> <div class="control-group"><label for="min-enrollment">Minimum Enrollment:</label><input type="range" id="min-enrollment" class="slider" min="0" max="{max_enrollment}" value="0" oninput="updateFilters()"><div class="slider-value" id="min-enrollment-value">0+</div></div> <div class="control-group" style="margin-top:15px;"><label style="display:block;margin-bottom:8px;">Enrollment Type:</label><div class="checkbox-group"><div class="checkbox-item"><input type="checkbox" id="enrollment-actual" checked onchange="updateFilters()"><label for="enrollment-actual">Actual</label></div><div class="checkbox-item"><input type="checkbox" id="enrollment-estimated" checked onchange="updateFilters()"><label for="enrollment-estimated">Estimated</label></div><div class="checkbox-item"><input type="checkbox" id="enrollment-na" checked onchange="updateFilters()"><label for="enrollment-na">N/A</label></div></div></div> </div> <div class="filter-section"><h3>Study Status</h3><div class="checkbox-group">{status_checkboxes}</div></div> <div class="filter-section"> <h3>Last Updated Year</h3> <div class="control-group"><label for="year-range">Minimum Year:</label><input type="range" id="year-range" class="slider" min="{min_year}" max="{max_year}" value="{min_year}" oninput="updateFilters()"><div class="slider-value" id="year-range-value">{min_year}+</div></div> </div> <div class="filter-section"><h3>Race Demographics</h3>{race_filter_html}</div> <div class="filter-section"><button class="reset-btn" onclick="resetAllFilters()">Reset All Filters</button></div> </div> """

    # --- JavaScript using .format() ---
    javascript_code = """
        let mapInstance, markersLayer, heatmapLayer;
        const locationsData = {locations_data_json};
        const heatmapData = {heatmap_data_json};
        const heatmapGradient = {heatmap_gradient_json};
        const allRaceColumns = {all_race_columns_json};
        const raceDataInfo = {race_data_info_json};
        const allStatuses = {all_statuses_json};
        const statusDisplayMap = {status_display_map_json};
        const minYear = {min_year};

        function findMapInstance() {{ return window[document.querySelector('.folium-map').id]; }}
        window.addEventListener('load', function() {{ setTimeout(initializeMap, 500); }});

        function initializeMap() {{
            mapInstance = findMapInstance();
            if (!mapInstance) {{ console.error("Map instance not found."); return; }}
            markersLayer = L.layerGroup();
            heatmapLayer = L.heatLayer(heatmapData, {{ radius: 20, blur: 15, gradient: heatmapGradient}});
            updateVisualization(); // Sets the initial view
            updateFilters();
        }}
        
        // --- Re-added for Heatmap ---
        window.updateVisualization = function() {{
            const vizType = document.querySelector('input[name="viz-type"]:checked').value;
            if (vizType === 'dots') {{
                if (mapInstance.hasLayer(heatmapLayer)) mapInstance.removeLayer(heatmapLayer);
                if (!mapInstance.hasLayer(markersLayer)) mapInstance.addLayer(markersLayer);
            }} else {{ // heatmap
                if (mapInstance.hasLayer(markersLayer)) mapInstance.removeLayer(markersLayer);
                if (!mapInstance.hasLayer(heatmapLayer)) mapInstance.addLayer(heatmapLayer);
            }}
        }};

        function passesFilters(record, enrollmentFilter, enrollmentTypes, statusTypes, raceFilters, activeTypes, yearFilter) {{
            if (!activeTypes.some(type => record[`Type_${{type}}`] === 1)) return false;
            const recordYear = parseInt(record.last_update_year);
            if (!isNaN(recordYear) && recordYear < yearFilter) return false;
            const enrollment = record.enrollment === 'N/A' ? 0 : record.enrollment;
            if (enrollment < enrollmentFilter) return false;
            if (!enrollmentTypes.includes((record.enrollment_type || 'N/A').toUpperCase())) return false;
            if (!statusTypes.includes(record.status || 'N/A')) return false;
            for (const [raceCol, minVal] of Object.entries(raceFilters)) {{
                if ((record[raceCol] || 0) < minVal) return false;
            }}
            return true;
        }}

        window.updateFilters = function() {{
            if (!mapInstance || !markersLayer) return;
            const activeTypes = Array.from(document.querySelectorAll('.skin-type-item.active')).map(el => el.dataset.type);
            const enrollmentFilter = parseInt(document.getElementById('min-enrollment').value);
            document.getElementById('min-enrollment-value').textContent = enrollmentFilter + '+';
            const yearFilter = parseInt(document.getElementById('year-range').value);
            document.getElementById('year-range-value').textContent = yearFilter + '+';
            const enrollmentTypes = [];
            if (document.getElementById('enrollment-actual').checked) enrollmentTypes.push('ACTUAL');
            if (document.getElementById('enrollment-estimated').checked) enrollmentTypes.push('ESTIMATED');
            if (document.getElementById('enrollment-na').checked) enrollmentTypes.push('N/A');
            const statusTypes = allStatuses.filter(status => document.getElementById(`status-${{status.toLowerCase()}}`)?.checked);
            const raceFilters = {{}};
            for (const raceCol in raceDataInfo) {{
                const elId = raceCol.toLowerCase();
                const element = document.getElementById(elId);
                if (element) {{
                    const minValue = parseInt(element.value);
                    raceFilters[raceCol] = minValue;
                    document.getElementById(elId + '-value').textContent = minValue + '+';
                }}
            }}
            markersLayer.clearLayers();
            let visibleLocations = 0;
            const visibleStudies = new Set();
            for (const [locKey, studiesAtLoc] of Object.entries(locationsData)) {{
                const passingStudies = studiesAtLoc.filter(study => passesFilters(study, enrollmentFilter, enrollmentTypes, statusTypes, raceFilters, activeTypes, yearFilter));
                if (passingStudies.length > 0) {{
                    visibleLocations++;
                    passingStudies.forEach(study => visibleStudies.add(study.nctId));
                    const [lat, lon] = locKey.split(',').map(Number);
                    let popupHtml = '<div style="font-family: Arial, sans-serif; max-height: 300px; overflow-y: auto; min-width: 350px;">';
                    passingStudies.forEach((study, i) => {{
                        let raceHtml = "";
                        allRaceColumns.forEach(raceCol => {{
                            const count = study[raceCol] || 0;
                            if (count > 0) raceHtml += `<li>${{raceDataInfo[raceCol]?.display_name || raceCol}}: <strong>${{count}}</strong></li>`;
                        }});
                        if (raceHtml) raceHtml = `<p style="margin:5px 0 3px;"><strong>Demographics:</strong></p><ul style="margin:0;padding-left:20px;">${{raceHtml}}</ul>`;
                        const enrollmentDisplay = study.enrollment !== 'N/A' && study.enrollment_type !== 'N/A' ? `${{study.enrollment}} (${{study.enrollment_type}})` : (study.enrollment || 'N/A');
                        const statusDisplay = statusDisplayMap[study.status] || study.status;
                        const includedSkinTypes = ['I', 'II', 'III', 'IV', 'V', 'VI'].filter(roman => study[`Type_${{roman}}`] === 1);
                        const skinTypeDisplay = includedSkinTypes.length > 0 ? includedSkinTypes.join(', ') : 'Not Specified';
                        const lastUpdateYearDisplay = study.last_update_year || 'N/A';
                        popupHtml += `<div style="border-top: ${{i > 0 ? '1px solid #ccc' : 'none'}}; padding: 10px 5px;"><h4 style="margin:0 0 10px 0;">Study Details</h4><p><strong>NCT ID:</strong> <a href="https://clinicaltrials.gov/study/${{study.nctId}}" target="_blank">${{study.nctId}}</a></p><p><strong>Status:</strong> ${{statusDisplay}}</p><p><strong>Last Updated:</strong> ${{lastUpdateYearDisplay}}</p><p><strong>Enrollment:</strong> <strong>${{enrollmentDisplay}}</strong></p><p><strong>Facility:</strong> ${{study.facility}}</p><p><strong>Skin Types:</strong> ${{skinTypeDisplay}}</p>${{raceHtml}}</div>`;
                    }});
                    popupHtml += '</div>';

                    const firstStudy = passingStudies[0];
                    const isPrecise = firstStudy.place_name && firstStudy.place_name !== 'NO_RESULTS_FOUND';
                    const tooltipPrefix = isPrecise ? '[Facility]' : '[City]';
                    const tooltipName = isPrecise ? firstStudy.place_name : firstStudy.city;
                    
                    L.circleMarker([lat, lon], {{ radius: 6 + Math.sqrt(passingStudies.length), color: '#ffffff', weight: 2, fillColor: '#764ba2', fillOpacity: 0.8 }})
                        .bindPopup(popupHtml, {{maxWidth: 400}})
                        .bindTooltip(`${{tooltipPrefix}} ${{tooltipName}} (${{passingStudies.length}} studies)`)
                        .addTo(markersLayer);
                }}
            }}
            document.getElementById('visible-locations-count').textContent = visibleLocations;
            document.getElementById('visible-studies-count').textContent = visibleStudies.size;
        }};

        window.resetAllFilters = function() {{
            // --- Re-added for Heatmap ---
            document.getElementById('viz-dots').checked = true;
            updateVisualization();

            document.querySelectorAll('.skin-type-item').forEach(item => item.classList.add('active'));
            document.querySelectorAll('.slider').forEach(slider => slider.value = 0);
            const yearSlider = document.getElementById('year-range');
            if(yearSlider) yearSlider.value = minYear;
            document.getElementById('enrollment-actual').checked = true;
            document.getElementById('enrollment-estimated').checked = true;
            document.getElementById('enrollment-na').checked = true;
            allStatuses.forEach(status => {{ const checkbox = document.getElementById(`status-${{status.toLowerCase()}}`); if (checkbox) checkbox.checked = true; }});
            updateFilters();
        }};
    """.format(
        locations_data_json=json.dumps(locations_data),
        heatmap_data_json=json.dumps(heatmap_data),
        heatmap_gradient_json=json.dumps(heatmap_gradient),
        all_race_columns_json=json.dumps(all_race_columns),
        race_data_info_json=json.dumps(race_data),
        all_statuses_json=json.dumps(all_statuses),
        status_display_map_json=json.dumps(status_display_map),
        min_year=min_year
    )

    m.get_root().header.add_child(Element(f"<style>{css_rules}</style>"))
    m.get_root().html.add_child(Element(sidebar_html))
    m.get_root().script.add_child(Element(javascript_code))

    m.save(filename)
    print(f"\n[*] Success! Interactive map saved to '{filename}'.")
# --- 5. Main Orchestrator ---

def main():
    """Main function to run the entire data processing and mapping pipeline."""
    if os.path.exists(FINAL_MASTER_CSV):
        print(f"[*] Final dataset '{FINAL_MASTER_CSV}' found. Skipping to map generation.")
        df_final = pd.read_csv(FINAL_MASTER_CSV)
    else:
        print(f"[!] Final dataset not found. Starting full data pipeline...")
        # Step 1: Fetch from API if raw JSON doesn't exist
        if not os.path.exists(RAW_JSON_FILENAME):
            fetch_clinical_trials_data(API_BASE_URL, SEARCH_KEYWORD, RAW_JSON_FILENAME)
        
        # Step 2: Load and process the raw JSON data
        try:
            with open(RAW_JSON_FILENAME, 'r', encoding='utf-8') as f:
                studies = json.load(f).get('studies', [])
            print(f"[*] Loaded {len(studies)} studies from '{RAW_JSON_FILENAME}'.")
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"[!] Error loading raw JSON file: {e}. Exiting.")
            return

        df_processed = process_raw_data(studies)
        if df_processed.empty:
            print("[!] No data to process after initial parsing. Exiting.")
            return
            
        # Step 3: Geocode locations using Google Places API
        df_final = geocode_locations_with_places_api(df_processed)
        
        # Step 4: Save the final master dataset
        try:
            os.makedirs(os.path.dirname(FINAL_MASTER_CSV), exist_ok=True)
            df_final.to_csv(FINAL_MASTER_CSV, index=False, encoding='utf-8')
            print(f"\n[*] Success! Final master dataset saved to '{FINAL_MASTER_CSV}'.")
        except IOError as e:
            print(f"[!] Error writing final CSV file: {e}")
            return
    
    # Final Step: Create the interactive map
    if df_final.empty:
        print("[!] Final dataset is empty. Cannot create map.")
        return
        
    # Convert DataFrame to list of dicts for the map function
    # The map's JS expects camelCase keys, so we ensure columns match that format
    map_data = df_final.to_dict('records')
    create_interactive_map_with_sidebar(map_data, MAP_OUTPUT_HTML)

if __name__ == "__main__":
    main()